package surfstore

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func needUpdateIndex(fileMeta FileMetaData, hashList []string) bool {
	if len(fileMeta.BlockHashList) != len(hashList) {
		return true
	}

	for i := 0; i < len(hashList); i++ {
		if fileMeta.BlockHashList[i] != hashList[i] {
			return true
		}
	}

	return false
}

func parseIndex(indexPath string) (fileMap map[string]FileMetaData, err error) {
	// Open mime_types file
	file, err := os.Open(indexPath)
	defer file.Close()
	if err != nil {
		return nil, err
	}

	// Read mime_types
	fileMap = make(map[string]FileMetaData)
	fscanner := bufio.NewScanner(file)

	for fscanner.Scan() {
		line := fscanner.Text()
		tokens := strings.Split(line, ",")
		version, err := strconv.Atoi(tokens[1])
		if err != nil {
			return nil, err
		}

		fileMap[tokens[0]] = FileMetaData{
			Filename:      tokens[0],
			Version:       version,
			BlockHashList: strings.Split(tokens[2], " "),
		}
	}

	return fileMap, nil
}

func createIndexFile(indexPath string) error {
	file, err := os.Create(indexPath)
	defer file.Close()
	return err
}

// remote index refers to a file not present in the local index or in the base directory.
func handleNewFilesInRemote(remoteIndexMap, localIndexMap, newFileMap map[string]FileMetaData,
	client RPCClient) error {
	for fileName, remoteFileMetaData := range remoteIndexMap {
		// File does not exist in local index or server version is newer
		if localFileMeta, ok := localIndexMap[fileName]; !ok || localFileMeta.Version < remoteFileMetaData.Version {
			err := overrideLocalFile(client, fileName, remoteFileMetaData, localIndexMap)
			if err != nil {
				log.Println("overrideLocalFile", err)
				return err
			}

			// Do not need to push older version if it is also changed by client
			_, ok := newFileMap[fileName]
			if ok {
				delete(newFileMap, fileName)
			}
		}
	}
	return nil
}

func overrideLocalFile(client RPCClient, fileName string, remoteFileMetaData FileMetaData,
	localIndexMap map[string]FileMetaData) error {

	if len(remoteFileMetaData.BlockHashList) != 1 || remoteFileMetaData.BlockHashList[0] != "0" {
		// normal event of update file
		newFile, err := os.OpenFile(filepath.Join(client.BaseDir, fileName), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		defer newFile.Close()
		if err != nil {
			log.Println("os.OpenFile", err)
			return err
		}

		// Write block to file
		for _, blockHash := range remoteFileMetaData.BlockHashList {
			// call GetBlock
			var block Block
			err := client.GetBlock(blockHash, &block)
			if err != nil {
				log.Println("client.GetBlock", err)
				return err
			}

			// write to file
			n, err := newFile.Write(block.BlockData)
			if err != nil {
				log.Println("newFile.Write", err)
				return err
			}
			if n != block.BlockSize {
				log.Println("newFile.Write expected size", block.BlockSize, ", actual size", n)
				return err
			}
		}
	} else {
		// delete event
		err := os.Remove(filepath.Join(client.BaseDir, fileName))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// update file infomration to local index
	localIndexMap[fileName] = remoteFileMetaData

	return nil
}

func updateBlocksAndMeta(client RPCClient, fileMetaData FileMetaData, localIndexMap map[string]FileMetaData,
	newBlockMap map[string]Block, fileName string) error {
	// upload the blocks corresponding to this file
	var succ bool

	if len(fileMetaData.BlockHashList) != 1 || fileMetaData.BlockHashList[0] != "0" {
		for _, blockHash := range fileMetaData.BlockHashList {
			err := client.PutBlock(newBlockMap[blockHash], &succ)
			if err != nil {
				log.Println("client.PutBlock", err)
				return err
			}
			if !succ {
				log.Println("client.PutBlock succ = false")
				return err
			}
		}
	}

	// update the server with the new FileInfo
	var latestVersion int
	err := client.UpdateFile(&fileMetaData, &latestVersion)
	if err != nil {
		log.Println("client.UpdateFile", err)
		return err
	}

	// update local index
	localIndexMap[fileName] = FileMetaData{
		Filename:      fileMetaData.Filename,
		Version:       latestVersion,
		BlockHashList: fileMetaData.BlockHashList,
	}

	return nil
}

// local index == base dir, remote index != base dir
// upload file to remote, remote refresh index -> local index refresh
// server newer than local

// new files in the local base directory that aren’t in the local or remote index.
func handleNewFilesInLocal(remoteIndexMap, localIndexMap, newFileMap map[string]FileMetaData,
	client RPCClient, newBlockMap map[string]Block) error {

	for fileName, fileMetaData := range newFileMap {
		// File does not exist in server info map or client has newer commit
		serverMetaData, ok := remoteIndexMap[fileName]
		if !ok || serverMetaData.Version == fileMetaData.Version {
			err := updateBlocksAndMeta(client, fileMetaData, localIndexMap, newBlockMap, fileName)
			if err != nil {
				log.Println("updateBlocksAndMeta new file", err)
				return err
			}
		} else if fileMetaData.Version < serverMetaData.Version {
			err := overrideLocalFile(client, fileName, serverMetaData, localIndexMap)
			if err != nil {
				log.Println("overrideLocalFile new file", err)
				return err
			}
		}
	}

	return nil
}

// ClientSync sync a client with the server
// 1. Local index, base directory, remote index
// 2. Find new file in base directory (not exist, or hash changed)
// 3. Connect to server and download fileInfoMap
// 4. New file in Remote Index (not in base dir or local index)-> download it, update local index
// 5. New file in base dir (not in local or remote index) -> upload files to server, update server file table -> update local index (It is possible fail with version error, -> handle conflits)
// Conflicts
// 6. compare local index with local file, No local modification, remote is newer, update local
// 7. Local modification, remote and local index same version -> update to remote and update local file index
// 8. Local file index 3, remote index 4, local changed -> download cloud and update version
func ClientSync(client RPCClient) {
	// client check base directory
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("ioutil.ReadDir", err)
		return
	}

	// check index file exists
	indexPath := filepath.Join(client.BaseDir, "index.txt")
	if !fileExists(indexPath) {
		err := createIndexFile(indexPath)
		if err != nil {
			log.Println("createIndexFile", err)
			return
		}
	}

	// read index file
	indexMap, err := parseIndex(indexPath)
	if err != nil {
		log.Println("parseIndex", err)
		return
	}

	// make a copy of index map to indicate delete map
	deletedMap := map[string]FileMetaData{}
	for fileName, fileMetaData := range indexMap {
		deletedMap[fileName] = fileMetaData
	}

	// store a local map that need changes
	newFileMap := map[string]FileMetaData{}

	// store a local block map
	newBlockMap := make(map[string]Block)

	// read file into block of client size
	for _, fileInfo := range files {
		if fileInfo.Name() == "index.txt" {
			continue
		}

		// Remove seen file from deletedMap
		_, ok := deletedMap[fileInfo.Name()]
		if ok {
			delete(deletedMap, fileInfo.Name())
		}

		delete(deletedMap, fileInfo.Name())

		f, err := os.Open(filepath.Join(client.BaseDir, fileInfo.Name()))
		defer f.Close()

		hashList := []string{}

		for {
			data := make([]byte, client.BlockSize)
			if err != nil {
				log.Println("os.Open", err)
				return
			}
			n, err := f.Read(data)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Println("f.Read", err)
				return
			}
			data = data[:n]

			// hash data
			hashCodeByte := sha256.Sum256(data)
			hashCode := fmt.Sprintf("%x", hashCodeByte)
			hashList = append(hashList, hashCode)
			newBlockMap[hashCode] = Block{
				BlockData: data,
				BlockSize: n,
			}
		}

		// Check file difference with local index
		if _, ok := indexMap[fileInfo.Name()]; !ok {
			// New file that is not in local index
			newFileMap[fileInfo.Name()] = FileMetaData{
				Filename:      fileInfo.Name(),
				Version:       0,
				BlockHashList: hashList,
			}
		} else if fileMeta, _ := indexMap[fileInfo.Name()]; needUpdateIndex(fileMeta, hashList) {
			// File is newer than local index
			newFileMap[fileInfo.Name()] = FileMetaData{
				Filename:      fileInfo.Name(),
				Version:       fileMeta.Version,
				BlockHashList: hashList,
			}
		}
	}

	// Update delete events to newFileMap
	for fileName, fileMetaData := range deletedMap {
		newFileMap[fileName] = FileMetaData{
			Filename:      fileName,
			Version:       fileMetaData.Version,
			BlockHashList: []string{"0"},
		}
	}

	// Download server's FileInfoMap
	var serverFileInfoMap map[string]FileMetaData = make(map[string]FileMetaData)
	var succ bool
	err = client.GetFileInfoMap(&succ, &serverFileInfoMap)
	if err != nil {
		log.Println("client.GetFileInfoMap", err)
		return
	}

	// 1. remote index not present in the local index or in the base directory (PULL)
	err = handleNewFilesInRemote(serverFileInfoMap, indexMap, newFileMap, client)
	if err != nil {
		log.Println("handleNewFilesInRemote", err)
		return
	}

	// 2. new files in the local base directory that aren’t in the local or remote index (PUSH)
	err = handleNewFilesInLocal(serverFileInfoMap, indexMap, newFileMap, client, newBlockMap)
	if err != nil {
		log.Println("handleNewFilesInLocal", err)
		err = handleNewFilesInRemote(serverFileInfoMap, indexMap, newFileMap, client)
		if err != nil {
			log.Println("handleNewFilesInRemote", err)
			return
		}
	}

	// update local index file
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	defer indexFile.Close()
	if err != nil {
		log.Println("os.OpenFile(indexPath)", err)
		return
	}
	for fileName, fileMetaData := range indexMap {
		indexFile.Write(
			[]byte(fmt.Sprintf("%s,%d,%s\n", fileName, fileMetaData.Version,
				strings.Join(fileMetaData.BlockHashList, " "))))
	}
}

// PrintMetaMap prints the contents of the metadata map.
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}
