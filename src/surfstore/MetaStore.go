package surfstore

import (
	"fmt"
)

// MetaStore struct contains the map from filename to FileMetaData
type MetaStore struct {
	FileMetaMap map[string]FileMetaData
}

// GetFileInfoMap get the mapping of files stored in cloud
func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	*serverFileInfoMap = m.FileMetaMap
	return nil
}

// UpdateFile updates the FileInfo hash when the new version number is exactly one greater
// than the current version number
func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {
	fileName := fileMetaData.Filename
	serverFileMetaData, ok := m.FileMetaMap[fileName]

	// Check version mismatch
	if ok && fileMetaData.Version != serverFileMetaData.Version {
		*latestVersion = serverFileMetaData.Version
		return fmt.Errorf("UpdateFile version mismatch. "+
			"Current version for \"%s\" on server is %d", fileName, serverFileMetaData.Version)
	}

	// Update server file map
	m.FileMetaMap[fileName] = FileMetaData{
		Filename:      fileMetaData.Filename,
		Version:       fileMetaData.Version + 1,
		BlockHashList: fileMetaData.BlockHashList,
	}

	*latestVersion = fileMetaData.Version + 1
	return nil
}

var _ MetaStoreInterface = new(MetaStore)
