package surfstore

// Block describes the byte data and the size
type Block struct {
	BlockData []byte
	BlockSize int
}

// FileMetaData describes the file data including name, version, and hash list
type FileMetaData struct {
	Filename      string
	Version       int
	BlockHashList []string
}

// Surfstore describes the interface to MetaStoreInterface and BlockStoreInterface
type Surfstore interface {
	MetaStoreInterface
	BlockStoreInterface
}

// MetaStoreInterface is the interface to MetaStore
type MetaStoreInterface interface {
	// Retrieves the server's FileInfoMap
	GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error

	// Update a file's fileinfo entry
	UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error)
}

// BlockStoreInterface is the interface to BlockStore
type BlockStoreInterface interface {

	// Get a block based on its hash
	GetBlock(blockHash string, block *Block) error

	// Put a block
	PutBlock(block Block, succ *bool) error

	// Check if certain blocks are alredy present on the server
	HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error
}
