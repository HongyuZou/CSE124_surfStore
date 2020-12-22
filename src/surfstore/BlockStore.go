package surfstore

import (
	"crypto/sha256"
	"fmt"
)

// BlockStore Datastructure to store block
type BlockStore struct {
	BlockMap map[string]Block
}

// GetBlock get a block data for the given hash
func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	blockMap := bs.BlockMap

	// bug
	if _, ok := blockMap[blockHash]; !ok {
		return fmt.Errorf("Block with hash %s does not exists", blockHash)
	}

	*blockData = bs.BlockMap[blockHash]
	return nil
}

// PutBlock stores block data to server
func (bs *BlockStore) PutBlock(block Block, succ *bool) error {
	hashCode := sha256.Sum256(block.BlockData)
	hashCodeStr := fmt.Sprintf("%x", hashCode)
	bs.BlockMap[hashCodeStr] = block
	*succ = true
	return nil
}

// HasBlocks retrieves a list containing the subset of ​"blockHashesIn" that are stored on server
func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	hashoutlist := []string{}
	blockMap := bs.BlockMap
	for _, hashin := range blockHashesIn {
		if _, ok := blockMap[hashin]; ok {
			hashoutlist = append(hashoutlist, hashin)
		}
	}
	*blockHashesOut = hashoutlist
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)
