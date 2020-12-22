package surfstore

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// Server represents two stores
type Server struct {
	BlockStore BlockStoreInterface
	MetaStore  MetaStoreInterface
}

// GetFileInfoMap calls the GetFileInfoMap function in MetaStore
func (s *Server) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	err := s.MetaStore.GetFileInfoMap(_ignore, serverFileInfoMap)
	if err != nil {
		return err
	}

	return nil
}

// UpdateFile calls the UpdateFile function in MetaStore
func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	return s.MetaStore.UpdateFile(fileMetaData, latestVersion)
}

// GetBlock calls the GetBlock function in BlockStore
func (s *Server) GetBlock(blockHash string, blockData *Block) error {
	return s.BlockStore.GetBlock(blockHash, blockData)
}

// PutBlock calls the PutBlock function in BlockStore
func (s *Server) PutBlock(blockData Block, succ *bool) error {
	return s.BlockStore.PutBlock(blockData, succ)
}

// HasBlocks calls the HasBlocks function in BlockStore
func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	return s.BlockStore.HasBlocks(blockHashesIn, blockHashesOut)
}

// This line guarantees all method for surfstore are implemented
var _ Surfstore = new(Server)

// NewSurfstoreServer initializes a server with two stores
func NewSurfstoreServer() Server {
	blockStore := BlockStore{BlockMap: map[string]Block{}}
	metaStore := MetaStore{FileMetaMap: map[string]FileMetaData{}}

	return Server{
		BlockStore: &blockStore,
		MetaStore:  &metaStore,
	}
}

// ServeSurfstoreServer hosts the server
func ServeSurfstoreServer(hostAddr string, surfstoreServer Server) error {
	rpc.Register(&surfstoreServer)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", hostAddr)
	if e != nil {
		log.Fatal("listen error:", e)
		return e
	}

	return http.Serve(l, nil)
}
