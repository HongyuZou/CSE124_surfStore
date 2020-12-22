package surfstore

import (
	"net/rpc"
)

// RPCClient defines the struct of client
type RPCClient struct {
	ServerAddr string
	BaseDir    string
	BlockSize  int
}

// GetBlock calls server to get a block data for the given hash
func (surfClient *RPCClient) GetBlock(blockHash string, block *Block) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Server.GetBlock", blockHash, block)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

// PutBlock calls server to update a block of data
func (surfClient *RPCClient) PutBlock(block Block, succ *bool) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Server.PutBlock", block, succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

// HasBlocks calls server to check if blocks exist
func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Server.HasBlocks", blockHashesIn, blockHashesOut)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

// GetFileInfoMap calls server to get a FileInfoMap
func (surfClient *RPCClient) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Server.GetFileInfoMap", _ignore, serverFileInfoMap)
	if e != nil {
		conn.Close()
		return e
	}
	// close the connection
	return conn.Close()
}

// UpdateFile calls server to update a FileMetaData
func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", surfClient.ServerAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Server.UpdateFile", fileMetaData, latestVersion)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

var _ Surfstore = new(RPCClient)

// NewSurfstoreRPCClient creates an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		ServerAddr: hostPort,
		BaseDir:    baseDir,
		BlockSize:  blockSize,
	}
}
