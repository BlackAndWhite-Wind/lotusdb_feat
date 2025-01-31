package lotusdb

import (
	"github.com/rosedblabs/diskhash"
)

// indexFileExt is the file extension for index files.
const indexFileExt = "INDEX.%d"

// Index is the interface for index implementations.
// An index is a key-value store that maps keys to chunk positions.
// The index is used to find the chunk position of a key.
//
// Currently, the only implementation is a BoltDB index.
// But you can implement your own index if you want.
type Index interface {
	// PutBatch 批量插入记录
	PutBatch(keyPositions []*KeyPosition, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error)
	// Get 根据给定的key查找其位置
	Get(key []byte, matchKeyFunc ...diskhash.MatchKeyFunc) (*KeyPosition, error)
	// DeleteBatch 批量删除记录
	DeleteBatch(keys [][]byte, matchKeyFunc ...diskhash.MatchKeyFunc) ([]*KeyPosition, error)
	// Sync 将索引数据同步到磁盘
	Sync() error
	// Close 关闭索引并进行清理
	Close() error
}

// open the specified index according to the index type
// currently, we support two index types: BTree and Hash,
// both of them are disk-based index.
func openIndex(options indexOptions) (Index, error) {
	switch options.indexType {
	case BTree:
		return openBTreeIndex(options)
	case Hash:
		return openHashIndex(options)
	default:
		panic("unknown index type")
	}
}

type IndexType int8

const (
	BTree IndexType = iota
	Hash
)

type indexOptions struct {
	indexType       IndexType
	dirPath         string              // index directory path
	partitionNum    int                 // index partition nums for sharding
	keyHashFunction func([]byte) uint64 // hash function for sharding
}

func (io *indexOptions) getKeyPartition(key []byte) int {
	hashFn := io.keyHashFunction
	return int(hashFn(key) % uint64(io.partitionNum))
}
