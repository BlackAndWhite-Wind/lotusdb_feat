package lotusdb

import (
	"fmt"
	"github.com/rosedblabs/diskhash"
)

// IndexType 定义了索引的类型
type IndexType int8

const (
	BTree IndexType = iota
	Hash
)

// indexOptions provides options for creating an index
type indexOptions struct {
	indexType       IndexType           // 索引类型
	dirPath         string              // 索引的目录路径
	partitionNum    int                 // 分区数
	keyHashFunction func([]byte) uint64 // 用于分区的哈希函数
}

const indexFileExt = "INDEX.%d"

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

// getKeyPartition calculates the partition for a given key
func (io *indexOptions) getKeyPartition(key []byte) int {
	hashFn := io.keyHashFunction
	return int(hashFn(key) % uint64(io.partitionNum))
}

// openIndex 打开指定类型的索引，目前支持 BTree 和 Hash 索引
func openIndex(options indexOptions) (Index, error) {
	switch options.indexType {
	case BTree:
		return openBTreeIndex(options)
	case Hash:
		return openHashIndex(options)
	default:
		return nil, fmt.Errorf("unknown index type: %d", options.indexType)
	}
}
