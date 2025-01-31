package lotusdb

import (
	"github.com/google/uuid"
	"sync"
)

// ThresholdState 表示压缩阈值的状态
type ThresholdState int

const (
	// ArriveAdvisedThreshold 表示推荐此时进行压缩
	ArriveAdvisedThreshold ThresholdState = iota
	// ArriveForceThreshold 表示需要强制压缩的状态
	ArriveForceThreshold
	// UnarriveThreshold 表示不需要压缩
	UnarriveThreshold
)

// DeprecatedTable 用于存储已删除/更新键的旧信息。
// 每次写入/更新都会生成一个 UUID，并将其存储在表中。
// 它在压缩过程中很有用，允许我们知道 value log 中的 KV 是否是最新的，而无需访问索引。
type DeprecatedTable struct {
	partition int                    // 所属的 vlog 分区
	table     map[uuid.UUID]struct{} // 在内存中存储已废弃键的 UUID
	size      uint32                 // 当前废弃条目的数量
	mu        sync.RWMutex           // 保护并发访问的互斥锁
}

// DeprecatedState 用于向 autoCompact 发送消息
type DeprecatedState struct {
	thresholdState ThresholdState // 用于发送压缩消息
}

// NewDeprecatedTable 创建一个新的已废弃表
func NewDeprecatedTable(partition int) *DeprecatedTable {
	return &DeprecatedTable{
		partition: partition,
		table:     make(map[uuid.UUID]struct{}),
		size:      0,
	}
}

// AddEntry 添加一个 UUID 到指定键
func (dt *DeprecatedTable) AddEntry(id uuid.UUID) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.table[id] = struct{}{}
	dt.size++
}

// ExistEntry 检查一个 UUID 是否存在于表中
func (dt *DeprecatedTable) ExistEntry(id uuid.UUID) bool {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	_, exists := dt.table[id]
	return exists
}

// Clean 清空已废弃表中的所有条目
func (dt *DeprecatedTable) Clean() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.table = make(map[uuid.UUID]struct{})
	dt.size = 0
}
