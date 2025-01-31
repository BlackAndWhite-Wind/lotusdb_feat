package lotusdb

import (
	"bytes"
)

// iterType 定义迭代器类型
type iterType uint8

// iterHeap 是一个单迭代器的堆，便于进行合并和排序
type iterHeap []*singleIter

// singleIter 是一个单独的迭代器，以及相关的元信息
type singleIter struct {
	iType   iterType
	options IteratorOptions
	rank    int          // 较高的 rank 表示较新的数据
	idx     int          // 在堆中的索引
	iter    baseIterator // 具体的迭代器实现
}

// 各种迭代器类型
const (
	BptreeItr iterType = iota // Bptree 迭代器类型
	MemItr                    // 内存迭代器类型
)

// Len 返回堆的长度
func (ih *iterHeap) Len() int {
	return len(*ih)
}

// Less 比较堆中两个元素的大小，决定优先级
func (ih *iterHeap) Less(i int, j int) bool {
	iterI, iterJ := (*ih)[i], (*ih)[j]
	keyI, keyJ := iterI.iter.Key(), iterJ.iter.Key()
	comparison := bytes.Compare(keyI, keyJ)
	if comparison == 0 {
		return iterI.rank > iterJ.rank
	}
	// 如果是反向迭代，则较大的键值优先
	if iterI.options.Reverse {
		return comparison > 0
	}
	return comparison < 0
}

// Swap 交换堆中的两个元素
func (ih *iterHeap) Swap(i int, j int) {
	(*ih)[i], (*ih)[j] = (*ih)[j], (*ih)[i]
	(*ih)[i].idx, (*ih)[j].idx = i, j
}

// Push 向堆中添加一个元素
func (ih *iterHeap) Push(x any) {
	*ih = append(*ih, x.(*singleIter))
}

// Pop 从堆中移除并返回最顶上的元素
func (ih *iterHeap) Pop() any {
	oldHeap := *ih
	n := len(oldHeap)
	item := oldHeap[n-1]
	*ih = oldHeap[:n-1]
	return item
}
