package lotusdb

import (
	"bytes"
)

type iterType uint8

const (
	BptreeItr iterType = iota // Bptree 迭代器类型
	MemItr                    // 内存迭代器类型
)

// singleIter element used to construct the heap，implementing the container.heap interface.
type singleIter struct {
	iType   iterType
	options IteratorOptions
	rank    int          // 较高的 rank 表示较新的数据
	idx     int          // 在堆中的索引
	iter    baseIterator // 具体的迭代器实现
}

// 迭代器堆
type iterHeap []*singleIter

// Len 长度
func (ih *iterHeap) Len() int {
	return len(*ih)
}

// Less 如果两个元素的键相同，rank较大的认为较新。否则根据 options.Reverse 属性来决定键的比较顺序
func (ih *iterHeap) Less(i int, j int) bool {
	ki, kj := (*ih)[i].iter.Key(), (*ih)[j].iter.Key()
	if bytes.Equal(ki, kj) {
		return (*ih)[i].rank > (*ih)[j].rank
	}
	if (*ih)[i].options.Reverse {
		return bytes.Compare(ki, kj) == 1
	}
	return bytes.Compare(ki, kj) == -1
}

// Swap 交换堆中两个元素的位置，同时更新它们的索引
func (ih *iterHeap) Swap(i int, j int) {
	(*ih)[i], (*ih)[j] = (*ih)[j], (*ih)[i]
	(*ih)[i].idx, (*ih)[j].idx = i, j
}

// Push 向堆中添加一个新元素
func (ih *iterHeap) Push(x any) {
	*ih = append(*ih, x.(*singleIter))
}

// Pop 从堆中移除并返回最后一个元素
func (ih *iterHeap) Pop() any {
	old := *ih
	n := len(old)
	x := old[n-1]
	*ih = old[0 : n-1]
	return x
}
