package ledger

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// cellNode 是不可变的版本化值节点，构成单向链表（新值在前）。
type cellNode[T any] struct {
	version uint64
	value   T
	prev    *cellNode[T]
}

// cell 是一个多版本单元格：写方追加新版本节点，读方可按版本读取历史值。
// 初始值版本为 0。
type cell[T any] struct {
	head atomic.Pointer[cellNode[T]]
}

func newCell[T any](initial T) *cell[T] {
	c := &cell[T]{}
	c.head.Store(&cellNode[T]{version: 0, value: initial})
	return c
}

// load 返回最新值。调用方需通过外部同步（锁或快照协议）保证语义。
func (c *cell[T]) load() T {
	return c.head.Load().value
}

// loadVersion 返回最新值及其版本。
func (c *cell[T]) loadVersion() (T, uint64) {
	n := c.head.Load()
	return n.value, n.version
}

// store 追加一个带版本的新值。必须由持有相应业务锁的写方调用。
func (c *cell[T]) store(version uint64, v T) {
	c.head.Store(&cellNode[T]{version: version, value: v, prev: c.head.Load()})
}

// loadAsOf 返回版本 <= v 的最新值。前提：版本 <= v 的写已全部发布
// （由 commitTracker 的连续前沿保证）。
func (c *cell[T]) loadAsOf(v uint64) T {
	n := c.head.Load()
	for n != nil && n.version > v {
		n = n.prev
	}
	if n == nil {
		var zero T
		return zero
	}
	return n.value
}

// commitTracker 分配全局递增的提交版本号，并维护"已连续发布"的前沿版本。
// 写方：assign 取得版本号 -> 发布全部 cell -> published 标记完成。
// 快照：等待 frontier >= 目标版本后，即可按该版本读取任意 cell，
// 保证看到的是完整事务边界上的状态，不会混读半个事务。
type commitTracker struct {
	counter  atomic.Uint64
	mu       sync.Mutex
	done     map[uint64]bool
	frontier atomic.Uint64
}

func newCommitTracker() *commitTracker {
	return &commitTracker{done: make(map[uint64]bool)}
}

// assign 分配一个新的提交版本号。
func (t *commitTracker) assign() uint64 {
	return t.counter.Add(1)
}

// published 标记版本 v 的所有 cell 已发布完毕，并推进连续前沿。
func (t *commitTracker) published(v uint64) {
	t.mu.Lock()
	t.done[v] = true
	f := t.frontier.Load()
	for t.done[f+1] {
		f++
		delete(t.done, f)
	}
	t.frontier.Store(f)
	t.mu.Unlock()
}

// waitFrontier 自旋等待连续前沿达到 v。只等待已分配版本的写方完成发布，
// 不阻塞也不等待新发起的写操作。
func (t *commitTracker) waitFrontier(v uint64) {
	for t.frontier.Load() < v {
		runtime.Gosched()
	}
}
