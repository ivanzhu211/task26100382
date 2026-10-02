package ledger

import (
	"sort"
	"sync"
	"sync/atomic"
)

// batchState 是种植批次重量账户的可变部分。
type batchState struct {
	bulk  Centigram // 剩余散装重量
	waste Centigram // 累计废弃重量
}

// batchAccount 初始重量不可变，状态为版本化单元格。
type batchAccount struct {
	initial Centigram
	mu      sync.Mutex
	state   *cell[batchState]
}

// lot 是一个库存批号：来源批次不可变，余额（整件）为版本化单元格。
// 余额只在持有所属 SKU 锁时被修改。
type lot struct {
	id      LotID
	batch   BatchID
	sku     SKU
	balance *cell[uint64]
}

// skuState 单件净重不可变；库存批号列表只增不减，按批号升序，整体为版本化单元格。
type skuState struct {
	netWeight Centigram
	mu        sync.Mutex
	lots      *cell[[]*lot] // 不可变切片，追加时整体替换
}

// saleState 销售凭据不可变；每个 SKU 的累计已退数量为版本化单元格，
// 只在持有本销售锁时修改。
type saleState struct {
	mu       sync.Mutex
	receipt  SaleReceipt
	refunded map[SKU]*cell[uint64]
}

// saleNode / refundNode 构成只增链表。不同写事务可能并发追加，
// 由 listsMu 串行化链接；节点携带提交版本，快照按版本过滤。
type saleNode struct {
	version uint64
	sale    *saleState
	next    *saleNode
}

type refundNode struct {
	version uint64
	receipt RefundReceipt
	next    *refundNode
}

// testHooks 供测试用屏障控制交错，不得在生产路径传递非 nil 钩子。
type testHooks struct {
	// afterPackageValidate 在 Package 余额校验通过、尚未提交时调用。
	afterPackageValidate func()
	// afterCommitVersion 在写事务取得提交版本号、尚未发布任何 cell 时调用。
	afterCommitVersion func()
	// afterFirstPublish 在写事务发布第一个 cell 后调用。
	afterFirstPublish func()
	// beforeCommit 在写事务进入提交流程、分配版本号之前调用。
	beforeCommit func()
	// snapshotWaitStart 在快照读到目标版本、等待发布前沿之前调用。
	snapshotWaitStart func(version uint64)
}

// Ledger 是进程内并发重量账本。
//
// 并发设计：
//   - 写操作只锁涉及的种植批次 / SKU / 原销售，按固定顺序加锁，无全局业务锁。
//   - 每个可变数据是版本化 cell；写事务在持锁状态下取得提交版本号、
//     发布全部 cell、标记发布后释放锁。
//   - Snapshot 不加任何业务锁：读取提交计数为目标版本，等待连续发布前沿
//     达到该版本后，按版本读取各 cell，得到事务边界上的一致视图。
type Ledger struct {
	batches map[BatchID]*batchAccount
	skus    map[SKU]*skuState

	salesMu   sync.Mutex // 仅保护 salesByID 映射本身
	salesByID map[SaleID]*saleState
	listsMu   sync.Mutex                 // 串行化 sales/refunds 共享链表的追加
	sales     atomic.Pointer[saleNode]   // 只增链表，头插
	refunds   atomic.Pointer[refundNode] // 只增链表，头插

	commits *commitTracker

	nextLotID    atomic.Uint64
	nextSaleID   atomic.Uint64
	nextRefundID atomic.Uint64

	hooks atomic.Pointer[testHooks]
}

// NewLedger 以给定的种植批次初始重量和 SKU 单件净重构造账本。
// 此后不再新增批次或 SKU。初始重量与净重必须为正。
func NewLedger(batches map[BatchID]Centigram, skus map[SKU]Centigram) (*Ledger, error) {
	l := &Ledger{
		batches:   make(map[BatchID]*batchAccount, len(batches)),
		skus:      make(map[SKU]*skuState, len(skus)),
		salesByID: make(map[SaleID]*saleState),
		commits:   newCommitTracker(),
	}
	for id, w := range batches {
		if w <= 0 {
			return nil, ErrInvalidArgument
		}
		l.batches[id] = &batchAccount{initial: w, state: newCell(batchState{bulk: w})}
	}
	for id, w := range skus {
		if w <= 0 {
			return nil, ErrInvalidArgument
		}
		l.skus[id] = &skuState{netWeight: w, lots: newCell([]*lot(nil))}
	}
	return l, nil
}

// setTestHooks 安装测试钩子（仅测试使用）。
func (l *Ledger) setTestHooks(h *testHooks) {
	l.hooks.Store(h)
}

func (l *Ledger) getHooks() *testHooks {
	return l.hooks.Load()
}

// appendSale / appendRefund 在提交阶段把新节点链接到共享链表头。
// listsMu 是叶子锁（不再嵌套其他锁），只保护链接本身，不串行化业务写。
func (l *Ledger) appendSale(version uint64, sale *saleState) {
	l.listsMu.Lock()
	l.sales.Store(&saleNode{version: version, sale: sale, next: l.sales.Load()})
	l.listsMu.Unlock()
}

func (l *Ledger) appendRefund(version uint64, r RefundReceipt) {
	l.listsMu.Lock()
	l.refunds.Store(&refundNode{version: version, receipt: r, next: l.refunds.Load()})
	l.listsMu.Unlock()
}

// lockSKUs 按字典序锁定一组 SKU（去重后），保证所有写方加锁顺序一致。
func (l *Ledger) lockSKUs(set map[SKU]bool) ([]SKU, func()) {
	ordered := make([]SKU, 0, len(set))
	for s := range set {
		ordered = append(ordered, s)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, s := range ordered {
		l.skus[s].mu.Lock()
	}
	return ordered, func() {
		for _, s := range ordered {
			l.skus[s].mu.Unlock()
		}
	}
}

// commit 是写事务的统一提交流程：分配提交版本号 -> 依次发布全部 cell ->
// 标记发布完成。调用时必须持有本事务全部业务锁。
// 每个 store 闭包把一个带版本的新值写入对应 cell。
func (l *Ledger) commit(stores ...func(version uint64)) {
	if h := l.getHooks(); h != nil && h.beforeCommit != nil {
		h.beforeCommit()
	}
	version := l.commits.assign()
	if h := l.getHooks(); h != nil && h.afterCommitVersion != nil {
		h.afterCommitVersion()
	}
	for i, store := range stores {
		store(version)
		if i == 0 {
			if h := l.getHooks(); h != nil && h.afterFirstPublish != nil {
				h.afterFirstPublish()
			}
		}
	}
	l.commits.published(version)
}
