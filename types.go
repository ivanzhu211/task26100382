// Package weightledger 实现进程内并发的种植-分装-销售重量账本。
// 重量统一使用整数厘克（1 厘克 = 0.01 克）。
package weightledger

import (
	"sync"
	"sync/atomic"
)

// Centigram 重量单位：厘克，1 厘克 = 0.01 克。
type Centigram int64

// BatchID 种植批次标识。
type BatchID string

// SKU 单品标识。
type SKU string

// LotID 库存批号，唯一递增。
type LotID uint64

// SaleID 销售凭据编号，唯一递增。
type SaleID uint64

// RefundID 退款凭据编号，唯一递增。
type RefundID uint64

// LotAllocation 销售/退款凭据中针对单个库存批号的数量分配。
type LotAllocation struct {
	LotID LotID
	Batch BatchID
	Units int64
}

// SKULine 凭据中一个 SKU 的分配明细，Lots 按库存批号从小到大排列。
type SKULine struct {
	SKU   SKU
	Lots  []LotAllocation
	Total int64
}

// SaleReceipt 不可变销售凭据。
type SaleReceipt struct {
	ID    SaleID
	Lines []SKULine
}

// RefundReceipt 不可变退款凭据，关联原销售。
type RefundReceipt struct {
	ID     RefundID
	SaleID SaleID
	Lines  []SKULine
}

// BatchAccount 种植批次重量账户（快照视图）。
type BatchAccount struct {
	Initial Centigram // 初始重量
	Bulk    Centigram // 剩余散装重量
	InStock Centigram // 在库包装净重
	SoldNet Centigram // 净已售重量（已售减已退）
	Waste   Centigram // 累计废弃重量
}

// LotBalance 库存批号余额（快照视图）。
type LotBalance struct {
	LotID LotID
	Batch BatchID
	SKU   SKU
	Units int64
}

// Snapshot 账本一致视图，返回后不随后续写入变化。
type Snapshot struct {
	Batches map[BatchID]BatchAccount
	Lots    []LotBalance
	Sales   []SaleReceipt
	Refunds []RefundReceipt
}

// batchState 种植批次的可变状态。
type batchState struct {
	mu      sync.Mutex
	version atomic.Uint64
	initial Centigram
	bulk    Centigram
	waste   Centigram
}

// lot 库存批号。
type lot struct {
	id      LotID
	batch   BatchID
	balance int64 // 以整件计
}

// skuState 一个 SKU 的可变状态：净重与库存批号列表（按 LotID 升序）。
type skuState struct {
	mu         sync.Mutex
	version    atomic.Uint64
	netPerUnit Centigram
	lots       []lot
}

// refundProgress 退款进度：沿原销售分配顺序的已退数量。
type refundProgress struct {
	skuRefunded map[SKU]int64
}

// saleRecord 销售记录：不可变凭据 + 可变退款进度。
type saleRecord struct {
	receipt  SaleReceipt
	progress refundProgress
}

// saleStore 销售与退款凭据存储。
type saleStore struct {
	mu      sync.Mutex
	version atomic.Uint64
	sales   map[SaleID]*saleRecord
	refunds map[RefundID]RefundReceipt
}

// HookEvent 测试钩子事件。
type HookEvent int

const (
	// HookPackageChecked 分装余额校验通过、提交之前（持有批次与SKU锁）。
	HookPackageChecked HookEvent = iota
	// HookSellChecked 销售库存校验通过、提交之前（持有相关SKU锁）。
	HookSellChecked
	// HookRefundChecked 退款校验通过、提交之前。
	HookRefundChecked
	// HookSnapshotScan Snapshot 完成版本采样、开始读取数据之前。
	HookSnapshotScan
)

// Ledger 进程内并发重量账本。
type Ledger struct {
	batches    map[BatchID]*batchState
	batchOrder []BatchID // 排序后的批次，供确定性遍历
	skus       map[SKU]*skuState
	skuOrder   []SKU // 排序后的 SKU
	store      *saleStore
	lotSeq     atomic.Uint64
	saleSeq    atomic.Uint64
	refundSeq  atomic.Uint64
	hook       func(HookEvent)
}

// cloneSKULines 深拷贝凭据明细，保证凭据不可变。
func cloneSKULines(lines []SKULine) []SKULine {
	out := make([]SKULine, len(lines))
	for i, ln := range lines {
		out[i] = SKULine{SKU: ln.SKU, Total: ln.Total}
		if ln.Lots != nil {
			out[i].Lots = append([]LotAllocation(nil), ln.Lots...)
		}
	}
	return out
}

func cloneSaleReceipt(r SaleReceipt) SaleReceipt {
	return SaleReceipt{ID: r.ID, Lines: cloneSKULines(r.Lines)}
}

func cloneRefundReceipt(r RefundReceipt) RefundReceipt {
	return RefundReceipt{ID: r.ID, SaleID: r.SaleID, Lines: cloneSKULines(r.Lines)}
}
