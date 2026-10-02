package ledger

import "errors"

// Centigram 是重量单位，1 厘克 = 0.01 克。全部重量以整数厘克表示。
type Centigram int64

type BatchID string
type SKU string
type LotID uint64
type SaleID uint64
type RefundID uint64

var (
	ErrUnknownBatch       = errors.New("ledger: unknown batch")
	ErrUnknownSKU         = errors.New("ledger: unknown sku")
	ErrUnknownSale        = errors.New("ledger: unknown sale")
	ErrInsufficientBulk   = errors.New("ledger: insufficient bulk weight")
	ErrInsufficientStock  = errors.New("ledger: insufficient stock")
	ErrRefundExceeds      = errors.New("ledger: refund exceeds refundable quantity")
	ErrInvalidArgument    = errors.New("ledger: invalid argument")
	ErrInvariantViolation = errors.New("ledger: invariant violation")
)

// Allocation 记录某个库存批号被扣减（或退回）的件数。
type Allocation struct {
	Lot   LotID
	Units uint64
}

// SaleLine 是一笔销售中某个 SKU 的实际扣减明细。
type SaleLine struct {
	SKU         SKU
	Units       uint64
	Allocations []Allocation
}

// SaleReceipt 是不可变销售凭据。
type SaleReceipt struct {
	ID    SaleID
	Lines []SaleLine
}

// TotalUnits 返回凭据中某 SKU 的总件数。
func (r SaleReceipt) TotalUnits(sku SKU) uint64 {
	var n uint64
	for _, ln := range r.Lines {
		if ln.SKU == sku {
			n += ln.Units
		}
	}
	return n
}

// RefundLine 是一笔退款中某个 SKU 的实际退回明细。
type RefundLine struct {
	SKU         SKU
	Units       uint64
	Allocations []Allocation
}

// RefundReceipt 是不可变退款凭据，关联原销售。
type RefundReceipt struct {
	ID     RefundID
	SaleID SaleID
	Lines  []RefundLine
}

// BatchView 是快照中一个种植批次的重量账户。
type BatchView struct {
	Initial       Centigram
	BulkRemaining Centigram
	Waste         Centigram
}

// LotView 是快照中一个库存批号的余额。
type LotView struct {
	Lot       LotID
	Batch     BatchID
	SKU       SKU
	Units     uint64
	NetWeight Centigram
}

// SaleView 是快照中一笔销售及其累计已退数量。
type SaleView struct {
	Receipt  SaleReceipt
	Refunded map[SKU]uint64
}

// Snapshot 是账本的一致视图，返回后不随后续写入变化。
type Snapshot struct {
	Batches map[BatchID]BatchView
	Lots    []LotView
	Sales   []SaleView
	Refunds []RefundReceipt
}
