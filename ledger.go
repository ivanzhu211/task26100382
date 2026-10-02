package weightledger

import (
	"errors"
	"sort"
)

var (
	// ErrInsufficientBulk 种植批次散装余额不足。
	ErrInsufficientBulk = errors.New("insufficient bulk weight")
	// ErrInsufficientStock SKU 库存不足。
	ErrInsufficientStock = errors.New("insufficient stock")
	// ErrRefundExceeds 退款数量超过可退数量。
	ErrRefundExceeds = errors.New("refund exceeds sold quantity")
	// ErrSaleNotFound 销售凭据不存在。
	ErrSaleNotFound = errors.New("sale not found")
	// ErrUnknownBatch 未知种植批次。
	ErrUnknownBatch = errors.New("unknown batch")
	// ErrUnknownSKU 未知 SKU。
	ErrUnknownSKU = errors.New("unknown sku")
	// ErrInvalidArgument 非法参数。
	ErrInvalidArgument = errors.New("invalid argument")
)

// NewLedger 构造账本。batches 为种植批次初始重量（厘克），skus 为 SKU 单件净重（厘克）。
// 此后不再新增种植批次或 SKU。
func NewLedger(batches map[BatchID]Centigram, skus map[SKU]Centigram) (*Ledger, error) {
	if len(batches) == 0 || len(skus) == 0 {
		return nil, ErrInvalidArgument
	}
	l := &Ledger{
		batches: make(map[BatchID]*batchState, len(batches)),
		skus:    make(map[SKU]*skuState, len(skus)),
		store: &saleStore{
			sales:   make(map[SaleID]*saleRecord),
			refunds: make(map[RefundID]RefundReceipt),
		},
	}
	for id, w := range batches {
		if w <= 0 {
			return nil, ErrInvalidArgument
		}
		l.batches[id] = &batchState{initial: w, bulk: w}
	}
	for s, net := range skus {
		if net <= 0 {
			return nil, ErrInvalidArgument
		}
		l.skus[s] = &skuState{netPerUnit: net}
	}
	for id := range l.batches {
		l.batchOrder = append(l.batchOrder, id)
	}
	sort.Slice(l.batchOrder, func(i, j int) bool { return l.batchOrder[i] < l.batchOrder[j] })
	for s := range l.skus {
		l.skuOrder = append(l.skuOrder, s)
	}
	sort.Slice(l.skuOrder, func(i, j int) bool { return l.skuOrder[i] < l.skuOrder[j] })
	return l, nil
}

// SetHook 设置测试钩子（非线程安全，须在并发使用前设置）。
func (l *Ledger) SetHook(h func(HookEvent)) {
	l.hook = h
}

func (l *Ledger) fireHook(e HookEvent) {
	if l.hook != nil {
		l.hook(e)
	}
}
