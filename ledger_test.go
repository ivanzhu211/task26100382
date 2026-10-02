package weightledger

import "testing"

// checkInvariant 校验快照的硬性强约束：
// 每个批次 初始 = 散装 + 在库净重 + 净已售 + 废弃，各项非负；
// 在库净重与净已售可由快照内的库存余额、销售及退款凭据复核。
func checkInvariant(t *testing.T, l *Ledger, snap Snapshot) {
	t.Helper()
	inStock := make(map[BatchID]Centigram)
	for _, lb := range snap.Lots {
		if lb.Units < 0 {
			t.Fatalf("lot %d negative balance %d", lb.LotID, lb.Units)
		}
		inStock[lb.Batch] += l.skus[lb.SKU].netPerUnit * Centigram(lb.Units)
	}
	soldNet := make(map[BatchID]Centigram)
	for _, sale := range snap.Sales {
		for _, ln := range sale.Lines {
			for _, a := range ln.Lots {
				soldNet[a.Batch] += l.skus[ln.SKU].netPerUnit * Centigram(a.Units)
			}
		}
	}
	for _, refund := range snap.Refunds {
		for _, ln := range refund.Lines {
			for _, a := range ln.Lots {
				soldNet[a.Batch] -= l.skus[ln.SKU].netPerUnit * Centigram(a.Units)
			}
		}
	}
	for id, acc := range snap.Batches {
		for name, v := range map[string]Centigram{"Bulk": acc.Bulk, "InStock": acc.InStock, "SoldNet": acc.SoldNet, "Waste": acc.Waste} {
			if v < 0 {
				t.Fatalf("batch %s negative %s: %d", id, name, v)
			}
		}
		if acc.Initial != acc.Bulk+acc.InStock+acc.SoldNet+acc.Waste {
			t.Fatalf("batch %s weight mismatch: initial=%d bulk=%d instock=%d soldnet=%d waste=%d",
				id, acc.Initial, acc.Bulk, acc.InStock, acc.SoldNet, acc.Waste)
		}
		if acc.InStock != inStock[id] {
			t.Fatalf("batch %s InStock=%d, recomputed from lots=%d", id, acc.InStock, inStock[id])
		}
		if acc.SoldNet != soldNet[id] {
			t.Fatalf("batch %s SoldNet=%d, recomputed from receipts=%d", id, acc.SoldNet, soldNet[id])
		}
	}
}

func newTestLedger(t *testing.T) *Ledger {
	t.Helper()
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000, "B2": 80000},
		map[SKU]Centigram{"S1": 100, "S2": 250},
	)
	if err != nil {
		t.Fatalf("NewLedger: %v", err)
	}
	return l
}

func TestBasicFlow(t *testing.T) {
	l := newTestLedger(t)

	lot1, err := l.Package("B1", "S1", 10, 50) // 10*100+50=1050
	if err != nil {
		t.Fatalf("Package: %v", err)
	}
	lot2, err := l.Package("B2", "S1", 5, 0)
	if err != nil {
		t.Fatalf("Package: %v", err)
	}
	if lot2 <= lot1 {
		t.Fatalf("lot ids not increasing: %d then %d", lot1, lot2)
	}

	sale, err := l.Sell(map[SKU]int64{"S1": 12}) // 跨 lot1(10) + lot2(2)
	if err != nil {
		t.Fatalf("Sell: %v", err)
	}
	if len(sale.Lines) != 1 || len(sale.Lines[0].Lots) != 2 {
		t.Fatalf("expected sale across 2 lots, got %+v", sale.Lines)
	}
	if sale.Lines[0].Lots[0].LotID != lot1 || sale.Lines[0].Lots[0].Units != 10 ||
		sale.Lines[0].Lots[1].LotID != lot2 || sale.Lines[0].Lots[1].Units != 2 {
		t.Fatalf("unexpected allocation: %+v", sale.Lines[0].Lots)
	}

	refund, err := l.Refund(sale.ID, map[SKU]int64{"S1": 12})
	if err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if refund.SaleID != sale.ID {
		t.Fatalf("refund not linked to sale: %+v", refund)
	}

	snap := l.Snapshot()
	checkInvariant(t, l, snap)
	// 全额退款后各来源批次的净已售应为 0，库存恢复原值。
	for id, acc := range snap.Batches {
		if acc.SoldNet != 0 {
			t.Fatalf("batch %s SoldNet=%d after full refund", id, acc.SoldNet)
		}
	}
	for _, lb := range snap.Lots {
		want := map[LotID]int64{lot1: 10, lot2: 5}[lb.LotID]
		if lb.Units != want {
			t.Fatalf("lot %d balance=%d, want %d", lb.LotID, lb.Units, want)
		}
	}
	// 废弃重量不因退款撤销。
	if snap.Batches["B1"].Waste != 50 {
		t.Fatalf("waste=%d, want 50", snap.Batches["B1"].Waste)
	}
}

func TestFailuresLeaveNoTrace(t *testing.T) {
	l := newTestLedger(t)
	if _, err := l.Package("B1", "S1", 10, 0); err != nil {
		t.Fatalf("Package: %v", err)
	}
	before := l.Snapshot()

	if _, err := l.Package("B1", "S1", 1, 1000000); err != ErrInsufficientBulk {
		t.Fatalf("want ErrInsufficientBulk, got %v", err)
	}
	if _, err := l.Sell(map[SKU]int64{"S1": 11}); err != ErrInsufficientStock {
		t.Fatalf("want ErrInsufficientStock, got %v", err)
	}
	if _, err := l.Sell(map[SKU]int64{"S1": 5, "S2": 1}); err != ErrInsufficientStock {
		t.Fatalf("want ErrInsufficientStock, got %v", err)
	}
	sale, err := l.Sell(map[SKU]int64{"S1": 4})
	if err != nil {
		t.Fatalf("Sell: %v", err)
	}
	if _, err := l.Refund(sale.ID, map[SKU]int64{"S1": 5}); err != ErrRefundExceeds {
		t.Fatalf("want ErrRefundExceeds, got %v", err)
	}
	if _, err := l.Refund(SaleID(999), map[SKU]int64{"S1": 1}); err != ErrSaleNotFound {
		t.Fatalf("want ErrSaleNotFound, got %v", err)
	}

	snap := l.Snapshot()
	checkInvariant(t, l, snap)
	// 失败操作不得留下任何变化：与成功销售后的状态对比。
	if len(snap.Sales) != 1 || len(snap.Refunds) != 0 {
		t.Fatalf("failed ops leaked receipts: sales=%d refunds=%d", len(snap.Sales), len(snap.Refunds))
	}
	if snap.Batches["B1"].Bulk != before.Batches["B1"].Bulk {
		t.Fatalf("bulk drifted: %d", snap.Batches["B1"].Bulk)
	}
	for _, lb := range snap.Lots {
		if lb.SKU == "S1" && lb.Units != 6 {
			t.Fatalf("lot balance drifted: %+v", lb)
		}
	}
}

func TestReceiptImmutability(t *testing.T) {
	l := newTestLedger(t)
	if _, err := l.Package("B1", "S1", 10, 0); err != nil {
		t.Fatalf("Package: %v", err)
	}
	sale, err := l.Sell(map[SKU]int64{"S1": 4})
	if err != nil {
		t.Fatalf("Sell: %v", err)
	}
	// 篡改返回的凭据，账本内的记录不得受影响。
	sale.Lines[0].Lots[0].Units = 9999
	sale.Lines[0].Total = 9999
	snap := l.Snapshot()
	if snap.Sales[0].Lines[0].Lots[0].Units != 4 || snap.Sales[0].Lines[0].Total != 4 {
		t.Fatalf("stored receipt mutated: %+v", snap.Sales[0])
	}
	// 快照本身不随后续写入变化。
	if _, err := l.Sell(map[SKU]int64{"S1": 1}); err != nil {
		t.Fatalf("Sell: %v", err)
	}
	if len(snap.Sales) != 1 {
		t.Fatalf("snapshot changed after later write: %d sales", len(snap.Sales))
	}
}
