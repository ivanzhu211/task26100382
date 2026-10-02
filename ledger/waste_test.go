package ledger

import "testing"

// 退款不撤销原分装的废弃重量。
func TestRefundDoesNotUndoWaste(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 10000},
		map[SKU]Centigram{"A": 100},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Package("B1", "A", 10, 250); err != nil {
		t.Fatal(err)
	}
	sale, err := l.Sell(map[SKU]uint64{"A": 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Refund(sale.ID, map[SKU]uint64{"A": 10}); err != nil {
		t.Fatal(err)
	}
	snap := l.Snapshot()
	if err := snap.CheckInvariants(); err != nil {
		t.Fatalf("invariants: %v", err)
	}
	b := snap.Batches["B1"]
	if b.Waste != 250 {
		t.Fatalf("waste=%d, want 250 (refund must not undo waste)", b.Waste)
	}
	// 全额退款后：散装 8750 + 在库 1000 + 净已售 0 + 废弃 250 = 10000。
	if b.BulkRemaining != 8750 {
		t.Fatalf("bulk=%d, want 8750", b.BulkRemaining)
	}
	if snap.Lots[0].Units != 10 {
		t.Fatalf("lot units=%d, want 10 (returned goods resalable)", snap.Lots[0].Units)
	}
	// 退回商品可再次销售。
	if _, err := l.Sell(map[SKU]uint64{"A": 10}); err != nil {
		t.Fatalf("resell returned goods: %v", err)
	}
	if err := l.Snapshot().CheckInvariants(); err != nil {
		t.Fatalf("invariants after resell: %v", err)
	}
}
