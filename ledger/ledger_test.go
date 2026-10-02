package ledger

import "testing"

func newTestLedger(t *testing.T) *Ledger {
	t.Helper()
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000, "B2": 50000},
		map[SKU]Centigram{"A": 100, "B": 250},
	)
	if err != nil {
		t.Fatalf("NewLedger: %v", err)
	}
	return l
}

func TestBasicFlow(t *testing.T) {
	l := newTestLedger(t)

	lot1, err := l.Package("B1", "A", 10, 50)
	if err != nil {
		t.Fatalf("Package: %v", err)
	}
	if lot1 != 1 {
		t.Fatalf("lot id = %d, want 1", lot1)
	}
	if _, err := l.Package("B1", "A", 100000, 0); err != ErrInsufficientBulk {
		t.Fatalf("want ErrInsufficientBulk, got %v", err)
	}

	rcpt, err := l.Sell(map[SKU]uint64{"A": 4, "B": 0})
	if err == nil {
		t.Fatalf("want error for zero qty, got %v", rcpt)
	}
	sale, err := l.Sell(map[SKU]uint64{"A": 4})
	if err != nil {
		t.Fatalf("Sell: %v", err)
	}
	if sale.TotalUnits("A") != 4 || len(sale.Lines[0].Allocations) != 1 {
		t.Fatalf("bad receipt: %+v", sale)
	}

	ref, err := l.Refund(sale.ID, map[SKU]uint64{"A": 1})
	if err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if ref.SaleID != sale.ID || ref.Lines[0].Allocations[0].Lot != lot1 {
		t.Fatalf("bad refund receipt: %+v", ref)
	}
	if _, err := l.Refund(sale.ID, map[SKU]uint64{"A": 4}); err != ErrRefundExceeds {
		t.Fatalf("want ErrRefundExceeds, got %v", err)
	}

	snap := l.Snapshot()
	if err := snap.CheckInvariants(); err != nil {
		t.Fatalf("invariants: %v", err)
	}
	b1 := snap.Batches["B1"]
	// 初始 100000 = 散装(100000-1000-50) + 在库(7*100) + 净已售(3*100) + 废弃 50
	wantBulk := Centigram(100000 - 1050)
	if b1.BulkRemaining != wantBulk || b1.Waste != 50 {
		t.Fatalf("batch view: %+v", b1)
	}
	if len(snap.Lots) != 1 || snap.Lots[0].Units != 7 {
		t.Fatalf("lots: %+v", snap.Lots)
	}
	if len(snap.Sales) != 1 || snap.Sales[0].Refunded["A"] != 1 {
		t.Fatalf("sales: %+v", snap.Sales)
	}
	if len(snap.Refunds) != 1 {
		t.Fatalf("refunds: %+v", snap.Refunds)
	}

	// 快照不可变：后续写入不影响已返回的快照。
	if _, err := l.Sell(map[SKU]uint64{"A": 1}); err != nil {
		t.Fatalf("Sell: %v", err)
	}
	if snap.Batches["B1"].BulkRemaining != wantBulk || snap.Lots[0].Units != 7 {
		t.Fatalf("snapshot mutated by later write")
	}
}

func TestUnknownArgs(t *testing.T) {
	l := newTestLedger(t)
	if _, err := l.Package("BX", "A", 1, 0); err != ErrUnknownBatch {
		t.Fatalf("got %v", err)
	}
	if _, err := l.Package("B1", "X", 1, 0); err != ErrUnknownSKU {
		t.Fatalf("got %v", err)
	}
	if _, err := l.Sell(map[SKU]uint64{"X": 1}); err != ErrUnknownSKU {
		t.Fatalf("got %v", err)
	}
	if _, err := l.Refund(999, map[SKU]uint64{"A": 1}); err != ErrUnknownSale {
		t.Fatalf("got %v", err)
	}
}
