package ledger

import (
	"sync"
	"testing"
	"time"
)

// 场景一：两笔销售以相反 SKU 排列竞争同一组库存，库存只够一笔完整销售。
// 要求恰好一笔成功、无死锁；失败销售不留下对先访问 SKU 的扣减。
func TestConcurrentSalesOppositeOrder(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000},
		map[SKU]Centigram{"A": 100, "B": 100},
	)
	if err != nil {
		t.Fatal(err)
	}
	// A 有 10 件、B 只有 5 件：两笔各要 {A:5, B:5}，只有一笔能完整成交。
	// 失败笔在校验阶段会先看到 A 充足、再发现 B 不足，
	// 若实现不当会把 A 的扣减留下。
	if _, err := l.Package("B1", "A", 10, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Package("B1", "B", 5, 0); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	// 两笔销售传入的 SKU 排列相反（map 字面量顺序不同）。
	for _, lines := range []map[SKU]uint64{
		{"A": 5, "B": 5},
		{"B": 5, "A": 5},
	} {
		wg.Add(1)
		go func(lines map[SKU]uint64) {
			defer wg.Done()
			<-start
			_, err := l.Sell(lines)
			results <- err
		}(lines)
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: sales did not finish")
	}
	close(results)

	var successes, failures int
	for err := range results {
		if err == nil {
			successes++
		} else if err == ErrInsufficientStock {
			failures++
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("successes=%d failures=%d, want 1/1", successes, failures)
	}

	snap := l.Snapshot()
	if err := snap.CheckInvariants(); err != nil {
		t.Fatalf("invariants: %v", err)
	}
	// 失败笔不得留下扣减：A 应剩 5 件（仅成功笔扣了 5），B 剩 0。
	stock := make(map[SKU]uint64)
	for _, lv := range snap.Lots {
		stock[lv.SKU] += lv.Units
	}
	if stock["A"] != 5 || stock["B"] != 0 {
		t.Fatalf("stock A=%d B=%d, want 5/0 (loser must not deduct)", stock["A"], stock["B"])
	}
	// 快照中恰有一笔销售凭据，无退款。
	if len(snap.Sales) != 1 || len(snap.Refunds) != 0 {
		t.Fatalf("sales=%d refunds=%d", len(snap.Sales), len(snap.Refunds))
	}
	sale := snap.Sales[0]
	if sale.Receipt.TotalUnits("A") != 5 || sale.Receipt.TotalUnits("B") != 5 {
		t.Fatalf("receipt: %+v", sale.Receipt)
	}
	// 重量复核：B1 初始 100000 = 散装 98500 + 在库 500 + 净已售 1000 + 废弃 0。
	b := snap.Batches["B1"]
	if b.BulkRemaining != 98500 || b.Waste != 0 {
		t.Fatalf("batch view: %+v", b)
	}
}
