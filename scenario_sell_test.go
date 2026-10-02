package weightledger

import (
	"sync"
	"testing"
	"time"
)

// 两笔销售以相反的 SKU 排列访问同一组库存，库存只够一笔完整销售。
// 必须恰好一笔成功、无死锁，失败销售不能留下部分扣减。
func TestConcurrentSellOppositeOrder(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000},
		map[SKU]Centigram{"A": 100, "B": 100},
	)
	if err != nil {
		t.Fatalf("NewLedger: %v", err)
	}
	// A、B 各 5 件；两笔销售各要 A:5+B:5，只够一笔。
	if _, err := l.Package("B1", "A", 5, 0); err != nil {
		t.Fatalf("Package: %v", err)
	}
	if _, err := l.Package("B1", "B", 5, 0); err != nil {
		t.Fatalf("Package: %v", err)
	}

	start := make(chan struct{})
	type result struct {
		receipt SaleReceipt
		err     error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	// 两笔销售以相反的 SKU 排列提交（map 遍历顺序不同，内部按排序加锁）。
	for _, lines := range []map[SKU]int64{
		{"A": 5, "B": 5},
		{"B": 5, "A": 5},
	} {
		wg.Add(1)
		go func(lines map[SKU]int64) {
			defer wg.Done()
			<-start
			r, err := l.Sell(lines)
			results <- result{r, err}
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

	var successes, insufficient int
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err == nil {
			successes++
		} else if r.err == ErrInsufficientStock {
			insufficient++
		} else {
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if successes != 1 || insufficient != 1 {
		t.Fatalf("successes=%d insufficient=%d, want exactly 1 each", successes, insufficient)
	}

	snap := l.Snapshot()
	checkInvariant(t, l, snap)
	// 失败销售不得留下任何扣减或凭据：恰好一笔销售凭据，两个 SKU 各卖 5 件。
	if len(snap.Sales) != 1 {
		t.Fatalf("got %d sales, want 1", len(snap.Sales))
	}
	for _, lb := range snap.Lots {
		if lb.Units != 0 {
			t.Fatalf("lot %d balance=%d, want 0 (winner took all, loser left nothing)", lb.LotID, lb.Units)
		}
	}
	if snap.Batches["B1"].SoldNet != 1000 {
		t.Fatalf("SoldNet=%d, want 1000", snap.Batches["B1"].SoldNet)
	}
}
