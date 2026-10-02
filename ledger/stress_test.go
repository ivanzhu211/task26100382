package ledger

import (
	"sync"
	"testing"
)

// 混合并发压力测试：多协程并发分装/销售/退款/快照，
// 每次快照都必须满足账务不变量。
func TestMixedConcurrencyStress(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000000, "B2": 100000000, "B3": 100000000},
		map[SKU]Centigram{"A": 100, "B": 250, "C": 40},
	)
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var snapWG sync.WaitGroup
	for i := 0; i < 2; i++ {
		snapWG.Add(1)
		go func() {
			defer snapWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := l.Snapshot().CheckInvariants(); err != nil {
					t.Errorf("invariants: %v", err)
					return
				}
			}
		}()
	}

	batches := []BatchID{"B1", "B2", "B3"}
	skus := []SKU{"A", "B", "C"}
	saleIDs := make(chan SaleID, 4096)

	var writeWG sync.WaitGroup
	for w := 0; w < 4; w++ {
		writeWG.Add(1)
		go func(w int) {
			defer writeWG.Done()
			for i := 0; i < 300; i++ {
				b := batches[(w+i)%len(batches)]
				s := skus[(w+i)%len(skus)]
				if _, err := l.Package(b, s, 5, Centigram(i%7)); err != nil {
					t.Errorf("package: %v", err)
					return
				}
				r, err := l.Sell(map[SKU]uint64{s: 3})
				if err != nil {
					t.Errorf("sell: %v", err)
					return
				}
				saleIDs <- r.ID
			}
		}(w)
	}

	var refundWG sync.WaitGroup
	refundWG.Add(1)
	go func() {
		defer refundWG.Done()
		for id := range saleIDs {
			// 尝试对每个 SKU 退 1 件；未售出的 SKU 会整体失败，属预期。
			_, _ = l.Refund(id, map[SKU]uint64{"A": 1, "B": 1, "C": 1})
		}
	}()

	writeWG.Wait()
	close(saleIDs)
	refundWG.Wait()
	close(stop)
	snapWG.Wait()

	if err := l.Snapshot().CheckInvariants(); err != nil {
		t.Fatalf("final invariants: %v", err)
	}
}
