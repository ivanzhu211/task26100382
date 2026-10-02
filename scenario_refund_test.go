package weightledger

import (
	"reflect"
	"sync"
	"testing"
)

// 同一 SKU 由两个种植批次分装，一笔销售跨两个库存批号。
// 并发提交总请求量超过原销量的部分退款：成功退款累计不得超额，
// 必须回到原来源，原销售凭据不变；补退到全额后来源重量完全冲回。
func TestConcurrentRefundsExceedingSale(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000, "B2": 100000},
		map[SKU]Centigram{"S": 100},
	)
	if err != nil {
		t.Fatalf("NewLedger: %v", err)
	}
	lot1, err := l.Package("B1", "S", 6, 0)
	if err != nil {
		t.Fatalf("Package: %v", err)
	}
	lot2, err := l.Package("B2", "S", 4, 0)
	if err != nil {
		t.Fatalf("Package: %v", err)
	}
	sale, err := l.Sell(map[SKU]int64{"S": 10}) // lot1(6) + lot2(4)
	if err != nil {
		t.Fatalf("Sell: %v", err)
	}
	if len(sale.Lines[0].Lots) != 2 {
		t.Fatalf("expected cross-batch sale, got %+v", sale.Lines[0])
	}

	// 改变现有库存分布：新分装 + 新销售消耗新批号。
	lot3, err := l.Package("B2", "S", 5, 0)
	if err != nil {
		t.Fatalf("Package: %v", err)
	}
	if _, err := l.Sell(map[SKU]int64{"S": 2}); err != nil { // 从 lot3 扣
		t.Fatalf("Sell: %v", err)
	}

	// 并发提交 3 笔各 4 件的部分退款，总量 12 > 10。
	start := make(chan struct{})
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := l.Refund(sale.ID, map[SKU]int64{"S": 4})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()

	succeeded := 0
	for i := 0; i < 3; i++ {
		err := <-errs
		switch err {
		case nil:
			succeeded++
		case ErrRefundExceeds:
		default:
			t.Fatalf("unexpected refund error: %v", err)
		}
	}
	// 4+4+4 超过 10：恰两笔成功，第三笔超额失败。
	if succeeded != 2 {
		t.Fatalf("succeeded=%d, want 2", succeeded)
	}

	snap := l.Snapshot()
	checkInvariant(t, l, snap)
	var refundedTotal int64
	for _, r := range snap.Refunds {
		if r.SaleID != sale.ID {
			t.Fatalf("refund %d linked to wrong sale %d", r.ID, r.SaleID)
		}
		for _, ln := range r.Lines {
			refundedTotal += ln.Total
		}
	}
	if refundedTotal != 8 {
		t.Fatalf("refunded total=%d, want 8 (<= sold 10)", refundedTotal)
	}
	// 退款必须回到原来源批号，而不是当前库存重新选来源。
	returned := make(map[LotID]int64)
	for _, r := range snap.Refunds {
		for _, ln := range r.Lines {
			for _, a := range ln.Lots {
				returned[a.LotID] += a.Units
			}
		}
	}
	if returned[lot3] != 0 {
		t.Fatalf("refund went to uninvolved lot3: %+v", returned)
	}
	if returned[lot1]+returned[lot2] != 8 {
		t.Fatalf("returns not back to original lots: %+v", returned)
	}
	// 原销售凭据不变。
	if !reflect.DeepEqual(snap.Sales[0], sale) {
		t.Fatalf("original sale receipt changed:\n got %+v\nwant %+v", snap.Sales[0], sale)
	}

	// 补退到全额。
	if _, err := l.Refund(sale.ID, map[SKU]int64{"S": 2}); err != nil {
		t.Fatalf("top-up refund: %v", err)
	}
	// 已全额，再退必须失败。
	if _, err := l.Refund(sale.ID, map[SKU]int64{"S": 1}); err != ErrRefundExceeds {
		t.Fatalf("want ErrRefundExceeds after full refund, got %v", err)
	}

	snap = l.Snapshot()
	checkInvariant(t, l, snap)
	// 原销售的各来源重量完全冲回：lot1=6、lot2=4 恢复，lot3 保持第二次销售后的 3。
	want := map[LotID]int64{lot1: 6, lot2: 4, lot3: 3}
	for _, lb := range snap.Lots {
		if lb.Units != want[lb.LotID] {
			t.Fatalf("lot %d balance=%d, want %d", lb.LotID, lb.Units, want[lb.LotID])
		}
	}
	// B1 净已售完全冲回；B2 只剩第二次销售的 2 件。
	if got := snap.Batches["B1"].SoldNet; got != 0 {
		t.Fatalf("B1 SoldNet=%d, want 0", got)
	}
	if got := snap.Batches["B2"].SoldNet; got != 200 {
		t.Fatalf("B2 SoldNet=%d, want 200", got)
	}
}
