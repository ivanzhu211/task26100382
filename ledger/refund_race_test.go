package ledger

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

// 场景二：同一 SKU 由两个批次分装，一笔销售跨两个库存批号；
// 改变库存分布后并发提交多笔部分退款，请求总量超过原销量。
func TestConcurrentRefundsExceedingSale(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000, "B2": 100000},
		map[SKU]Centigram{"S": 100},
	)
	if err != nil {
		t.Fatal(err)
	}
	lot1, err := l.Package("B1", "S", 5, 0) // 批号1：5 件，来自 B1
	if err != nil {
		t.Fatal(err)
	}
	lot2, err := l.Package("B2", "S", 5, 0) // 批号2：5 件，来自 B2
	if err != nil {
		t.Fatal(err)
	}

	// 跨批号销售 8 件：批号1 出 5、批号2 出 3。
	sale, err := l.Sell(map[SKU]uint64{"S": 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(sale.Lines) != 1 || len(sale.Lines[0].Allocations) != 2 {
		t.Fatalf("want cross-lot sale, got %+v", sale)
	}
	original := cloneSaleReceipt(sale)

	// 改变现有库存分布：再分装批号3，并把批号2剩余 2 件卖光。
	if _, err := l.Package("B1", "S", 3, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Sell(map[SKU]uint64{"S": 2}); err != nil {
		t.Fatal(err)
	}

	// 4 笔并发部分退款，各退 3 件，总量 12 > 原销量 8。
	const refunders = 4
	const perRefund = 3
	start := make(chan struct{})
	type outcome struct {
		receipt RefundReceipt
		err     error
	}
	results := make(chan outcome, refunders)
	var wg sync.WaitGroup
	for i := 0; i < refunders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, err := l.Refund(sale.ID, map[SKU]uint64{"S": perRefund})
			results <- outcome{r, err}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: refunds did not finish")
	}

	var succeeded []RefundReceipt
	for i := 0; i < refunders; i++ {
		o := <-results
		if o.err == nil {
			succeeded = append(succeeded, o.receipt)
		} else if o.err != ErrRefundExceeds {
			t.Fatalf("unexpected error: %v", o.err)
		}
	}
	var totalRefunded uint64
	for _, r := range succeeded {
		totalRefunded += r.Lines[0].Units
	}
	if totalRefunded > 8 {
		t.Fatalf("refunded %d > sold 8", totalRefunded)
	}
	// 3 件一笔的退款最多成功两笔（6 件），第三笔必超额失败。
	if len(succeeded) != 2 || totalRefunded != 6 {
		t.Fatalf("succeeded=%d total=%d, want 2/6", len(succeeded), totalRefunded)
	}

	// 退款必须回到原来源批号，不得落到新批号3。
	returned := make(map[LotID]uint64)
	for _, r := range succeeded {
		for _, a := range r.Lines[0].Allocations {
			returned[a.Lot] += a.Units
		}
	}
	for lotID := range returned {
		if lotID != lot1 && lotID != lot2 {
			t.Fatalf("refund landed on wrong lot %d", lotID)
		}
	}

	// 原销售凭据不得被改写。
	if !reflect.DeepEqual(sale, original) {
		t.Fatalf("original receipt mutated: %+v vs %+v", sale, original)
	}

	snap := l.Snapshot()
	if err := snap.CheckInvariants(); err != nil {
		t.Fatalf("invariants: %v", err)
	}
	view := findSaleView(t, snap, sale.ID)
	if view.Refunded["S"] != 6 {
		t.Fatalf("refunded=%d, want 6", view.Refunded["S"])
	}
	if !reflect.DeepEqual(view.Receipt, original) {
		t.Fatalf("snapshot receipt differs from original")
	}

	// 补退剩余 2 件到全额。
	final, err := l.Refund(sale.ID, map[SKU]uint64{"S": 2})
	if err != nil {
		t.Fatalf("final refund: %v", err)
	}
	if final.Lines[0].Units != 2 {
		t.Fatalf("final refund: %+v", final)
	}
	// 再退任何数量都必须失败。
	if _, err := l.Refund(sale.ID, map[SKU]uint64{"S": 1}); err != ErrRefundExceeds {
		t.Fatalf("want ErrRefundExceeds, got %v", err)
	}

	snap = l.Snapshot()
	if err := snap.CheckInvariants(); err != nil {
		t.Fatalf("invariants: %v", err)
	}
	// 原销售各来源重量完全冲回：批号1 恢复 5 件；
	// 批号2 恢复 3 件（另 2 件属第二笔销售，未退）。
	bal := make(map[LotID]uint64)
	for _, lv := range snap.Lots {
		bal[lv.Lot] = lv.Units
	}
	if bal[lot1] != 5 || bal[lot2] != 3 {
		t.Fatalf("lot balances: lot1=%d lot2=%d, want 5/3", bal[lot1], bal[lot2])
	}
	// 全账本净已售只剩第二笔销售的 2 件（来自批号2/B2）。
	if findSaleView(t, snap, sale.ID).Refunded["S"] != 8 {
		t.Fatalf("refunded=%d, want 8", findSaleView(t, snap, sale.ID).Refunded["S"])
	}
	b2 := snap.Batches["B2"]
	// B2：散装 99500 + 在库 3*100 + 净已售 2*100（第二笔销售未退）= 100000。
	if b2.BulkRemaining+300+200 != b2.Initial {
		t.Fatalf("B2 weight account inconsistent: %+v", b2)
	}
}

func findSaleView(t *testing.T, snap Snapshot, id SaleID) SaleView {
	t.Helper()
	for _, sv := range snap.Sales {
		if sv.Receipt.ID == id {
			return sv
		}
	}
	t.Fatalf("sale %d not in snapshot", id)
	return SaleView{}
}
