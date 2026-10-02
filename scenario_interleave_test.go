package weightledger

import (
	"sync/atomic"
	"testing"
	"time"
)

// 暂停一笔已完成余额校验但尚未提交的分装，
// 证明不相交的写操作（不共享 SKU、种植批次）仍可完成。
func TestPausedPackageAllowsDisjointWrites(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000, "B2": 100000},
		map[SKU]Centigram{"S1": 100, "S2": 100},
	)
	if err != nil {
		t.Fatalf("NewLedger: %v", err)
	}
	if _, err := l.Package("B2", "S2", 5, 0); err != nil {
		t.Fatalf("Package: %v", err)
	}

	paused := make(chan struct{})
	release := make(chan struct{})
	var hooked atomic.Bool
	l.SetHook(func(e HookEvent) {
		if e == HookPackageChecked && hooked.CompareAndSwap(false, true) {
			close(paused)
			<-release
		}
	})

	pkgDone := make(chan error, 1)
	go func() {
		_, err := l.Package("B1", "S1", 10, 10) // 校验通过后暂停在提交前
		pkgDone <- err
	}()
	<-paused // 确认分装已暂停在“校验通过、提交之前”

	// 不相交的写操作必须能完成，不得被暂停的分装阻塞。
	disjointDone := make(chan error, 2)
	go func() {
		_, err := l.Package("B2", "S2", 3, 0)
		disjointDone <- err
	}()
	go func() {
		_, err := l.Sell(map[SKU]int64{"S2": 2})
		disjointDone <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case err := <-disjointDone:
			if err != nil {
				t.Fatalf("disjoint write failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("disjoint write blocked by paused package")
		}
	}

	close(release)
	if err := <-pkgDone; err != nil {
		t.Fatalf("paused package failed: %v", err)
	}

	snap := l.Snapshot()
	checkInvariant(t, l, snap)
	if got := snap.Batches["B1"].InStock; got != 1000 {
		t.Fatalf("B1 InStock=%d, want 1000", got)
	}
	if got := snap.Batches["B1"].Waste; got != 10 {
		t.Fatalf("B1 Waste=%d, want 10", got)
	}
	if got := snap.Batches["B2"].SoldNet; got != 200 {
		t.Fatalf("B2 SoldNet=%d, want 200", got)
	}
}

// 用受控交错让跨批次销售发生在快照读取期间：
// 快照只能对应整笔交易之前或之后，不能混合两个时刻的库存与凭据。
func TestSnapshotConsistentDuringSale(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000, "B2": 100000},
		map[SKU]Centigram{"S": 100},
	)
	if err != nil {
		t.Fatalf("NewLedger: %v", err)
	}
	if _, err := l.Package("B1", "S", 6, 0); err != nil {
		t.Fatalf("Package: %v", err)
	}
	if _, err := l.Package("B2", "S", 4, 0); err != nil {
		t.Fatalf("Package: %v", err)
	}

	// 快照在版本采样后、读取数据前暂停；此时让跨批次销售完成提交。
	scanning := make(chan struct{})
	release := make(chan struct{})
	var hooked atomic.Bool
	l.SetHook(func(e HookEvent) {
		if e == HookSnapshotScan && hooked.CompareAndSwap(false, true) {
			close(scanning)
			<-release
		}
	})

	snapCh := make(chan Snapshot, 1)
	go func() { snapCh <- l.Snapshot() }()
	<-scanning

	sale, err := l.Sell(map[SKU]int64{"S": 10}) // 跨 B1、B2 两个库存批号
	if err != nil {
		t.Fatalf("Sell: %v", err)
	}
	close(release)

	var snap Snapshot
	select {
	case snap = <-snapCh:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot did not finish")
	}

	// 快照必须内部一致：要么完全不包含该销售，要么完整包含。
	checkInvariant(t, l, snap)
	var stockUnits, soldUnits int64
	for _, lb := range snap.Lots {
		stockUnits += lb.Units
	}
	containsSale := false
	for _, s := range snap.Sales {
		if s.ID == sale.ID {
			containsSale = true
		}
		for _, ln := range s.Lines {
			soldUnits += ln.Total
		}
	}
	if stockUnits+soldUnits != 10 {
		t.Fatalf("mixed view: stock=%d sold=%d, total=%d, want 10", stockUnits, soldUnits, stockUnits+soldUnits)
	}
	// 销售在快照读取开始前已提交，重试后的快照应完整包含它。
	if !containsSale {
		t.Fatal("snapshot missing the committed sale")
	}
	if soldUnits != 10 || stockUnits != 0 {
		t.Fatalf("want after-sale view stock=0 sold=10, got stock=%d sold=%d", stockUnits, soldUnits)
	}
}
