package ledger

import (
	"testing"
	"time"
)

// 场景三（上）：一笔分装在校验通过、提交前被暂停，
// 不相交的写操作（不同批次与 SKU）必须能完成。
func TestPausedPackageDoesNotBlockDisjointWrites(t *testing.T) {
	l, err := NewLedger(
		map[BatchID]Centigram{"B1": 100000, "B2": 100000},
		map[SKU]Centigram{"S": 100, "T": 100},
	)
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	// 钩子只拦住第一笔分装；后续（不相交的）分装直接放行。
	l.setTestHooks(&testHooks{
		afterPackageValidate: func() {
			select {
			case <-entered:
			default:
				close(entered)
				<-release
			}
		},
	})

	pkgDone := make(chan error, 1)
	go func() {
		_, err := l.Package("B1", "S", 5, 0)
		pkgDone <- err
	}()
	<-entered // 分装已暂停在"校验通过、尚未提交"

	// 不相交的写：另一批次、另一 SKU 的分装与销售必须能完成。
	disjointDone := make(chan error, 1)
	go func() {
		if _, err := l.Package("B2", "T", 3, 0); err != nil {
			disjointDone <- err
			return
		}
		_, err := l.Sell(map[SKU]uint64{"T": 1})
		disjointDone <- err
	}()
	select {
	case err := <-disjointDone:
		if err != nil {
			t.Fatalf("disjoint write failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("disjoint write blocked by paused package")
	}

	// 快照也不应被暂停的分装阻塞。
	snapDone := make(chan Snapshot, 1)
	go func() { snapDone <- l.Snapshot() }()
	select {
	case snap := <-snapDone:
		if err := snap.CheckInvariants(); err != nil {
			t.Fatalf("invariants: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("snapshot blocked by paused package")
	}

	close(release)
	if err := <-pkgDone; err != nil {
		t.Fatalf("paused package failed: %v", err)
	}
	l.setTestHooks(nil)

	snap := l.Snapshot()
	if err := snap.CheckInvariants(); err != nil {
		t.Fatalf("invariants: %v", err)
	}
	if snap.Batches["B1"].BulkRemaining != 100000-500 {
		t.Fatalf("B1: %+v", snap.Batches["B1"])
	}
}

// 场景三（下）：跨批次销售发生在快照读取期间，
// 快照只能对应整笔交易之前或之后，不得混合两个时刻。
func TestSnapshotDuringCrossBatchSale(t *testing.T) {
	newLedger := func(t *testing.T) *Ledger {
		l, err := NewLedger(
			map[BatchID]Centigram{"B1": 100000, "B2": 100000},
			map[SKU]Centigram{"S": 100},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := l.Package("B1", "S", 5, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := l.Package("B2", "S", 5, 0); err != nil {
			t.Fatal(err)
		}
		return l
	}
	lotBalance := func(snap Snapshot, id LotID) uint64 {
		for _, lv := range snap.Lots {
			if lv.Lot == id {
				return lv.Units
			}
		}
		return ^uint64(0)
	}

	// 情形 1：销售已发布第一个 cell 时暂停，快照必须等到事务完整提交，
	// 返回"之后"的视图，不得返回混合状态。
	t.Run("after", func(t *testing.T) {
		l := newLedger(t)
		paused := make(chan struct{})
		release := make(chan struct{})
		snapWaiting := make(chan uint64, 1)
		l.setTestHooks(&testHooks{
			afterFirstPublish: func() {
				close(paused)
				<-release
			},
			snapshotWaitStart: func(v uint64) { snapWaiting <- v },
		})

		saleDone := make(chan SaleReceipt, 1)
		go func() {
			r, err := l.Sell(map[SKU]uint64{"S": 8})
			if err != nil {
				t.Errorf("sell: %v", err)
			}
			saleDone <- r
		}()
		<-paused // 销售已暂停在提交中途：批号1 已扣、批号2 未扣

		snapDone := make(chan Snapshot, 1)
		go func() { snapDone <- l.Snapshot() }()
		<-snapWaiting // 快照已读到目标版本，正在等待发布前沿

		// 销售事务未标记发布前，快照不可能返回。
		select {
		case snap := <-snapDone:
			t.Fatalf("snapshot returned mid-transaction: %+v", snap)
		default:
		}

		close(release)
		sale := <-saleDone
		l.setTestHooks(nil)

		var snap Snapshot
		select {
		case snap = <-snapDone:
		case <-time.After(10 * time.Second):
			t.Fatal("snapshot did not return after sale committed")
		}
		if err := snap.CheckInvariants(); err != nil {
			t.Fatalf("invariants: %v", err)
		}
		// 必须是完整的"之后"视图：批号1=0、批号2=2，且含该销售凭据。
		if lotBalance(snap, 1) != 0 || lotBalance(snap, 2) != 2 {
			t.Fatalf("mixed or stale lots: lot1=%d lot2=%d",
				lotBalance(snap, 1), lotBalance(snap, 2))
		}
		if len(snap.Sales) != 1 || snap.Sales[0].Receipt.ID != sale.ID {
			t.Fatalf("snapshot missing committed sale: %+v", snap.Sales)
		}
	})

	// 情形 2：销售在分配提交版本号之前暂停，快照对应"之前"的视图。
	t.Run("before", func(t *testing.T) {
		l := newLedger(t)
		paused := make(chan struct{})
		release := make(chan struct{})
		l.setTestHooks(&testHooks{
			beforeCommit: func() {
				close(paused)
				<-release
			},
		})

		saleDone := make(chan struct{})
		go func() {
			defer close(saleDone)
			if _, err := l.Sell(map[SKU]uint64{"S": 8}); err != nil {
				t.Errorf("sell: %v", err)
			}
		}()
		<-paused // 销售已完成扣减计算，尚未进入提交

		snap := l.Snapshot() // 不得被阻塞，且必须是"之前"的视图
		if err := snap.CheckInvariants(); err != nil {
			t.Fatalf("invariants: %v", err)
		}
		if lotBalance(snap, 1) != 5 || lotBalance(snap, 2) != 5 {
			t.Fatalf("pre-sale snapshot has wrong lots: lot1=%d lot2=%d",
				lotBalance(snap, 1), lotBalance(snap, 2))
		}
		if len(snap.Sales) != 0 {
			t.Fatalf("pre-sale snapshot contains sale: %+v", snap.Sales)
		}

		close(release)
		<-saleDone
		l.setTestHooks(nil)
	})
}
