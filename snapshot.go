package weightledger

import "sort"

// Snapshot 返回账本一致视图。
// 不持有全局锁：先采样各分区版本号，再逐个分区在局部锁下读取，
// 最后复核版本号；若期间有任何写入提交则重试。
func (l *Ledger) Snapshot() Snapshot {
	for {
		v0 := l.versionSum()
		l.fireHook(HookSnapshotScan)
		snap := l.readAll()
		if l.versionSum() == v0 {
			return snap
		}
	}
}

// versionSum 汇总所有分区的版本号。版本号单调递增，
// 总和不变当且仅当没有任何分区发生提交。
func (l *Ledger) versionSum() uint64 {
	var sum uint64
	for _, id := range l.batchOrder {
		sum += l.batches[id].version.Load()
	}
	for _, s := range l.skuOrder {
		sum += l.skus[s].version.Load()
	}
	sum += l.store.version.Load()
	return sum
}

// readAll 逐个分区在局部锁下读取数据并组装视图。
func (l *Ledger) readAll() Snapshot {
	snap := Snapshot{
		Batches: make(map[BatchID]BatchAccount, len(l.batches)),
	}
	for _, id := range l.batchOrder {
		b := l.batches[id]
		b.mu.Lock()
		snap.Batches[id] = BatchAccount{Initial: b.initial, Bulk: b.bulk, Waste: b.waste}
		b.mu.Unlock()
	}
	for _, s := range l.skuOrder {
		st := l.skus[s]
		st.mu.Lock()
		net := st.netPerUnit
		for _, lt := range st.lots {
			snap.Lots = append(snap.Lots, LotBalance{LotID: lt.id, Batch: lt.batch, SKU: s, Units: lt.balance})
			acc := snap.Batches[lt.batch]
			acc.InStock += net * Centigram(lt.balance)
			snap.Batches[lt.batch] = acc
		}
		st.mu.Unlock()
	}
	l.store.mu.Lock()
	for _, rec := range l.store.sales {
		snap.Sales = append(snap.Sales, cloneSaleReceipt(rec.receipt))
	}
	for _, r := range l.store.refunds {
		snap.Refunds = append(snap.Refunds, cloneRefundReceipt(r))
	}
	l.store.mu.Unlock()

	// 由同一视图中的销售与退款凭据复核净已售重量。
	for _, sale := range snap.Sales {
		for _, ln := range sale.Lines {
			net := l.skus[ln.SKU].netPerUnit
			for _, alloc := range ln.Lots {
				acc := snap.Batches[alloc.Batch]
				acc.SoldNet += net * Centigram(alloc.Units)
				snap.Batches[alloc.Batch] = acc
			}
		}
	}
	for _, refund := range snap.Refunds {
		for _, ln := range refund.Lines {
			net := l.skus[ln.SKU].netPerUnit
			for _, alloc := range ln.Lots {
				acc := snap.Batches[alloc.Batch]
				acc.SoldNet -= net * Centigram(alloc.Units)
				snap.Batches[alloc.Batch] = acc
			}
		}
	}
	sort.Slice(snap.Lots, func(i, j int) bool { return snap.Lots[i].LotID < snap.Lots[j].LotID })
	sort.Slice(snap.Sales, func(i, j int) bool { return snap.Sales[i].ID < snap.Sales[j].ID })
	sort.Slice(snap.Refunds, func(i, j int) bool { return snap.Refunds[i].ID < snap.Refunds[j].ID })
	return snap
}
