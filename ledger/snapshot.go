package ledger

// Snapshot 返回账本的一致视图：不加业务锁、不暂停写入。
// 先读取提交计数为目标版本，等待连续发布前沿达到该版本（只等已分配
// 版本的写方完成发布），然后按版本读取各 cell，保证视图落在事务边界上。
// 返回的数据均为拷贝，不随后续写入变化。
func (l *Ledger) Snapshot() Snapshot {
	version := l.commits.counter.Load()
	if h := l.getHooks(); h != nil && h.snapshotWaitStart != nil {
		h.snapshotWaitStart(version)
	}
	l.commits.waitFrontier(version)

	snap := Snapshot{
		Batches: make(map[BatchID]BatchView, len(l.batches)),
	}
	for id, b := range l.batches {
		st := b.state.loadAsOf(version)
		snap.Batches[id] = BatchView{Initial: b.initial, BulkRemaining: st.bulk, Waste: st.waste}
	}
	for sku, s := range l.skus {
		for _, lt := range s.lots.loadAsOf(version) {
			snap.Lots = append(snap.Lots, LotView{
				Lot:       lt.id,
				Batch:     lt.batch,
				SKU:       sku,
				Units:     lt.balance.loadAsOf(version),
				NetWeight: s.netWeight,
			})
		}
	}
	// 共享链表节点版本沿链不一定单调（并发追加），按版本过滤。
	for n := l.sales.Load(); n != nil; n = n.next {
		if n.version > version {
			continue
		}
		view := SaleView{
			Receipt:  cloneSaleReceipt(n.sale.receipt),
			Refunded: make(map[SKU]uint64, len(n.sale.refunded)),
		}
		for sku, c := range n.sale.refunded {
			view.Refunded[sku] = c.loadAsOf(version)
		}
		snap.Sales = append(snap.Sales, view)
	}
	for n := l.refunds.Load(); n != nil; n = n.next {
		if n.version > version {
			continue
		}
		snap.Refunds = append(snap.Refunds, cloneRefundReceipt(n.receipt))
	}
	return snap
}

// CheckInvariants 校验快照的账务不变量，供测试与调用方复核：
// 每个批次 初始 = 剩余散装 + 在库包装净重 + 净已售 + 累计废弃，各项非负；
// 在库与净已售均可由快照内的库存余额、销售与退款凭据复核。
func (s Snapshot) CheckInvariants() error {
	inStock := make(map[BatchID]Centigram)
	for _, lv := range s.Lots {
		inStock[lv.Batch] += Centigram(lv.Units) * lv.NetWeight
	}
	sold := make(map[BatchID]Centigram)
	lotByID := make(map[LotID]LotView, len(s.Lots))
	for _, lv := range s.Lots {
		lotByID[lv.Lot] = lv
	}
	addAlloc := func(m map[BatchID]Centigram, lotID LotID, units uint64) {
		lv, ok := lotByID[lotID]
		if !ok {
			return
		}
		m[lv.Batch] += Centigram(units) * lv.NetWeight
	}
	gross := make(map[BatchID]Centigram)
	for _, sv := range s.Sales {
		for _, ln := range sv.Receipt.Lines {
			for _, a := range ln.Allocations {
				addAlloc(gross, a.Lot, a.Units)
			}
		}
	}
	returned := make(map[BatchID]Centigram)
	for _, rv := range s.Refunds {
		for _, ln := range rv.Lines {
			for _, a := range ln.Allocations {
				addAlloc(returned, a.Lot, a.Units)
			}
		}
	}
	for b := range gross {
		sold[b] = gross[b] - returned[b]
	}
	for id, bv := range s.Batches {
		if bv.BulkRemaining < 0 || bv.Waste < 0 || inStock[id] < 0 || sold[id] < 0 {
			return ErrInvariantViolation
		}
		if bv.Initial != bv.BulkRemaining+inStock[id]+sold[id]+bv.Waste {
			return ErrInvariantViolation
		}
	}
	return nil
}
