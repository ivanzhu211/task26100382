package weightledger

import "sort"

// Package 从指定种植批次扣除 units*单件净重 + waste，生成库存批号并返回。
func (l *Ledger) Package(batchID BatchID, sku SKU, units int64, waste Centigram) (LotID, error) {
	if units <= 0 || waste < 0 {
		return 0, ErrInvalidArgument
	}
	b, ok := l.batches[batchID]
	if !ok {
		return 0, ErrUnknownBatch
	}
	s, ok := l.skus[sku]
	if !ok {
		return 0, ErrUnknownSKU
	}
	// 锁顺序：批次 -> SKU，全局一致，避免死锁。
	b.mu.Lock()
	defer b.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	need := s.netPerUnit*Centigram(units) + waste
	if need > b.bulk {
		return 0, ErrInsufficientBulk
	}
	l.fireHook(HookPackageChecked)

	b.bulk -= need
	b.waste += waste
	b.version.Add(1)

	id := LotID(l.lotSeq.Add(1))
	lt := lot{id: id, batch: batchID, balance: units}
	// 保持 lots 按 LotID 升序。
	pos := len(s.lots)
	for pos > 0 && s.lots[pos-1].id > id {
		pos--
	}
	s.lots = append(s.lots, lot{})
	copy(s.lots[pos+1:], s.lots[pos:])
	s.lots[pos] = lt
	s.version.Add(1)
	return id, nil
}

// Sell 一笔销售可涉及多个 SKU，同 SKU 按库存批号从小到大扣减。
// 返回不可变销售凭据。
func (l *Ledger) Sell(lines map[SKU]int64) (SaleReceipt, error) {
	if len(lines) == 0 {
		return SaleReceipt{}, ErrInvalidArgument
	}
	skus := make([]SKU, 0, len(lines))
	for s, qty := range lines {
		if qty <= 0 {
			return SaleReceipt{}, ErrInvalidArgument
		}
		if _, ok := l.skus[s]; !ok {
			return SaleReceipt{}, ErrUnknownSKU
		}
		skus = append(skus, s)
	}
	sort.Slice(skus, func(i, j int) bool { return skus[i] < skus[j] })

	// 按 SKU 排序顺序加锁，全局一致，避免死锁。
	locked := make([]*skuState, len(skus))
	for i, s := range skus {
		st := l.skus[s]
		st.mu.Lock()
		locked[i] = st
	}
	defer func() {
		for _, st := range locked {
			st.mu.Unlock()
		}
	}()

	// 全部校验通过前不做任何修改。
	for i, s := range skus {
		var avail int64
		for _, lt := range locked[i].lots {
			avail += lt.balance
		}
		if avail < lines[s] {
			return SaleReceipt{}, ErrInsufficientStock
		}
	}
	l.fireHook(HookSellChecked)

	receipt := SaleReceipt{Lines: make([]SKULine, 0, len(skus))}
	for i, s := range skus {
		st := locked[i]
		remaining := lines[s]
		line := SKULine{SKU: s, Total: remaining}
		for idx := range st.lots {
			if remaining == 0 {
				break
			}
			lt := &st.lots[idx]
			if lt.balance == 0 {
				continue
			}
			take := lt.balance
			if take > remaining {
				take = remaining
			}
			lt.balance -= take
			remaining -= take
			line.Lots = append(line.Lots, LotAllocation{LotID: lt.id, Batch: lt.batch, Units: take})
		}
		st.version.Add(1)
		receipt.Lines = append(receipt.Lines, line)
	}

	receipt.ID = SaleID(l.saleSeq.Add(1))
	l.store.mu.Lock()
	l.store.sales[receipt.ID] = &saleRecord{
		receipt:  cloneSaleReceipt(receipt),
		progress: refundProgress{skuRefunded: make(map[SKU]int64)},
	}
	l.store.version.Add(1)
	l.store.mu.Unlock()
	return receipt, nil
}

// Refund 对指定销售按 SKU 部分退款，实物退回原库存批号。
func (l *Ledger) Refund(saleID SaleID, lines map[SKU]int64) (RefundReceipt, error) {
	if len(lines) == 0 {
		return RefundReceipt{}, ErrInvalidArgument
	}
	skus := make([]SKU, 0, len(lines))
	for s, qty := range lines {
		if qty <= 0 {
			return RefundReceipt{}, ErrInvalidArgument
		}
		if _, ok := l.skus[s]; !ok {
			return RefundReceipt{}, ErrUnknownSKU
		}
		skus = append(skus, s)
	}
	sort.Slice(skus, func(i, j int) bool { return skus[i] < skus[j] })

	l.store.mu.Lock()
	rec, ok := l.store.sales[saleID]
	l.store.mu.Unlock()
	if !ok {
		return RefundReceipt{}, ErrSaleNotFound
	}

	// 锁顺序：SKU（排序）-> 凭据存储，与 Sell 一致。
	locked := make([]*skuState, len(skus))
	for i, s := range skus {
		st := l.skus[s]
		st.mu.Lock()
		locked[i] = st
	}
	defer func() {
		for _, st := range locked {
			st.mu.Unlock()
		}
	}()
	l.store.mu.Lock()
	defer l.store.mu.Unlock()

	// 校验：每个 SKU 的已退数量 + 本次请求不得超过原销量。
	soldBySKU := make(map[SKU]int64, len(rec.receipt.Lines))
	for _, ln := range rec.receipt.Lines {
		soldBySKU[ln.SKU] = ln.Total
	}
	for _, s := range skus {
		if rec.progress.skuRefunded[s]+lines[s] > soldBySKU[s] {
			return RefundReceipt{}, ErrRefundExceeds
		}
	}
	l.fireHook(HookRefundChecked)

	receipt := RefundReceipt{SaleID: saleID, Lines: make([]SKULine, 0, len(skus))}
	for i, s := range skus {
		st := locked[i]
		remaining := lines[s]
		skipped := rec.progress.skuRefunded[s] // 已退部分按 FIFO 跳过
		line := SKULine{SKU: s, Total: remaining}
		for _, ln := range rec.receipt.Lines {
			if ln.SKU != s {
				continue
			}
			for _, alloc := range ln.Lots {
				if remaining == 0 {
					break
				}
				if skipped >= alloc.Units {
					skipped -= alloc.Units
					continue
				}
				avail := alloc.Units - skipped
				skipped = 0
				take := avail
				if take > remaining {
					take = remaining
				}
				lt := findLot(st, alloc.LotID)
				lt.balance += take
				remaining -= take
				line.Lots = append(line.Lots, LotAllocation{LotID: alloc.LotID, Batch: alloc.Batch, Units: take})
			}
		}
		rec.progress.skuRefunded[s] += lines[s]
		st.version.Add(1)
		receipt.Lines = append(receipt.Lines, line)
	}

	receipt.ID = RefundID(l.refundSeq.Add(1))
	l.store.refunds[receipt.ID] = cloneRefundReceipt(receipt)
	l.store.version.Add(1)
	return receipt, nil
}

// findLot 在按 LotID 升序的 lots 中二分查找，调用方须持有 sku 锁。
// 库存批号一旦创建不会删除，必然存在。
func findLot(st *skuState, id LotID) *lot {
	lo, hi := 0, len(st.lots)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		switch {
		case st.lots[mid].id < id:
			lo = mid + 1
		case st.lots[mid].id > id:
			hi = mid - 1
		default:
			return &st.lots[mid]
		}
	}
	return nil
}
