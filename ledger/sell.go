package ledger

// Sell 完成一笔多 SKU 销售：同 SKU 按库存批号升序扣减，可跨来源批次。
// 任一 SKU 库存不足时整笔失败，不留下任何扣减或凭据。
// 成功时返回不可变销售凭据。
func (l *Ledger) Sell(lines map[SKU]uint64) (SaleReceipt, error) {
	if len(lines) == 0 {
		return SaleReceipt{}, ErrInvalidArgument
	}
	set := make(map[SKU]bool, len(lines))
	for sku, qty := range lines {
		if _, ok := l.skus[sku]; !ok {
			return SaleReceipt{}, ErrUnknownSKU
		}
		if qty == 0 {
			return SaleReceipt{}, ErrInvalidArgument
		}
		set[sku] = true
	}

	// 只锁本笔涉及的 SKU，按统一顺序加锁，避免死锁。
	ordered, unlock := l.lockSKUs(set)
	defer unlock()

	// 第一阶段：只读校验并为每个 SKU 计算扣减计划。
	type skuPlan struct {
		sku    SKU
		allocs []Allocation
		lots   []*lot // 与 allocs 平行
	}
	plans := make([]skuPlan, 0, len(ordered))
	for _, sku := range ordered {
		remaining := lines[sku]
		plan := skuPlan{sku: sku}
		for _, lt := range l.skus[sku].lots.load() {
			if remaining == 0 {
				break
			}
			bal := lt.balance.load()
			if bal == 0 {
				continue
			}
			take := bal
			if take > remaining {
				take = remaining
			}
			plan.allocs = append(plan.allocs, Allocation{Lot: lt.id, Units: take})
			plan.lots = append(plan.lots, lt)
			remaining -= take
		}
		if remaining > 0 {
			return SaleReceipt{}, ErrInsufficientStock
		}
		plans = append(plans, plan)
	}

	// 第二阶段：构造不可变凭据并统一提交。
	receipt := SaleReceipt{ID: SaleID(l.nextSaleID.Add(1))}
	sale := &saleState{refunded: make(map[SKU]*cell[uint64], len(plans))}
	stores := make([]func(uint64), 0, len(plans)+1)
	for _, p := range plans {
		line := SaleLine{SKU: p.sku, Allocations: p.allocs}
		for _, a := range p.allocs {
			line.Units += a.Units
		}
		receipt.Lines = append(receipt.Lines, line)
		sale.refunded[p.sku] = newCell(uint64(0))
		for i, lt := range p.lots {
			lt := lt
			newBal := lt.balance.load() - p.allocs[i].Units
			stores = append(stores, func(v uint64) { lt.balance.store(v, newBal) })
		}
	}
	sale.receipt = receipt
	stores = append(stores, func(v uint64) { l.appendSale(v, sale) })
	l.commit(stores...)

	l.salesMu.Lock()
	l.salesByID[sale.receipt.ID] = sale
	l.salesMu.Unlock()
	return cloneSaleReceipt(receipt), nil
}

func cloneAllocations(in []Allocation) []Allocation {
	out := make([]Allocation, len(in))
	copy(out, in)
	return out
}

// cloneSaleReceipt 深拷贝销售凭据，保证调用方拿到的凭据不可变。
func cloneSaleReceipt(r SaleReceipt) SaleReceipt {
	out := SaleReceipt{ID: r.ID, Lines: make([]SaleLine, len(r.Lines))}
	for i, ln := range r.Lines {
		out.Lines[i] = SaleLine{SKU: ln.SKU, Units: ln.Units, Allocations: cloneAllocations(ln.Allocations)}
	}
	return out
}

func cloneRefundReceipt(r RefundReceipt) RefundReceipt {
	out := RefundReceipt{ID: r.ID, SaleID: r.SaleID, Lines: make([]RefundLine, len(r.Lines))}
	for i, ln := range r.Lines {
		out.Lines[i] = RefundLine{SKU: ln.SKU, Units: ln.Units, Allocations: cloneAllocations(ln.Allocations)}
	}
	return out
}
