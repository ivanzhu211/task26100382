package ledger

// Refund 对指定销售按 SKU 部分退款：每个 SKU 按原销售凭据中的分配顺序，
// 依次返还尚未退过的数量，实物退回原库存批号，可再次销售。
// 超额退款时整笔失败，不留下任何变化；成功时不改写原销售凭据。
func (l *Ledger) Refund(saleID SaleID, lines map[SKU]uint64) (RefundReceipt, error) {
	if len(lines) == 0 {
		return RefundReceipt{}, ErrInvalidArgument
	}
	l.salesMu.Lock()
	sale, ok := l.salesByID[saleID]
	l.salesMu.Unlock()
	if !ok {
		return RefundReceipt{}, ErrUnknownSale
	}
	for sku, qty := range lines {
		if _, ok := l.skus[sku]; !ok {
			return RefundReceipt{}, ErrUnknownSKU
		}
		if qty == 0 {
			return RefundReceipt{}, ErrInvalidArgument
		}
		if _, sold := sale.refunded[sku]; !sold {
			return RefundReceipt{}, ErrRefundExceeds
		}
	}

	// 加锁顺序：先原销售锁，后按统一顺序锁涉及的 SKU。
	sale.mu.Lock()
	defer sale.mu.Unlock()
	set := make(map[SKU]bool, len(lines))
	for sku := range lines {
		set[sku] = true
	}
	ordered, unlock := l.lockSKUs(set)
	defer unlock()

	// 第一阶段：只读校验并计算返还计划，目标批号来自原凭据而非当前库存。
	type refundPlan struct {
		sku    SKU
		allocs []Allocation
		lots   []*lot
	}
	plans := make([]refundPlan, 0, len(ordered))
	for _, sku := range ordered {
		want := lines[sku]
		sold := sale.receipt.TotalUnits(sku)
		already := sale.refunded[sku].load()
		if want > sold-already {
			return RefundReceipt{}, ErrRefundExceeds
		}
		plan := refundPlan{sku: sku}
		remaining := want
		// 已退数量按原分配顺序依次消耗各批号额度，用游标顺序分摊。
		consumed := already
		for _, line := range sale.receipt.Lines {
			if line.SKU != sku {
				continue
			}
			for _, a := range line.Allocations {
				used := a.Units
				if consumed < used {
					used = consumed
				}
				consumed -= used
				outstanding := a.Units - used
				if outstanding == 0 || remaining == 0 {
					continue
				}
				take := outstanding
				if take > remaining {
					take = remaining
				}
				plan.allocs = append(plan.allocs, Allocation{Lot: a.Lot, Units: take})
				plan.lots = append(plan.lots, l.findLot(sku, a.Lot))
				remaining -= take
			}
		}
		if remaining > 0 {
			return RefundReceipt{}, ErrRefundExceeds
		}
		plans = append(plans, plan)
	}

	// 第二阶段：构造不可变退款凭据并统一提交。
	receipt := RefundReceipt{ID: RefundID(l.nextRefundID.Add(1)), SaleID: saleID}
	stores := make([]func(uint64), 0, len(plans)+1)
	for _, p := range plans {
		line := RefundLine{SKU: p.sku}
		for _, a := range p.allocs {
			line.Units += a.Units
		}
		line.Allocations = p.allocs
		receipt.Lines = append(receipt.Lines, line)

		refundedCell := sale.refunded[p.sku]
		newRefunded := refundedCell.load() + line.Units
		stores = append(stores, func(v uint64) { refundedCell.store(v, newRefunded) })
		for i, lt := range p.lots {
			lt := lt
			newBal := lt.balance.load() + p.allocs[i].Units
			stores = append(stores, func(v uint64) { lt.balance.store(v, newBal) })
		}
	}
	stores = append(stores, func(v uint64) { l.appendRefund(v, receipt) })
	l.commit(stores...)
	return cloneRefundReceipt(receipt), nil
}

// findLot 在指定 SKU 的库存列表中查找批号。调用方需持有该 SKU 锁。
func (l *Ledger) findLot(sku SKU, id LotID) *lot {
	for _, lt := range l.skus[sku].lots.load() {
		if lt.id == id {
			return lt
		}
	}
	return nil
}
