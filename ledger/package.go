package ledger

// Package 从指定种植批次的散装余额扣除 units 件净重与废弃重量，
// 生成一个库存批号并返回。余额校验未通过时不留下任何变化。
func (l *Ledger) Package(batchID BatchID, sku SKU, units uint64, waste Centigram) (LotID, error) {
	b, ok := l.batches[batchID]
	if !ok {
		return 0, ErrUnknownBatch
	}
	s, ok := l.skus[sku]
	if !ok {
		return 0, ErrUnknownSKU
	}
	if units == 0 || waste < 0 {
		return 0, ErrInvalidArgument
	}
	need := s.netWeight*Centigram(units) + waste

	// 加锁顺序：先批次锁，后 SKU 锁，全账本统一。
	b.mu.Lock()
	defer b.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	st := b.state.load()
	if st.bulk < need {
		return 0, ErrInsufficientBulk
	}
	if h := l.getHooks(); h != nil && h.afterPackageValidate != nil {
		h.afterPackageValidate()
	}

	newLot := &lot{
		id:      LotID(l.nextLotID.Add(1)),
		batch:   batchID,
		sku:     sku,
		balance: newCell(units),
	}
	newLots := make([]*lot, 0, len(s.lots.load())+1)
	newLots = append(newLots, s.lots.load()...)
	newLots = append(newLots, newLot)

	l.commit(
		func(v uint64) { b.state.store(v, batchState{bulk: st.bulk - need, waste: st.waste + waste}) },
		func(v uint64) { s.lots.store(v, newLots) },
	)
	return newLot.id, nil
}
