package parser

import "cdcpipeline/internal/model"

// TxnSplitter tags transaction-internal order and splits large transactions
// into bounded groups. The transaction sequence is the ordering contract that
// the transport layer must preserve across retries.
type TxnSplitter struct {
	maxGroup int
}

// NewTxnSplitter returns a splitter with the given group size.
func NewTxnSplitter(maxGroup int) *TxnSplitter {
	if maxGroup <= 0 {
		maxGroup = 200
	}
	return &TxnSplitter{maxGroup: maxGroup}
}

// Sequence fills the transaction sequence for events that did not carry one
// from the source and keeps the global order stable.
func (s *TxnSplitter) Sequence(events []model.ChangeEvent) []model.ChangeEvent {
	out := make([]model.ChangeEvent, 0, len(events))
	var txnSeq uint64
	var currentTxn string
	for _, ev := range events {
		if ev.TxnID != "" && ev.TxnID != currentTxn {
			currentTxn = ev.TxnID
			txnSeq = 0
		}
		if ev.TxnID != "" {
			txnSeq++
			ev.TxnSeq = txnSeq
		}
		out = append(out, ev)
	}
	return out
}

// Split breaks a large transaction into ordered groups. Consecutive events are
// kept together so the original transaction order is recoverable from the
// TxnSeq values on the events.
func (s *TxnSplitter) Split(events []model.ChangeEvent) [][]model.ChangeEvent {
	var groups [][]model.ChangeEvent
	for start := 0; start < len(events); start += s.maxGroup {
		end := start + s.maxGroup
		if end > len(events) {
			end = len(events)
		}
		group := append([]model.ChangeEvent(nil), events[start:end]...)
		groups = append(groups, group)
	}
	return groups
}
