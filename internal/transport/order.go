package transport

import "sync"

// OrderTracker keeps per-transaction progress so groups of a large transaction
// are applied in their original sequence even when earlier groups had to be
// retried.
type OrderTracker struct {
	mu   sync.Mutex
	next map[string]uint64
}

// NewOrderTracker returns an empty order tracker.
func NewOrderTracker() *OrderTracker {
	return &OrderTracker{next: make(map[string]uint64)}
}

// Acquire reports whether the group with the given transaction sequence may be
// applied now. A group is admitted only when it is the next expected sequence.
func (t *OrderTracker) Acquire(txnID string, txnSeq uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if txnSeq == 0 {
		return true
	}
	next := t.next[txnID]
	if next == 0 {
		next = 1
	}
	return txnSeq == next
}

// Release advances the expected sequence past the applied group.
func (t *OrderTracker) Release(txnID string, txnSeq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if txnSeq == 0 {
		return
	}
	if txnSeq >= t.next[txnID] {
		t.next[txnID] = txnSeq + 1
	}
}

// Reset drops all transaction progress; used when the pipeline transitions
// between phases and transactions can no longer span the boundary.
func (t *OrderTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next = make(map[string]uint64)
}
