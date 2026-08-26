package transport

import (
	"context"
	"sync"
	"time"

	"cdcpipeline/internal/model"
)

// Sink is the target write path the dispatcher delivers events to.
type Sink interface {
	Write(ctx context.Context, event model.ChangeEvent) error
}

// GroupSplitter chunks an event list into bounded delivery groups preserving
// transaction order. The parser implements it for large transactions.
type GroupSplitter interface {
	Split(events []model.ChangeEvent) [][]model.ChangeEvent
}

// Dispatcher delivers events to the target sink with deduplication, retry and
// transaction-order preservation. Delivered events stay marked across retries
// and rollbacks so a failed batch is never applied twice.
type Dispatcher struct {
	sink     Sink
	dedup    *Dedup
	order    *OrderTracker
	splitter GroupSplitter
	policy   RetryPolicy
	stats    *model.PipelineStats
	rollback func(pos uint64) error
	status   map[uint64]model.DeliveryStatus

	mu      sync.Mutex
	pending []pendingGroup
}

type pendingGroup struct {
	events []model.ChangeEvent
	txnID  string
	first  uint64
	last   uint64
}

// NewDispatcher returns a dispatcher delivering to the given sink.
func NewDispatcher(sink Sink, policy RetryPolicy, stats *model.PipelineStats) *Dispatcher {
	return &Dispatcher{
		sink:   sink,
		dedup:  NewDedup(),
		order:  NewOrderTracker(),
		policy: policy,
		stats:  stats,
		status: make(map[uint64]model.DeliveryStatus),
	}
}

// SetSplitter installs the group splitter used for large transactions.
func (d *Dispatcher) SetSplitter(splitter GroupSplitter) { d.splitter = splitter }

// SetRollback installs the offset rollback callback invoked when a batch
// fails partway.
func (d *Dispatcher) SetRollback(fn func(pos uint64) error) { d.rollback = fn }

// Deliver delivers the batch to the sink and returns the highest acknowledged
// source position. Failed events are retried according to the retry policy;
// already delivered events are never delivered again.
func (d *Dispatcher) Deliver(ctx context.Context, batch model.Batch) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deliverEvents(ctx, batch.Events)
}

// DeliverPlanned delivers table batches in the given dependency order. The
// order is produced by the mapper's foreign key planner and must not be
// re-sorted by the transport layer.
func (d *Dispatcher) DeliverPlanned(ctx context.Context, batches []model.TableBatch) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var acked uint64
	for _, tb := range batches {
		pos, err := d.deliverEvents(ctx, tb.Events)
		if err != nil {
			return acked, err
		}
		if pos > acked {
			acked = pos
		}
	}
	return acked, nil
}

func (d *Dispatcher) deliverEvents(ctx context.Context, events []model.ChangeEvent) (uint64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	groups := [][]model.ChangeEvent{events}
	if d.splitter != nil {
		groups = d.splitter.Split(events)
	}
	d.pending = d.pending[:0]
	for _, group := range groups {
		d.pending = append(d.pending, pendingGroup{
			events: group,
			txnID:  groupTxnID(group),
			first:  group[0].TxnSeq,
			last:   groupLastSeq(group),
		})
	}
	return d.drainPending(ctx)
}

func (d *Dispatcher) drainPending(ctx context.Context) (uint64, error) {
	var acked uint64
	for attempt := 1; attempt <= d.policy.MaxAttempts; attempt++ {
		var failed *pendingGroup
		var failedErr error
		acked = 0
		next := make([]pendingGroup, 0, len(d.pending))
		for i := range d.pending {
			group := d.pending[i]
			if group.txnID != "" && !d.order.Acquire(group.txnID, group.first) {
				// The predecessor of this transaction group has not been
				// applied yet; keep it waiting instead of letting it jump the
				// queue.
				next = append(next, group)
				continue
			}
			pos, err := d.deliverGroup(ctx, group)
			if err != nil {
				if failed == nil {
					failed = &group
					failedErr = err
				}
				next = append(next, group)
				continue
			}
			if pos > acked {
				acked = pos
			}
		}
		d.pending = next
		if failed == nil {
			return acked, nil
		}
		if attempt >= d.policy.MaxAttempts {
			if d.rollback != nil {
				_ = d.rollback(failed.first)
			}
			return acked, failedErr
		}
		select {
		case <-ctx.Done():
			return acked, ctx.Err()
		case <-time.After(d.policy.NextDelay(attempt)):
		}
	}
	return acked, nil
}

func (d *Dispatcher) deliverGroup(ctx context.Context, group pendingGroup) (uint64, error) {
	var acked uint64
	for _, ev := range group.events {
		key := ev.TableKey()
		if d.dedup.Seen(key) {
			if d.stats != nil {
				d.stats.DupSuppressed.Add(1)
			}
			continue
		}
		if err := d.sink.Write(ctx, ev); err != nil {
			if d.stats != nil {
				d.stats.RetryCount.Add(1)
			}
			d.status[ev.Seq] = model.StatusRetrying
			if group.txnID != "" && ev.TxnSeq > 0 {
				d.order.Release(group.txnID, ev.TxnSeq-1)
			}
			return acked, err
		}
		d.dedup.Mark(key)
		d.status[ev.Seq] = model.StatusAcked
		if d.stats != nil {
			d.stats.DeliveredCount.Add(1)
		}
		acked = ev.SourcePos
	}
	if group.txnID != "" && group.last > 0 {
		d.order.Release(group.txnID, group.last)
	}
	return acked, nil
}

func groupTxnID(events []model.ChangeEvent) string {
	if len(events) == 0 {
		return ""
	}
	return events[0].TxnID
}

func groupLastSeq(events []model.ChangeEvent) uint64 {
	if len(events) == 0 {
		return 0
	}
	return events[len(events)-1].TxnSeq
}

// Drain returns the number of events still waiting for delivery; used by the
// monitor to show pipeline backlog.
func (d *Dispatcher) PendingCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, group := range d.pending {
		count += len(group.events)
	}
	return count
}

// StatusSnapshot returns the delivery state of recently processed events.
func (d *Dispatcher) StatusSnapshot(limit int) []model.DeliveryRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]model.DeliveryRecord, 0, len(d.status))
	for seq, status := range d.status {
		out = append(out, model.DeliveryRecord{EventSeq: seq, Status: status})
	}
	return out
}
