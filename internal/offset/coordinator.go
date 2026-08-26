package offset

import (
	"context"
	"sync"

	"cdcpipeline/internal/model"
)

// DeliverFunc delivers a batch of events and reports the highest source
// position that has been acknowledged by the target.
type DeliverFunc func(context.Context, model.Batch) (uint64, error)

// Coordinator owns the cross-component sequencing contract between delivery
// and offset advancement: the committed offset moves only after the target has
// acknowledged the batch. This keeps a restart from dropping events that were
// read but never delivered.
type Coordinator struct {
	store   *Store
	deliver DeliverFunc
	stats   *model.PipelineStats
	mu      sync.Mutex
}

// NewCoordinator wires a store and a deliver function into one batch step.
func NewCoordinator(store *Store, deliver DeliverFunc, stats *model.PipelineStats) *Coordinator {
	return &Coordinator{store: store, deliver: deliver, stats: stats}
}

// ProcessBatch delivers the batch and advances the committed offset to the
// acknowledged position afterwards. A failed delivery leaves the offset where
// it was so the events can be retried after restart.
func (c *Coordinator) ProcessBatch(ctx context.Context, batch model.Batch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	acked, err := c.deliver(ctx, batch)
	if err != nil {
		return err
	}
	if err := c.store.Advance(acked); err != nil {
		return err
	}
	c.store.RecordRead(batch.MaxPosition())
	if c.stats != nil {
		c.stats.AckedCount.Add(uint64(len(batch.Events)))
		c.stats.LastCommitted.Store(acked)
		c.stats.LastRead.Store(batch.MaxPosition())
	}
	return nil
}
