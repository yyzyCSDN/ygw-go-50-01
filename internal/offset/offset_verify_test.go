package offset_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/offset"
	"cdcpipeline/internal/transport"
)

type failingSink struct {
	mu      sync.Mutex
	failSeq uint64
	written []uint64
}

func (s *failingSink) Write(_ context.Context, event model.ChangeEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.Seq == s.failSeq {
		return errors.New("sink down")
	}
	s.written = append(s.written, event.Seq)
	return nil
}

func TestOffsetAdvanceAfterDeliver(t *testing.T) {
	store := offset.NewStore(0)
	sink := &failingSink{failSeq: 2}
	dispatcher := transport.NewDispatcher(sink, transport.RetryPolicy{MaxAttempts: 1}, model.NewStats())
	coordinator := offset.NewCoordinator(store, dispatcher.Deliver, model.NewStats())
	batch := model.NewBatch([]model.ChangeEvent{
		{Seq: 1, Table: "accounts", Key: "1", SourcePos: 1},
		{Seq: 2, Table: "accounts", Key: "2", SourcePos: 2},
	})
	if err := coordinator.ProcessBatch(context.Background(), batch); err == nil {
		t.Fatalf("delivery was expected to fail")
	}
	if committed := store.Committed(); committed != 0 {
		t.Fatalf("committed offset advanced although delivery failed: %d", committed)
	}
	if restored := store.Restore(); restored != 0 {
		t.Fatalf("restart resumed from %d and would lose undelivered events", restored)
	}
}

func TestConcurrentFailedDeliveriesDoNotAdvance(t *testing.T) {
		store := offset.NewStore(0)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			coordinator := offset.NewCoordinator(store, func(context.Context, model.Batch) (uint64, error) {
				return 0, errors.New("down")
			}, nil)
			_ = coordinator.ProcessBatch(context.Background(), model.NewBatch([]model.ChangeEvent{
				{Seq: uint64(i + 1), Table: "t", SourcePos: uint64(i + 1)},
			}))
		}(i)
	}
	wg.Wait()
	if committed := store.Committed(); committed != 0 {
		t.Fatalf("committed advanced under concurrent failures: %d", committed)
	}
}
