package transport

import (
	"context"
	"testing"
	"time"

	"cdcpipeline/internal/model"
)

type recordingSink struct {
	events []model.ChangeEvent
}

func (s *recordingSink) Write(_ context.Context, event model.ChangeEvent) error {
	s.events = append(s.events, event)
	return nil
}

func TestDispatcherDeliversBatch(t *testing.T) {
	sink := &recordingSink{}
	dispatcher := NewDispatcher(sink, DefaultRetryPolicy(), model.NewStats())
	batch := model.NewBatch([]model.ChangeEvent{
		{Seq: 1, Table: "accounts", Key: "1", SourcePos: 1},
		{Seq: 2, Table: "accounts", Key: "2", SourcePos: 2},
	})
	acked, err := dispatcher.Deliver(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	if acked != 2 {
		t.Fatalf("expected acked=2, got %d", acked)
	}
	if len(sink.events) != 2 {
		t.Fatalf("expected 2 deliveries, got %d", len(sink.events))
	}
}

func TestDispatcherSuppressesDuplicates(t *testing.T) {
	sink := &recordingSink{}
	dispatcher := NewDispatcher(sink, DefaultRetryPolicy(), model.NewStats())
	batch := model.NewBatch([]model.ChangeEvent{
		{Seq: 1, Table: "accounts", Key: "1", SourcePos: 1},
	})
	if _, err := dispatcher.Deliver(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Deliver(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("duplicate delivery must be suppressed, got %d events", len(sink.events))
	}
}

func TestOrderTrackerSequence(t *testing.T) {
	tracker := NewOrderTracker()
	if !tracker.Acquire("txn-1", 1) {
		t.Fatalf("first group should be admitted")
	}
	if tracker.Acquire("txn-1", 3) {
		t.Fatalf("group 3 must wait for group 2")
	}
	tracker.Release("txn-1", 1)
	if !tracker.Acquire("txn-1", 2) {
		t.Fatalf("group 2 should now be admitted")
	}
	tracker.Release("txn-1", 2)
	tracker.Reset()
	if !tracker.Acquire("txn-1", 1) {
		t.Fatalf("reset must restore sequence from the beginning")
	}
}

func TestRetryPolicyBackoff(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 4 * time.Millisecond}
	if policy.NextDelay(1) != time.Millisecond {
		t.Fatalf("unexpected first delay")
	}
	if policy.NextDelay(4) != 4*time.Millisecond {
		t.Fatalf("expected capped delay")
	}
}

func TestDedupKeys(t *testing.T) {
	dedup := NewDedup()
	if dedup.Seen("orders/10") {
		t.Fatalf("fresh key must not be seen")
	}
	dedup.Mark("orders/10")
	if !dedup.Seen("orders/10") {
		t.Fatalf("marked key must be seen")
	}
	if dedup.Size() != 1 {
		t.Fatalf("expected size 1, got %d", dedup.Size())
	}
}
