package sink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/source"
)

var (
	// ErrWriteTimeout is returned when the target write does not finish within
	// the configured timeout. The sink also flips its backpressure flag so the
	// source pauses instead of piling more events into the pipeline.
	ErrWriteTimeout = errors.New("target write timed out")
	// ErrFKViolation is returned when a child row is written before its parent.
	ErrFKViolation = errors.New("foreign key dependency not satisfied")
)

// Sink is the target write path.
type Sink interface {
	Write(ctx context.Context, event model.ChangeEvent) error
	Backpressured() bool
}

// InMemorySink is an in-process target store used by the demo server and the
// pipeline tests. It enforces foreign key order, applies events idempotently
// and models write latency so backpressure behavior can be exercised.
type InMemorySink struct {
	mu           sync.Mutex
	rows         map[string]map[string]map[string]string
	order        []model.ChangeEvent
	fk           []model.FKEdge
	guard        *IdempotencyGuard
	writeDelay   time.Duration
	writeTimeout time.Duration
	failTable    map[string]error
	backpressured bool
}

// NewInMemorySink returns an empty in-memory target store.
func NewInMemorySink(fk []model.FKEdge) *InMemorySink {
	return &InMemorySink{
		rows:   make(map[string]map[string]map[string]string),
		guard:  NewIdempotencyGuard(),
		fk:     append([]model.FKEdge(nil), fk...),
		failTable: make(map[string]error),
	}
}

// Write applies one event idempotently. When a write exceeds the configured
// timeout the sink returns ErrWriteTimeout and raises the backpressure flag.
func (s *InMemorySink) Write(ctx context.Context, event model.ChangeEvent) error {
	key := deliveryIdentity(event)
	if s.guard.AlreadyApplied(key) {
		return nil
	}
	if err := s.failTable[event.Table]; err != nil {
		return err
	}
	if err := s.applyWithContext(ctx, event); err != nil {
		return err
	}
	s.guard.Apply(key)
	return nil
}

// Backpressured reports whether the target is currently unable to keep up.
func (s *InMemorySink) Backpressured() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.backpressured
}

func (s *InMemorySink) setBackpressured(value bool) {
	s.mu.Lock()
	s.backpressured = value
	s.mu.Unlock()
}

// SetWriteDelay simulates target write latency.
func (s *InMemorySink) SetWriteDelay(d time.Duration) { s.writeDelay = d }

// SetWriteTimeout configures the per-write deadline. A zero value disables it.
func (s *InMemorySink) SetWriteTimeout(d time.Duration) { s.writeTimeout = d }

// FailTable forces writes for a table to return the given error.
func (s *InMemorySink) FailTable(table string, err error) {
	if err == nil {
		delete(s.failTable, table)
		return
	}
	s.failTable[table] = err
}

// LoadSnapshot loads full-table snapshot rows during the full phase.
func (s *InMemorySink) LoadSnapshot(rows []source.SnapshotRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, row := range rows {
		if _, ok := s.rows[row.Table]; !ok {
			s.rows[row.Table] = make(map[string]map[string]string)
		}
		key := row.Row["id"]
		if key == "" {
			continue
		}
		s.rows[row.Table][key] = row.Row
		s.guard.Apply(row.Table + "/snapshot/" + key)
	}
	return nil
}

// deliveryIdentity is the idempotency identity of one source event: a replay
// of the same event (same position) is skipped, while a later delete of the
// same row is a distinct event and must be applied.
func deliveryIdentity(event model.ChangeEvent) string {
	return fmt.Sprintf("%s/%d", event.Table, event.Seq)
}

// Rows returns the applied rows for a table.
func (s *InMemorySink) Rows(table string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]map[string]string, len(s.rows[table]))
	for key, row := range s.rows[table] {
		out[key] = row
	}
	return out
}

// RowCount returns the number of applied rows for a table.
func (s *InMemorySink) RowCount(table string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows[table])
}

// AppliedOrder returns the events applied to the target in application order.
func (s *InMemorySink) AppliedOrder() []model.ChangeEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.ChangeEvent(nil), s.order...)
}
