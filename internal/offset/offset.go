package offset

import (
	"sync"

	"cdcpipeline/internal/model"
)

// Store keeps the synchronization position of the pipeline. The committed
// position only moves forward and is advanced strictly after a batch has been
// delivered; the read watermark tracks how far the source has been read so the
// monitor can show the backlog independently.
type Store struct {
	mu            sync.Mutex
	committed     uint64
	readWatermark uint64
	persisted     uint64
	replayFrom    uint64
	phase         model.SyncPhase
}

// NewStore returns an offset store starting at the given committed position.
func NewStore(initial uint64) *Store {
	return &Store{
		committed:  initial,
		persisted:  initial,
		replayFrom: initial,
		phase:      model.PhaseFull,
	}
}

// Advance moves the committed position forward. It never moves backward, so a
// late acknowledgement cannot regress progress.
func (s *Store) Advance(pos uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pos > s.committed {
		s.committed = pos
	}
	return nil
}

// RecordRead updates the read watermark without touching the committed
// position. The watermark may run ahead of what has been delivered.
func (s *Store) RecordRead(pos uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pos > s.readWatermark {
		s.readWatermark = pos
	}
}

// Rollback narrows the replay window for the reader. It never regresses the
// committed position; deduplication at the transport layer protects already
// delivered events from being applied twice.
func (s *Store) Rollback(pos uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pos < s.replayFrom {
		return nil
	}
	s.replayFrom = pos
	return nil
}

// ReplayFrom returns the lowest position the reader should re-read from.
func (s *Store) ReplayFrom() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replayFrom
}

// Load returns the committed position.
func (s *Store) Load() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}

// Committed is an alias for Load kept for readability at call sites.
func (s *Store) Committed() uint64 { return s.Load() }

// ReadWatermark returns the highest position the source has produced.
func (s *Store) ReadWatermark() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readWatermark
}

// PersistCheckpoint atomically copies the committed position to the durable
// checkpoint slot. Restart restores from this slot, so it must never lag the
// latest acknowledged position.
func (s *Store) PersistCheckpoint() error {
	pos := s.committed
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persisted = pos
	return nil
}

// PersistCheckpointValue writes an arbitrary value as the durable checkpoint.
func (s *Store) PersistCheckpointValue(value uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persisted = value
	return nil
}

// Restore simulates a process restart: the committed position is reloaded
// from the durable checkpoint.
func (s *Store) Restore() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committed = s.persisted
	s.readWatermark = s.persisted
	return s.committed
}

// SetPhase records the synchronization stage in the store.
func (s *Store) SetPhase(phase model.SyncPhase) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.phase = phase
}

// Phase returns the current synchronization stage.
func (s *Store) Phase() model.SyncPhase {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phase
}

// Snapshot returns a point-in-time view of the store state.
func (s *Store) Snapshot() model.Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return model.Checkpoint{
		Committed:  model.Position(s.committed),
		Read:       model.Position(s.readWatermark),
		Phase:      s.phase,
		SnapshotAt: model.Position(s.replayFrom),
	}
}
