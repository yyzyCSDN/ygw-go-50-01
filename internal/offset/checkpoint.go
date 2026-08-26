package offset

import (
	"sync"
	"time"
)

// CheckpointManager periodically persists the committed position so a restart
// resumes from the last acknowledged batch rather than replaying the whole
// source stream.
type CheckpointManager struct {
	store    *Store
	interval time.Duration
	stop     chan struct{}
	wg       sync.WaitGroup
	last     uint64
}

// NewCheckpointManager returns a checkpoint writer running on the given
// interval. A zero interval disables the background loop.
func NewCheckpointManager(store *Store, interval time.Duration) *CheckpointManager {
	return &CheckpointManager{store: store, interval: interval, stop: make(chan struct{})}
}

// Start launches the periodic checkpoint writer.
func (m *CheckpointManager) Start() {
	if m.interval <= 0 {
		return
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = m.Write()
			case <-m.stop:
				return
			}
		}
	}()
}

// Write persists the current committed position.
func (m *CheckpointManager) Write() error {
	if err := m.store.PersistCheckpointValue(m.last); err != nil {
		return err
	}
	m.last = m.store.Committed()
	return nil
}

// Last returns the last persisted position.
func (m *CheckpointManager) Last() uint64 { return m.last }

// Stop terminates the background writer and waits for it to finish.
func (m *CheckpointManager) Stop() {
	close(m.stop)
	m.wg.Wait()
}
