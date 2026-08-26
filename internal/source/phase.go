package source

import (
	"context"
	"fmt"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/offset"
)

// SnapshotLoader receives the full-table snapshot rows during handoff.
type SnapshotLoader interface {
	LoadSnapshot(rows []SnapshotRow) error
}

// PhaseCoordinator drives the full-to-incremental handoff. The incremental
// phase resumes from the snapshot start watermark, so every event appended
// while the scan was running is picked up exactly once.
type PhaseCoordinator struct {
	scanner *FullScanner
	store   *offset.Store
	loader  SnapshotLoader
}

// NewPhaseCoordinator wires the scanner, offset store and snapshot loader.
func NewPhaseCoordinator(scanner *FullScanner, store *offset.Store, loader SnapshotLoader) *PhaseCoordinator {
	return &PhaseCoordinator{scanner: scanner, store: store, loader: loader}
}

// RunFull executes the full snapshot and binds the incremental resume point to
// the snapshot start watermark.
func (pc *PhaseCoordinator) RunFull(ctx context.Context) (model.PhaseCheckpoint, error) {
	rows, _, err := pc.scanner.Snapshot(ctx)
	if err != nil {
		return model.PhaseCheckpoint{}, err
	}
	if pc.loader != nil {
		if err := pc.loader.LoadSnapshot(rows); err != nil {
			return model.PhaseCheckpoint{}, fmt.Errorf("load snapshot: %w", err)
		}
	}
	completion := pc.scanner.CurrentWatermark()
	pc.store.RecordRead(uint64(completion))
	if err := pc.store.Advance(uint64(completion)); err != nil {
		return model.PhaseCheckpoint{}, err
	}
	pc.store.SetPhase(model.PhaseIncremental)
	return model.PhaseCheckpoint{
		Phase:      model.PhaseIncremental,
		SnapshotAt: model.Position(completion),
		NextPos:    model.Position(completion),
		Tables:     pc.scanner.Tables(),
	}, nil
}

// RunIncremental reads the next incremental batch after the given position.
func (pc *PhaseCoordinator) RunIncremental(ctx context.Context, reader *Reader, from uint64) (ReadBatch, error) {
	return reader.Read(ctx, from)
}
