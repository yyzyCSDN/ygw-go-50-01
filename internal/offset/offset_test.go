package offset

import (
	"testing"

	"cdcpipeline/internal/model"
)

func TestStoreAdvanceAndLoad(t *testing.T) {
	store := NewStore(0)
	if store.Load() != 0 || store.Committed() != 0 {
		t.Fatalf("expected initial committed 0")
	}
	if err := store.Advance(10); err != nil {
		t.Fatal(err)
	}
	if store.Load() != 10 {
		t.Fatalf("expected committed 10, got %d", store.Load())
	}
	if err := store.Advance(5); err != nil {
		t.Fatal(err)
	}
	if store.Load() != 10 {
		t.Fatalf("committed must never move backward: %d", store.Load())
	}
}

func TestStoreReadWatermarkAndPhase(t *testing.T) {
	store := NewStore(3)
	store.RecordRead(8)
	if store.ReadWatermark() != 8 {
		t.Fatalf("expected read watermark 8, got %d", store.ReadWatermark())
	}
	if store.Phase() != model.PhaseFull {
		t.Fatalf("expected initial phase full")
	}
	store.SetPhase(model.PhaseIncremental)
	if store.Phase() != model.PhaseIncremental {
		t.Fatalf("phase not updated")
	}
}

func TestStoreSnapshot(t *testing.T) {
	store := NewStore(0)
	store.RecordRead(4)
	_ = store.Advance(2)
	store.SetPhase(model.PhaseIncremental)
	checkpoint := store.Snapshot()
	if checkpoint.Committed != 2 || checkpoint.Read != 4 || checkpoint.Phase != model.PhaseIncremental {
		t.Fatalf("unexpected snapshot: %+v", checkpoint)
	}
}
