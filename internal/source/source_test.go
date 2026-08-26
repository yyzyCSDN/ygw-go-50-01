package source

import (
	"context"
	"errors"
	"testing"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/offset"
)

func TestInMemoryLogAppendRead(t *testing.T) {
	log := NewInMemoryLog()
	if err := log.Append(
		LogEntry{Seq: 1, Table: "accounts", Op: model.OpInsert, Row: map[string]string{"id": "1"}},
		LogEntry{Seq: 2, Table: "orders", Op: model.OpInsert, Row: map[string]string{"id": "10"}},
		LogEntry{Seq: 3, Table: "accounts", Op: model.OpUpdate, Row: map[string]string{"id": "1"}},
	); err != nil {
		t.Fatal(err)
	}
	if log.MaxPosition() != 3 {
		t.Fatalf("expected max position 3, got %d", log.MaxPosition())
	}
	entries, next, err := log.ReadAfter(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || next != 3 {
		t.Fatalf("expected 2 entries ending at 3, got %d entries next=%d", len(entries), next)
	}
	if _, _, err := log.ReadAfter(3, 10); !errors.Is(err, ErrNoMore) {
		t.Fatalf("expected ErrNoMore after tail, got %v", err)
	}
	tables := log.Tables()
	if len(tables) != 2 || tables[0] != "accounts" || tables[1] != "orders" {
		t.Fatalf("unexpected tables: %v", tables)
	}
}

func TestInMemoryLogSnapshot(t *testing.T) {
	log := NewInMemoryLog()
	log.PutSnapshot("accounts", []map[string]string{{"id": "1"}, {"id": "2"}})
	rows, err := log.SnapshotRow("accounts")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 snapshot rows, got %d", len(rows))
	}
}

func TestReaderReadsBatches(t *testing.T) {
	log := NewInMemoryLog()
	_ = log.Append(
		LogEntry{Seq: 1, Table: "accounts", Op: model.OpInsert, Row: map[string]string{"id": "1"}},
		LogEntry{Seq: 2, Table: "accounts", Op: model.OpInsert, Row: map[string]string{"id": "2"}},
	)
	reader := NewReader(log, 1, model.NewStats())
	batch, err := reader.Read(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if batch.IsEmpty() || batch.MaxPos != 1 || len(batch.Entries) != 1 {
		t.Fatalf("unexpected first batch: max=%d count=%d", batch.MaxPos, len(batch.Entries))
	}
	second, err := reader.Read(context.Background(), batch.MaxPos)
	if err != nil {
		t.Fatal(err)
	}
	if second.MaxPos != 2 {
		t.Fatalf("expected second batch max 2, got %d", second.MaxPos)
	}
}

func TestFullScannerSnapshotAndHandoff(t *testing.T) {
	log := NewInMemoryLog()
	log.PutSnapshot("accounts", []map[string]string{{"id": "1"}})
	log.PutSnapshot("orders", []map[string]string{{"id": "10"}})
	scanner := NewFullScanner(log, []string{"accounts", "orders"})
	store := offset.NewStore(0)
	loader := &recordingLoader{}
	coordinator := NewPhaseCoordinator(scanner, store, loader)
	checkpoint, err := coordinator.RunFull(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Phase != model.PhaseIncremental {
		t.Fatalf("expected incremental phase, got %s", checkpoint.Phase)
	}
	if len(loader.rows) != 2 {
		t.Fatalf("expected 2 snapshot rows loaded, got %d", len(loader.rows))
	}
	if store.Phase() != model.PhaseIncremental {
		t.Fatalf("store phase not updated")
	}
}

type recordingLoader struct {
	rows []SnapshotRow
}

func (l *recordingLoader) LoadSnapshot(rows []SnapshotRow) error {
	l.rows = append(l.rows, rows...)
	return nil
}
