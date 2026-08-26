package source_test

import (
	"context"
	"testing"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/offset"
	"cdcpipeline/internal/sink"
	"cdcpipeline/internal/source"
)

func TestFullIncrementalHandoffOffset(t *testing.T) {
	log := source.NewInMemoryLog()
	log.PutSnapshot("accounts", []map[string]string{{"id": "1"}, {"id": "2"}})
	log.PutSnapshot("orders", []map[string]string{{"id": "10"}})
	scanner := source.NewFullScanner(log, []string{"accounts", "orders"})
	scanner.SetAfterTable(func(table string) {
		if table == "accounts" {
			_ = log.Append(
				source.LogEntry{Seq: 4, Table: "orders", Op: model.OpInsert, Row: map[string]string{"id": "11"}},
				source.LogEntry{Seq: 5, Table: "accounts", Op: model.OpInsert, Row: map[string]string{"id": "3"}},
			)
		}
	})
	store := offset.NewStore(0)
	target := sink.NewInMemorySink(nil)
	coordinator := source.NewPhaseCoordinator(scanner, store, target)
	checkpoint, err := coordinator.RunFull(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reader := source.NewReader(log, 10, model.NewStats())
	seen := make(map[uint64]bool)
	pos := uint64(checkpoint.NextPos)
	for {
		batch, err := reader.Read(context.Background(), pos)
		if err != nil || batch.IsEmpty() {
			break
		}
		for _, entry := range batch.Entries {
			seen[entry.Seq] = true
		}
		pos = batch.MaxPos
		if len(seen) >= 2 {
			break
		}
	}
	for _, seq := range []uint64{4, 5} {
		if !seen[seq] {
			t.Fatalf("mid-scan change seq=%d lost in full->incremental handoff", seq)
		}
	}
}
