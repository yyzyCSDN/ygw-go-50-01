package parser

import (
	"testing"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/source"
)

func TestParseSingleSchemaVersion(t *testing.T) {
	registry := schema.NewRegistry()
	if _, err := registry.Apply(model.SchemaChange{
		Table:    "accounts",
		Columns:  []model.ColumnDef{{Name: "id", Type: "bigint"}, {Name: "name", Type: "varchar"}},
		AppliedAt: 0,
		ChangeID:  "init",
	}); err != nil {
		t.Fatal(err)
	}
	parser := NewParser(registry, 100)
	events, err := parser.Parse([]source.LogEntry{
		{Seq: 1, Table: "accounts", Op: model.OpInsert, Row: map[string]string{"id": "1", "name": "alice"}},
		{Seq: 2, Table: "accounts", Op: model.OpUpdate, Row: map[string]string{"id": "1", "name": "bob"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	first := events[0]
	if first.SchemaVersion != 1 || len(first.Columns) != 2 {
		t.Fatalf("unexpected event: version=%d columns=%d", first.SchemaVersion, len(first.Columns))
	}
	if value, _ := first.Column("name"); value != "alice" {
		t.Fatalf("expected name=alice, got %q", value)
	}
}

func TestTxnSplitterSequenceAndSplit(t *testing.T) {
	splitter := NewTxnSplitter(2)
	events := []model.ChangeEvent{
		{Seq: 1, Table: "t", TxnID: "txn-1"},
		{Seq: 2, Table: "t", TxnID: "txn-1"},
		{Seq: 3, Table: "t", TxnID: "txn-1"},
		{Seq: 4, Table: "t", TxnID: "txn-1"},
		{Seq: 5, Table: "t", TxnID: "txn-1"},
	}
	sequenced := splitter.Sequence(events)
	for i, event := range sequenced {
		if event.TxnSeq != uint64(i+1) {
			t.Fatalf("event %d has txn seq %d", i, event.TxnSeq)
		}
	}
	groups := splitter.Split(sequenced)
	if len(groups) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(groups))
	}
	if len(groups[0]) != 2 || len(groups[1]) != 2 || len(groups[2]) != 1 {
		t.Fatalf("unexpected group sizes: %d %d %d", len(groups[0]), len(groups[1]), len(groups[2]))
	}
}

func TestRowDecoderRoundTrip(t *testing.T) {
	registry := schema.NewRegistry()
	_, _ = registry.Apply(model.SchemaChange{
		Table:    "items",
		Columns:  []model.ColumnDef{{Name: "id", Type: "bigint"}, {Name: "sku", Type: "varchar"}},
		AppliedAt: 0,
		ChangeID:  "init",
	})
	decoder := NewRowDecoder(registry)
	values := []model.ColumnValue{{Name: "id", Value: "5"}, {Name: "sku", Value: "SKU-9"}}
	row := decoder.Encode(values)
	decoded, err := decoder.Decode(source.LogEntry{Table: "items", Row: row}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 || decoded[1].Value != "SKU-9" {
		t.Fatalf("unexpected decoded values: %v", decoded)
	}
}
