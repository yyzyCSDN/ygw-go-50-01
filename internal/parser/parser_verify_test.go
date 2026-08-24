package parser_test

import (
	"testing"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/parser"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/source"
)

func TestParseUsesOffsetSchemaVersion(t *testing.T) {
	registry := schema.NewRegistry()
	if _, err := registry.Apply(model.SchemaChange{
		Table:     "orders",
		Columns:   []model.ColumnDef{{Name: "id"}, {Name: "a"}, {Name: "b"}},
		AppliedAt: 0,
		ChangeID:  "v1",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Apply(model.SchemaChange{
		Table:     "orders",
		Columns:   []model.ColumnDef{{Name: "id"}, {Name: "b"}, {Name: "a"}},
		AppliedAt: model.Position(500),
		ChangeID:  "v2",
	}); err != nil {
		t.Fatal(err)
	}
	p := parser.NewParser(registry, 100)
	events, err := p.Parse([]source.LogEntry{
		{Seq: 100, Table: "orders", Op: model.OpInsert, Row: map[string]string{"id": "10", "a": "1", "b": "2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := events[0]
	if event.SchemaVersion != 1 {
		t.Fatalf("event written before the DDL must use schema version 1, got %d", event.SchemaVersion)
	}
	if value, _ := event.Column("a"); value != "1" {
		t.Fatalf("column a misaligned: %q", value)
	}
	if value, _ := event.Column("b"); value != "2" {
		t.Fatalf("column b misaligned: %q", value)
	}
}
