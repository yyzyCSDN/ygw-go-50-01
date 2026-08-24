package parser

import (
	"testing"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/source"
)

// TestParseDecodesByEventSchemaVersion is the regression for the schema-version
// switch bug: after a DDL is applied at position P, an event written before P
// must still be decoded with the structure that was in effect when it was
// produced. Before the fix, Parse resolved the live head version for every
// entry, so pre-DDL rows were re-decoded with the new column layout and their
// values landed in the wrong target columns.
func TestParseDecodesByEventSchemaVersion(t *testing.T) {
	registry := schema.NewRegistry()
	// v1: id, name — active from position 0.
	if _, err := registry.Apply(model.SchemaChange{
		Table:     "accounts",
		Columns:  []model.ColumnDef{{Name: "id", Type: "bigint"}, {Name: "name", Type: "varchar"}},
		AppliedAt: 0,
		ChangeID:  "init",
	}); err != nil {
		t.Fatal(err)
	}
	// v2: id, name, email — active from position 10.
	if _, err := registry.Apply(model.SchemaChange{
		Table:     "accounts",
		Columns:  []model.ColumnDef{{Name: "id", Type: "bigint"}, {Name: "name", Type: "varchar"}, {Name: "email", Type: "varchar"}},
		AppliedAt: 10,
		ChangeID:  "add-email",
	}); err != nil {
		t.Fatal(err)
	}

	parser := NewParser(registry, 100)
	events, err := parser.Parse([]source.LogEntry{
		// Written at seq 5, before the DDL: must decode under v1 (2 columns).
		{Seq: 5, Table: "accounts", Op: model.OpInsert, Row: map[string]string{"id": "1", "name": "alice"}},
		// Written at seq 15, after the DDL: must decode under v2 (3 columns).
		{Seq: 15, Table: "accounts", Op: model.OpInsert, Row: map[string]string{"id": "2", "name": "bob", "email": "bob@x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	before := events[0]
	if before.SchemaVersion != 1 {
		t.Fatalf("pre-DDL event must use schema version 1, got %d", before.SchemaVersion)
	}
	if len(before.Columns) != 2 {
		t.Fatalf("pre-DDL event must carry 2 columns under v1, got %d", len(before.Columns))
	}
	if name, _ := before.Column("name"); name != "alice" {
		t.Fatalf("pre-DDL event name misaligned: got %q", name)
	}

	after := events[1]
	if after.SchemaVersion != 2 {
		t.Fatalf("post-DDL event must use schema version 2, got %d", after.SchemaVersion)
	}
	if len(after.Columns) != 3 {
		t.Fatalf("post-DDL event must carry 3 columns under v2, got %d", len(after.Columns))
	}
	if email, _ := after.Column("email"); email != "bob@x" {
		t.Fatalf("post-DDL event email misaligned: got %q", email)
	}
}
