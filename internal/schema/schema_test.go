package schema

import (
	"testing"

	"cdcpipeline/internal/model"
)

func TestRegistryApplyAndLookup(t *testing.T) {
	registry := NewRegistry()
	version, err := registry.Apply(model.SchemaChange{
		Table:    "accounts",
		Columns:  []model.ColumnDef{{Name: "id", Type: "bigint"}, {Name: "name", Type: "varchar"}},
		AppliedAt: 0,
		ChangeID:  "init",
	})
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || registry.CurrentVersion("accounts") != 1 {
		t.Fatalf("unexpected version state: applied=%d current=%d", version, registry.CurrentVersion("accounts"))
	}
	tableSchema, err := registry.Schema("accounts", 1)
	if err != nil {
		t.Fatal(err)
	}
	names := tableSchema.ColumnNames()
	if len(names) != 2 || names[0] != "id" || names[1] != "name" {
		t.Fatalf("unexpected column names: %v", names)
	}
	at, err := registry.VersionAt("accounts", model.Position(42))
	if err != nil {
		t.Fatal(err)
	}
	if at != 1 {
		t.Fatalf("expected version 1 at position 42, got %d", at)
	}
	if _, err := registry.Schema("accounts", 99); err == nil {
		t.Fatalf("expected error for unknown version")
	}
}

func TestRegistryRejectsEmptyChange(t *testing.T) {
	registry := NewRegistry()
	if _, err := registry.Apply(model.SchemaChange{Table: "t", ChangeID: "empty"}); err == nil {
		t.Fatalf("expected error for empty schema change")
	}
}
