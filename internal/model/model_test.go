package model

import "testing"

func TestChangeEventColumnAccess(t *testing.T) {
	event := ChangeEvent{
		Columns: []ColumnValue{
			{Name: "id", Value: "7"},
			{Name: "name", Value: "alice"},
		},
	}
	if value, isNull := event.Column("name"); isNull || value != "alice" {
		t.Fatalf("expected name=alice not null, got %q null=%v", value, isNull)
	}
	event.SetColumn("name", "bob")
	if value, isNull := event.Column("name"); isNull || value != "bob" {
		t.Fatalf("SetColumn did not update value: %q", value)
	}
}

func TestBatchPositions(t *testing.T) {
	events := []ChangeEvent{
		{Seq: 3, SourcePos: 3, Table: "t"},
		{Seq: 1, SourcePos: 1, Table: "t"},
		{Seq: 2, SourcePos: 2, Table: "t"},
	}
	batch := NewBatch(events)
	if batch.MinPos != 1 || batch.MaxPos != 3 {
		t.Fatalf("unexpected positions: min=%d max=%d", batch.MinPos, batch.MaxPos)
	}
	if batch.MaxPosition() != 3 || batch.Len() != 3 || batch.IsEmpty() {
		t.Fatalf("batch helpers returned unexpected values")
	}
	if NewBatch(nil).IsEmpty() != true {
		t.Fatalf("empty batch must report empty")
	}
}

func TestTableKey(t *testing.T) {
	withKey := ChangeEvent{Table: "orders", Key: "10"}
	if got := withKey.TableKey(); got != "orders/10" {
		t.Fatalf("unexpected table key: %s", got)
	}
	withoutKey := ChangeEvent{Table: "orders", Seq: 5}
	if got := withoutKey.TableKey(); got != "orders/seq/5" {
		t.Fatalf("unexpected fallback key: %s", got)
	}
}
