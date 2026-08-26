package sink

import (
	"context"
	"errors"
	"testing"

	"cdcpipeline/internal/model"
)

func TestInMemorySinkWriteAndRows(t *testing.T) {
	sink := NewInMemorySink(nil)
	event := model.ChangeEvent{
		Seq:     1,
		Table:   "accounts",
		Op:      model.OpInsert,
		Key:     "1",
		Columns: []model.ColumnValue{{Name: "id", Value: "1"}, {Name: "name", Value: "alice"}},
	}
	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	rows := sink.Rows("accounts")
	if len(rows) != 1 || rows["1"]["name"] != "alice" {
		t.Fatalf("unexpected rows: %v", rows)
	}
	if sink.RowCount("accounts") != 1 {
		t.Fatalf("expected row count 1")
	}
}

func TestInMemorySinkIdempotent(t *testing.T) {
	sink := NewInMemorySink(nil)
	event := model.ChangeEvent{Seq: 1, Table: "t", Key: "1", Op: model.OpInsert, Columns: []model.ColumnValue{{Name: "id", Value: "1"}}}
	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(sink.AppliedOrder()) != 1 {
		t.Fatalf("duplicate write must be ignored, applied %d times", len(sink.AppliedOrder()))
	}
}

func TestInMemorySinkFKViolation(t *testing.T) {
	sink := NewInMemorySink([]model.FKEdge{{Parent: "accounts", Child: "orders"}})
	event := model.ChangeEvent{
		Table: "orders",
		Op:    model.OpInsert,
		Key:   "10",
		Columns: []model.ColumnValue{
			{Name: "id", Value: "10"},
			{Name: "account_id", Value: "999"},
		},
	}
	if err := sink.Write(context.Background(), event); !errors.Is(err, ErrFKViolation) {
		t.Fatalf("expected FK violation, got %v", err)
	}
}

func TestInMemorySinkDelete(t *testing.T) {
	sink := NewInMemorySink(nil)
	_ = sink.Write(context.Background(), model.ChangeEvent{Seq: 1, Table: "t", Key: "1", Op: model.OpInsert, Columns: []model.ColumnValue{{Name: "id", Value: "1"}}})
	_ = sink.Write(context.Background(), model.ChangeEvent{Seq: 2, Table: "t", Key: "1", Op: model.OpDelete, Columns: []model.ColumnValue{{Name: "id", Value: "1"}}})
	if sink.RowCount("t") != 0 {
		t.Fatalf("delete must remove the row")
	}
}
