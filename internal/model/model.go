package model

import "fmt"

// OpType describes the kind of change captured from the source log.
type OpType string

const (
	OpInsert OpType = "insert"
	OpUpdate OpType = "update"
	OpDelete OpType = "delete"
	OpDDL    OpType = "ddl"
)

// ColumnValue is a single column value carried by a change event.
type ColumnValue struct {
	Name   string
	Value  string
	IsNull bool
}

// ChangeEvent is one parsed change row ready to be mapped and delivered.
type ChangeEvent struct {
	Seq           uint64
	Table         string
	Op            OpType
	Columns       []ColumnValue
	Key           string
	TxnID         string
	TxnSeq        uint64
	SchemaVersion uint64
	FilterRevision uint64
	SourcePos     uint64
	Filtered      bool
}

// Column returns the value and null flag of the named column.
func (e ChangeEvent) Column(name string) (string, bool) {
	for _, col := range e.Columns {
		if col.Name == name {
			return col.Value, col.IsNull
		}
	}
	return "", false
}

// SetColumn replaces or appends a column value.
func (e *ChangeEvent) SetColumn(name, value string) {
	for i := range e.Columns {
		if e.Columns[i].Name == name {
			e.Columns[i].Value = value
			e.Columns[i].IsNull = false
			return
		}
	}
	e.Columns = append(e.Columns, ColumnValue{Name: name, Value: value})
}

// TableKey computes the stable delivery key for the row.
func (e ChangeEvent) TableKey() string {
	if e.Key != "" {
		return e.Table + "/" + e.Key
	}
	return e.Table + "/seq/" + fmt.Sprintf("%d", e.Seq)
}

// Batch is a group of events read from one source segment.
type Batch struct {
	Events []ChangeEvent
	MinPos uint64
	MaxPos uint64
}

// NewBatch builds a batch and computes min/max source positions.
func NewBatch(events []ChangeEvent) Batch {
	b := Batch{Events: events}
	for _, ev := range events {
		if b.MinPos == 0 || ev.SourcePos < b.MinPos {
			b.MinPos = ev.SourcePos
		}
		if ev.SourcePos > b.MaxPos {
			b.MaxPos = ev.SourcePos
		}
	}
	return b
}

// MaxPosition returns the highest source position covered by the batch.
func (b Batch) MaxPosition() uint64 { return b.MaxPos }

// Len returns the number of events in the batch.
func (b Batch) Len() int { return len(b.Events) }

// IsEmpty reports whether the batch carries no events.
func (b Batch) IsEmpty() bool { return len(b.Events) == 0 }
