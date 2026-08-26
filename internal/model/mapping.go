package model

// MappingRule maps a source table to a target table with optional column rename.
type MappingRule struct {
	SourceTable string
	TargetTable string
	ColumnMap   map[string]string
	KeepColumns []string
}

// FilterRule is a per-table column filter snapshot.
type FilterRule struct {
	Table    string
	Keep     []string
	Revision uint64
}

// FKEdge declares a foreign key dependency: Parent must be applied before Child.
type FKEdge struct {
	Parent string
	Child  string
}

// TableBatch is an ordered delivery group for one target table.
type TableBatch struct {
	Table    string
	Events   []ChangeEvent
	Position uint64
}
