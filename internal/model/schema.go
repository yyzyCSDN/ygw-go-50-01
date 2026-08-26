package model

// ColumnDef describes one column of a table schema version.
type ColumnDef struct {
	Name string
	Type string
}

// TableSchema is an immutable schema version of a table.
type TableSchema struct {
	Table   string
	Version uint64
	Columns []ColumnDef
}

// SchemaChange is a DDL event applied at a source position.
type SchemaChange struct {
	Table     string
	Columns   []ColumnDef
	AppliedAt Position
	ChangeID  string
}

// ColumnNames returns the ordered column names.
func (s TableSchema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}
