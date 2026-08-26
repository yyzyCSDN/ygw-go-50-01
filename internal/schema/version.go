package schema

import "cdcpipeline/internal/model"

// BindVersion resolves and validates the schema version for an event position.
// It is the single entry point used by the parser so the whole pipeline
// agrees on which structure an event must be decoded with.
func BindVersion(reg *Registry, table string, pos model.Position) (uint64, error) {
	return reg.VersionAt(table, pos)
}

// VersionChanged reports whether the schema version for a table differs
// between two source positions.
func VersionChanged(reg *Registry, table string, before, after model.Position) bool {
	left, err := reg.VersionAt(table, before)
	if err != nil {
		return false
	}
	right, err := reg.VersionAt(table, after)
	if err != nil {
		return false
	}
	return left != right
}
