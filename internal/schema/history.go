package schema

import (
	"sort"

	"cdcpipeline/internal/model"
)

// ChangeHistory indexes schema changes by table and source position so the
// registry can answer position-aware version lookups without scanning every
// table on every parse.
type ChangeHistory struct {
	changes map[string][]model.SchemaChange
	version map[string][]uint64
}

// NewChangeHistory returns an empty change index.
func NewChangeHistory() *ChangeHistory {
	return &ChangeHistory{
		changes: make(map[string][]model.SchemaChange),
		version: make(map[string][]uint64),
	}
}

// Add records a schema change and the version it introduced.
func (h *ChangeHistory) Add(change model.SchemaChange, version uint64) {
	h.changes[change.Table] = append(h.changes[change.Table], change)
	h.version[change.Table] = append(h.version[change.Table], version)
}

// VersionAt returns the version that was active at the given position,
// falling back to the latest version when the position predates all changes.
func (h *ChangeHistory) VersionAt(table string, pos model.Position, fallback uint64) uint64 {
	changes := h.changes[table]
	versions := h.version[table]
	if len(changes) == 0 {
		return fallback
	}
	// changes are appended in position order; find the last change applied at
	// or before pos.
	idx := sort.Search(len(changes), func(i int) bool {
		return uint64(changes[i].AppliedAt) > uint64(pos)
	})
	if idx == 0 {
		return versions[0]
	}
	return versions[idx-1]
}
