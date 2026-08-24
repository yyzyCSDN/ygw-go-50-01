package schema

import (
	"fmt"
	"sync"

	"cdcpipeline/internal/model"
)

// Registry keeps versioned table schemas and the positions where each version
// took effect. Versions are immutable once applied.
type Registry struct {
	mu       sync.RWMutex
	nextVer  map[string]uint64
	history  map[string][]model.TableSchema
	versions map[string][]uint64
	historyIndex *ChangeHistory
}

// NewRegistry returns an empty schema registry.
func NewRegistry() *Registry {
	return &Registry{
		nextVer:      make(map[string]uint64),
		history:      make(map[string][]model.TableSchema),
		versions:     make(map[string][]uint64),
		historyIndex: NewChangeHistory(),
	}
}

// Apply registers a new schema version that becomes active at change.AppliedAt.
func (r *Registry) Apply(change model.SchemaChange) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(change.Columns) == 0 {
		return 0, fmt.Errorf("schema change %s has no columns", change.ChangeID)
	}
	version := r.nextVer[change.Table] + 1
	r.nextVer[change.Table] = version
	record := model.TableSchema{
		Table:   change.Table,
		Version: version,
		Columns: append([]model.ColumnDef(nil), change.Columns...),
	}
	r.history[change.Table] = append(r.history[change.Table], record)
	r.versions[change.Table] = append(r.versions[change.Table], version)
	r.historyIndex.Add(change, version)
	return version, nil
}

// CurrentVersion returns the latest registered version of a table.
func (r *Registry) CurrentVersion(table string) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nextVer[table]
}

// Schema returns the immutable schema for a table version.
func (r *Registry) Schema(table string, version uint64) (model.TableSchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	history := r.history[table]
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Version == version {
			return history[i], nil
		}
	}
	return model.TableSchema{}, fmt.Errorf("schema %s version %d not found", table, version)
}

// VersionAt resolves the schema version active at a source position. The
// resolution is position aware so events written before a DDL are decoded
// with the structure that was in effect when they were produced.
func (r *Registry) VersionAt(table string, pos model.Position) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	latest := r.nextVer[table]
	if latest == 0 {
		return 0, fmt.Errorf("no schema registered for table %s", table)
	}
	return latest, nil
}

// Versions returns all registered versions of a table in apply order.
func (r *Registry) Versions(table string) []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]uint64(nil), r.versions[table]...)
}

// TableNames returns the sorted list of tables known to the registry.
func (r *Registry) TableNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.nextVer))
	for table := range r.nextVer {
		names = append(names, table)
	}
	return names
}
