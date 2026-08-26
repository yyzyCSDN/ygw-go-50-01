package mapper

import (
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"

	"cdcpipeline/internal/model"
)

// mappingSnapshot is an immutable view of one schema version: target table
// name, column renames and the column filter that were in effect.
type mappingSnapshot struct {
	table          string
	target         string
	columns        []string
	columnMap      map[string]string
	keep           map[string]bool
	filterRevision uint64
}

// Apply maps one event through the snapshot. Columns are emitted in the
// snapshot's own target order and values are looked up by name, so an event
// decoded with an older schema version is never misaligned by a later DDL.
func (s *mappingSnapshot) Apply(event model.ChangeEvent) (model.ChangeEvent, error) {
	if event.Table != s.table {
		return event, fmt.Errorf("snapshot for %s cannot map event for %s", s.table, event.Table)
	}
	out := event
	out.Table = s.target
	cols := make([]model.ColumnValue, 0, len(s.columns))
	for _, targetCol := range s.columns {
		if s.keep != nil && !s.keep[targetCol] {
			continue
		}
		sourceName := targetCol
		for source, target := range s.columnMap {
			if target == targetCol {
				sourceName = source
				break
			}
		}
		found := false
		for _, col := range event.Columns {
			if col.Name == sourceName {
				cols = append(cols, model.ColumnValue{Name: targetCol, Value: col.Value, IsNull: col.IsNull})
				found = true
				break
			}
		}
		if !found {
			cols = append(cols, model.ColumnValue{Name: targetCol, IsNull: true})
		}
	}
	out.Columns = cols
	return out, nil
}

// Digest hashes the target layout and filter revision of the snapshot.
func (s *mappingSnapshot) Digest() string {
	names := append([]string(nil), s.columns...)
	sort.Strings(names)
	payload := s.table + "|" + s.target + "|" + fmt.Sprint(s.filterRevision) + "|"
	for _, name := range names {
		payload += name + ","
	}
	return fmt.Sprintf("%016x", xxhash.Sum64String(payload))
}

func hashMapping(snapshots map[mappingKey]*mappingSnapshot) string {
	keys := make([]int, 0, len(snapshots))
	for key := range snapshots {
		keys = append(keys, int(key.version)*100000+int(key.revision))
	}
	sort.Ints(keys)
	payload := ""
	for _, encoded := range keys {
		key := mappingKey{version: uint64(encoded / 100000), revision: uint64(encoded % 100000)}
		snap := snapshots[key]
		if snap != nil {
			payload += snap.Digest() + ";"
		}
	}
	return fmt.Sprintf("%016x", xxhash.Sum64String(payload))
}
