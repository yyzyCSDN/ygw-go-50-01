package mapper

import (
	"fmt"
	"sync"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
)

// Mapper applies table mapping rules and column filters to parsed events. Each
// schema version owns an immutable mapping snapshot, so events written before
// a DDL or a filter change are mapped with the rules that were active when
// they were produced.
type Mapper struct {
	mu             sync.RWMutex
	reg            *schema.Registry
	rules          *RuleStore
	planner        *FKPlanner
	snapshots      map[mappingKey]*mappingSnapshot
	currentVersion uint64
	currentRevision uint64
}

// mappingKey identifies one immutable mapping layout: the schema version and
// the filter revision that were in effect when an event was parsed.
type mappingKey struct {
	version  uint64
	revision uint64
}

// NewMapper wires the registry, rule store and foreign-key planner.
func NewMapper(reg *schema.Registry, rules *RuleStore, planner *FKPlanner) *Mapper {
	m := &Mapper{
		reg:             reg,
		rules:           rules,
		planner:         planner,
		snapshots:       make(map[mappingKey]*mappingSnapshot),
		currentRevision: rules.Revision(),
	}
	for _, table := range reg.TableNames() {
		version := reg.CurrentVersion(table)
		if version == 0 {
			continue
		}
		snap, err := m.buildSnapshot(table, version)
		if err == nil {
			m.snapshots[mappingKey{version: version, revision: m.currentRevision}] = snap
		}
		if version > m.currentVersion {
			m.currentVersion = version
		}
	}
	return m
}

// ApplySchemaChange registers a new schema version and creates the matching
// mapping snapshot. Older snapshots stay available for in-flight events.
func (m *Mapper) ApplySchemaChange(change model.SchemaChange) error {
	version, err := m.reg.Apply(change)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	snap, err := m.buildSnapshot(change.Table, version)
	if err != nil {
		return err
	}
	m.snapshots[mappingKey{version: version, revision: m.rules.Revision()}] = snap
	m.currentVersion = version
	return nil
}

// ApplyFilterUpdate installs a new filter revision. Only the current mapping
// snapshot is rebuilt; historical snapshots keep the previous filter so an
// event already in flight is never half-filtered.
func (m *Mapper) ApplyFilterUpdate(rule model.FilterRule) error {
	if err := m.rules.ApplyFilter(rule); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	revision := m.rules.Revision()
	if m.currentVersion == 0 {
		return nil
	}
	snap, err := m.buildSnapshot(rule.Table, m.currentVersion)
	if err != nil {
		return err
	}
	m.snapshots[mappingKey{version: m.currentVersion, revision: revision}] = snap
	m.currentRevision = revision
	return nil
}

// Map maps a single event with the snapshot belonging to its schema version.
func (m *Mapper) Map(event model.ChangeEvent) (model.ChangeEvent, error) {
	m.mu.RLock()
	snap := m.snapshots[mappingKey{version: event.SchemaVersion, revision: event.FilterRevision}]
	if snap == nil {
		snap = m.snapshots[mappingKey{version: event.SchemaVersion, revision: 0}]
	}
	if snap == nil {
		snap = m.snapshots[mappingKey{version: m.currentVersion, revision: m.currentRevision}]
	}
	m.mu.RUnlock()
	if snap == nil {
		return event, fmt.Errorf("no mapping snapshot available for table %s", event.Table)
	}
	return snap.Apply(event)
}

// MapBatch maps a list of events and preserves their order.
func (m *Mapper) MapBatch(events []model.ChangeEvent) ([]model.ChangeEvent, error) {
	out := make([]model.ChangeEvent, 0, len(events))
	for _, ev := range events {
		mapped, err := m.Map(ev)
		if err != nil {
			return nil, err
		}
		out = append(out, mapped)
	}
	return out, nil
}

// PlanBatches groups events by target table and orders the groups by foreign
// key dependency so parents are delivered before children.
func (m *Mapper) PlanBatches(events []model.ChangeEvent) ([]model.TableBatch, error) {
	groups := make(map[string][]model.ChangeEvent)
	var tables []string
	for _, ev := range events {
		if _, ok := groups[ev.Table]; !ok {
			tables = append(tables, ev.Table)
		}
		groups[ev.Table] = append(groups[ev.Table], ev)
	}
	ordered := m.planner.Order(tables)
	batches := make([]model.TableBatch, 0, len(ordered))
	for _, table := range ordered {
		group := groups[table]
		pos := uint64(0)
		for _, ev := range group {
			if ev.SourcePos > pos {
				pos = ev.SourcePos
			}
		}
		batches = append(batches, model.TableBatch{
			Table:    table,
			Events:   group,
			Position: pos,
		})
	}
	return batches, nil
}

// MappingDigest computes a stable hash of the current mapping layout; the
// monitor page uses it to surface unexpected mapping skew after a DDL.
func (m *Mapper) MappingDigest() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return hashMapping(m.snapshots)
}

func (m *Mapper) buildSnapshot(table string, version uint64) (*mappingSnapshot, error) {
	tableSchema, err := m.reg.Schema(table, version)
	if err != nil {
		return nil, err
	}
	rule, hasMapping := m.rules.Mapping(table)
	filter, hasFilter := m.rules.Filter(table)
	columnNames := tableSchema.ColumnNames()
	if hasMapping {
		renamed := make([]string, 0, len(columnNames))
		for _, name := range columnNames {
			if target, ok := rule.ColumnMap[name]; ok {
				renamed = append(renamed, target)
			} else {
				renamed = append(renamed, name)
			}
		}
		columnNames = renamed
	}
	snap := &mappingSnapshot{
		table:    table,
		target:   table,
		columns:  columnNames,
		columnMap: make(map[string]string),
	}
	if hasMapping {
		snap.target = rule.TargetTable
		for k, v := range rule.ColumnMap {
			snap.columnMap[k] = v
		}
		if rule.KeepColumns != nil {
			snap.keep = make(map[string]bool, len(rule.KeepColumns))
			for _, name := range rule.KeepColumns {
				snap.keep[name] = true
			}
		}
	}
	if hasFilter {
		snap.filterRevision = filter.Revision
		snap.keep = make(map[string]bool, len(filter.Keep))
		for _, name := range filter.Keep {
			snap.keep[name] = true
		}
	}
	return snap, nil
}
