package mapper

import (
	"fmt"
	"sync"

	"cdcpipeline/internal/model"
)

// RuleStore holds mapping and filter rules. Rules are replaced atomically with
// a new revision; shared slices are never mutated in place so readers always
// observe a consistent rule.
type RuleStore struct {
	mu       sync.RWMutex
	mappings map[string]model.MappingRule
	filters  map[string]model.FilterRule
	revision uint64
}

// NewRuleStore returns an empty rule store.
func NewRuleStore() *RuleStore {
	return &RuleStore{
		mappings: make(map[string]model.MappingRule),
		filters:  make(map[string]model.FilterRule),
	}
}

// ApplyMapping installs a mapping rule and bumps the revision.
func (rs *RuleStore) ApplyMapping(rule model.MappingRule) error {
	if rule.SourceTable == "" || rule.TargetTable == "" {
		return fmt.Errorf("mapping rule needs source and target table")
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.revision++
	rs.mappings[rule.SourceTable] = rule
	return nil
}

// ApplyFilter installs a new filter rule revision. The rule is copied so later
// updates never mutate a revision already handed to a mapper snapshot.
func (rs *RuleStore) ApplyFilter(rule model.FilterRule) error {
	if rule.Table == "" {
		return fmt.Errorf("filter rule needs a table")
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.revision++
	rule.Revision = rs.revision
	rs.filters[rule.Table] = rule
	return nil
}

// Mapping returns the mapping rule for a source table.
func (rs *RuleStore) Mapping(sourceTable string) (model.MappingRule, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	rule, ok := rs.mappings[sourceTable]
	return rule, ok
}

// Filter returns the current filter rule for a table.
func (rs *RuleStore) Filter(table string) (model.FilterRule, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	rule, ok := rs.filters[table]
	return rule, ok
}

// Revision returns the current rule generation.
func (rs *RuleStore) Revision() uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.revision
}
