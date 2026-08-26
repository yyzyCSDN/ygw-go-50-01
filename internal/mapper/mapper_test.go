package mapper

import (
	"testing"

	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
)

func TestMapperAppliesMappingRule(t *testing.T) {
	registry := schema.NewRegistry()
	_, _ = registry.Apply(model.SchemaChange{
		Table:    "orders",
		Columns:  []model.ColumnDef{{Name: "id", Type: "bigint"}, {Name: "status", Type: "varchar"}},
		AppliedAt: 0,
		ChangeID:  "init",
	})
	rules := NewRuleStore()
	_ = rules.ApplyMapping(model.MappingRule{
		SourceTable: "orders",
		TargetTable: "target_orders",
		ColumnMap:   map[string]string{"status": "state"},
	})
	planner := NewFKPlanner()
	mapper := NewMapper(registry, rules, planner)
	mapped, err := mapper.Map(model.ChangeEvent{
		Table:         "orders",
		SchemaVersion: 1,
		FilterRevision: rules.Revision(),
		Columns:       []model.ColumnValue{{Name: "id", Value: "10"}, {Name: "status", Value: "paid"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mapped.Table != "target_orders" {
		t.Fatalf("expected mapped table target_orders, got %s", mapped.Table)
	}
	if value, _ := mapped.Column("state"); value != "paid" {
		t.Fatalf("expected renamed column state=paid, got %q", value)
	}
}

func TestFKPlannerOrdersSortedInput(t *testing.T) {
	planner := NewFKPlanner()
	planner.SetDependencies([]model.FKEdge{
		{Parent: "accounts", Child: "orders"},
		{Parent: "orders", Child: "items"},
	})
	order := planner.Order([]string{"accounts", "orders", "items"})
	if len(order) != 3 || order[0] != "accounts" || order[1] != "orders" || order[2] != "items" {
		t.Fatalf("unexpected order: %v", order)
	}
}

func TestPlanBatchesGroupsByTable(t *testing.T) {
	registry := schema.NewRegistry()
	for _, table := range []string{"accounts", "orders"} {
		_, _ = registry.Apply(model.SchemaChange{
			Table:    table,
			Columns:  []model.ColumnDef{{Name: "id", Type: "bigint"}},
			AppliedAt: 0,
			ChangeID:  "init-" + table,
		})
	}
	rules := NewRuleStore()
	planner := NewFKPlanner()
	mapper := NewMapper(registry, rules, planner)
	batches, err := mapper.PlanBatches([]model.ChangeEvent{
		{Seq: 2, Table: "orders", SchemaVersion: 1, SourcePos: 2},
		{Seq: 1, Table: "accounts", SchemaVersion: 1, SourcePos: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 {
		t.Fatalf("expected 2 table batches, got %d", len(batches))
	}
	seen := make(map[string]bool)
	for _, batch := range batches {
		seen[batch.Table] = true
	}
	if !seen["accounts"] || !seen["orders"] {
		t.Fatalf("missing table in plan: %v", seen)
	}
}

func TestRuleStoreRevision(t *testing.T) {
	rules := NewRuleStore()
	_ = rules.ApplyMapping(model.MappingRule{SourceTable: "a", TargetTable: "b"})
	_ = rules.ApplyFilter(model.FilterRule{Table: "a", Keep: []string{"id"}})
	if rules.Revision() != 2 {
		t.Fatalf("expected revision 2, got %d", rules.Revision())
	}
	rule, ok := rules.Mapping("a")
	if !ok || rule.TargetTable != "b" {
		t.Fatalf("mapping rule not stored")
	}
	filter, ok := rules.Filter("a")
	if !ok || len(filter.Keep) != 1 {
		t.Fatalf("filter rule not stored")
	}
}
