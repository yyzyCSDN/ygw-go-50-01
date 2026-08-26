package mapper_test

import (
	"testing"

	"cdcpipeline/internal/mapper"
	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
)

func TestFilterUpdateNoColumnLoss(t *testing.T) {
	registry := schema.NewRegistry()
	if _, err := registry.Apply(model.SchemaChange{
		Table:     "events",
		Columns:   []model.ColumnDef{{Name: "id"}, {Name: "a"}, {Name: "b"}, {Name: "c"}},
		AppliedAt: 0,
		ChangeID:  "init",
	}); err != nil {
		t.Fatal(err)
	}
	rules := mapper.NewRuleStore()
	mapperInstance := mapper.NewMapper(registry, rules, mapper.NewFKPlanner())
	keep := []string{"id", "a"}
	if err := mapperInstance.ApplyFilterUpdate(model.FilterRule{Table: "events", Keep: keep}); err != nil {
		t.Fatal(err)
	}
	// The caller composes the next rule and mutates its own slice after the
	// update. An event parsed before the update must not see that mutation.
	keep[1] = "x"
	event := model.ChangeEvent{
		Table:          "events",
		SchemaVersion:  1,
		FilterRevision: 0,
		Columns: []model.ColumnValue{
			{Name: "id", Value: "1"},
			{Name: "a", Value: "2"},
			{Name: "b", Value: "3"},
			{Name: "c", Value: "4"},
		},
	}
	mapped, err := mapperInstance.Map(event)
	if err != nil {
		t.Fatal(err)
	}
	if len(mapped.Columns) != 4 {
		t.Fatalf("in-flight event lost columns after filter update: %v", mapped.Columns)
	}
	stored, ok := rules.Filter("events")
	if !ok {
		t.Fatal("filter rule missing")
	}
	if stored.Keep[1] != "a" {
		t.Fatalf("stored filter mutated by the caller: %v", stored.Keep)
	}
}
