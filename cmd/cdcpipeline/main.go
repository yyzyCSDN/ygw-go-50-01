package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cdcpipeline/internal/mapper"
	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/sink"
	"cdcpipeline/internal/source"
)

func main() {
	cfg := LoadConfig()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	stats := model.NewStats()
	changeLog := source.NewInMemoryLog()
	registry := schema.NewRegistry()
	rules := mapper.NewRuleStore()
	planner := mapper.NewFKPlanner()
	fkEdges := []model.FKEdge{
		{Parent: "accounts", Child: "orders"},
		{Parent: "orders", Child: "items"},
	}
	planner.SetDependencies(fkEdges)
	target := sink.NewInMemorySink(fkEdges)
	target.SetWriteDelay(cfg.SinkWriteDelay)
	target.SetWriteTimeout(cfg.SinkWriteTimeout)

	seedDemoData(cfg, changeLog, registry, target)

	runner := NewRunner(cfg, changeLog, registry, rules, planner, target, stats)
	server := NewServer(cfg, runner, registry, rules, planner, stats, changeLog, target)

	go runner.Run(ctx)
	log.Printf("cdcpipeline listening on %s", cfg.Addr)
	if err := server.Serve(); err != nil {
		log.Printf("server stopped: %v", err)
	}
}

// seedDemoData registers initial schemas, full-table snapshots and a small
// batch of change events so the demo server has visible state immediately.
func seedDemoData(cfg *Config, changeLog *source.InMemoryLog, registry *schema.Registry, target *sink.InMemorySink) {
	schemas := map[string][]model.ColumnDef{
		"accounts": {
			{Name: "id", Type: "bigint"},
			{Name: "name", Type: "varchar"},
			{Name: "balance", Type: "decimal"},
		},
		"orders": {
			{Name: "id", Type: "bigint"},
			{Name: "account_id", Type: "bigint"},
			{Name: "status", Type: "varchar"},
		},
		"items": {
			{Name: "id", Type: "bigint"},
			{Name: "order_id", Type: "bigint"},
			{Name: "sku", Type: "varchar"},
			{Name: "qty", Type: "int"},
		},
	}
	for table, columns := range schemas {
		if _, err := registry.Apply(model.SchemaChange{
			Table:    table,
			Columns:  columns,
			AppliedAt: 0,
			ChangeID:  "bootstrap-" + table,
		}); err != nil {
			log.Printf("seed schema %s: %v", table, err)
		}
	}
	changeLog.PutSnapshot("accounts", []map[string]string{
		{"id": "1", "name": "alice", "balance": "100"},
		{"id": "2", "name": "bob", "balance": "50"},
	})
	changeLog.PutSnapshot("orders", []map[string]string{
		{"id": "10", "account_id": "1", "status": "paid"},
	})
	changeLog.PutSnapshot("items", []map[string]string{
		{"id": "100", "order_id": "10", "sku": "SKU-1", "qty": "2"},
	})
	_ = cfg
}
