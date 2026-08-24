package mapper

import (
	"sync"

	"cdcpipeline/internal/model"
)

// FKPlanner orders table batches by foreign key dependency so parent tables
// are delivered before their children even when events arrive interleaved.
type FKPlanner struct {
	mu     sync.RWMutex
	edges  map[string][]string // child -> parents
	tables map[string]struct{}
}

// NewFKPlanner returns an empty dependency planner.
func NewFKPlanner() *FKPlanner {
	return &FKPlanner{
		edges:  make(map[string][]string),
		tables: make(map[string]struct{}),
	}
}

// SetDependencies replaces the foreign key graph.
func (p *FKPlanner) SetDependencies(edges []model.FKEdge) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.edges = make(map[string][]string, len(edges))
	p.tables = make(map[string]struct{})
	for _, edge := range edges {
		p.edges[edge.Child] = append(p.edges[edge.Child], edge.Parent)
		p.tables[edge.Parent] = struct{}{}
		p.tables[edge.Child] = struct{}{}
	}
}

// Order returns the input tables in dependency order with parents first.
// Tables without dependencies keep their input order.
func (p *FKPlanner) Order(tables []string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	inDegree := make(map[string]int, len(tables))
	children := make(map[string][]string, len(tables))
	seen := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		inDegree[table] = 0
		children[table] = nil
		seen[table] = struct{}{}
	}
	for child, parents := range p.edges {
		if _, ok := seen[child]; !ok {
			continue
		}
		for _, parent := range parents {
			if _, ok := seen[parent]; !ok {
				continue
			}
			inDegree[child]++
			children[parent] = append(children[parent], child)
		}
	}
	var ready []string
	for _, table := range tables {
		if inDegree[table] == 0 {
			ready = append(ready, table)
		}
	}
	var order []string
	for len(ready) > 0 {
		table := ready[0]
		ready = ready[1:]
		order = append(order, table)
		for _, child := range children[table] {
			inDegree[child]--
			if inDegree[child] == 0 {
				ready = append(ready, child)
			}
		}
	}
	// Any table left out by the topo walk (cycle or unknown) is appended in
	// input order so nothing is ever silently dropped.
	for _, table := range tables {
		if !containsString(order, table) {
			order = append(order, table)
		}
	}
	return order
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}
