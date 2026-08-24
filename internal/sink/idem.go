package sink

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

// IdempotencyGuard tracks which delivery keys have already been applied to the
// target so a replay never corrupts rows that are already there.
type IdempotencyGuard struct {
	mu   sync.Mutex
	keys map[uint64]struct{}
}

// NewIdempotencyGuard returns an empty guard.
func NewIdempotencyGuard() *IdempotencyGuard {
	return &IdempotencyGuard{keys: make(map[uint64]struct{})}
}

// AlreadyApplied reports whether the key has been applied.
func (g *IdempotencyGuard) AlreadyApplied(deliveryKey string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.keys[xxhash.Sum64String(deliveryKey)]
	return ok
}

// Apply records the key as applied.
func (g *IdempotencyGuard) Apply(deliveryKey string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.keys[xxhash.Sum64String(deliveryKey)] = struct{}{}
}

// Size returns the number of applied keys.
func (g *IdempotencyGuard) Size() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.keys)
}
