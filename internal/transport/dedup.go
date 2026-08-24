package transport

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

// Dedup remembers every event key that has been accepted by the target for the
// lifetime of the process. Marks survive retries and offset rollbacks so a
// replay never re-applies an already delivered change.
type Dedup struct {
	mu   sync.Mutex
	seen map[uint64]struct{}
}

// NewDedup returns an empty dedup registry.
func NewDedup() *Dedup {
	return &Dedup{seen: make(map[uint64]struct{})}
}

// Key hashes a delivery key into the dedup space.
func Key(deliveryKey string) uint64 {
	return xxhash.Sum64String(deliveryKey)
}

// Seen reports whether the key has already been delivered.
func (d *Dedup) Seen(deliveryKey string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[Key(deliveryKey)]
	return ok
}

// Mark records a delivered key.
func (d *Dedup) Mark(deliveryKey string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[Key(deliveryKey)] = struct{}{}
}

// Size returns the number of marked keys.
func (d *Dedup) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
