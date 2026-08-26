package offset_test

import (
	"sync"
	"testing"

	"cdcpipeline/internal/offset"
)

func TestCheckpointNoRegress(t *testing.T) {
	store := offset.NewStore(0)
	manager := offset.NewCheckpointManager(store, 0)
	if err := store.Advance(100); err != nil {
		t.Fatal(err)
	}
	if err := manager.Write(); err != nil {
		t.Fatal(err)
	}
	if restored := store.Restore(); restored != 100 {
		t.Fatalf("checkpoint regressed: restart resumed from %d instead of 100", restored)
	}
}

func TestConcurrentAckCheckpointMonotonic(t *testing.T) {
	store := offset.NewStore(0)
	manager := offset.NewCheckpointManager(store, 0)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 1; n <= 200; n++ {
				_ = store.Advance(uint64(n))
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 200; n++ {
				_ = manager.Write()
			}
		}()
	}
	wg.Wait()
	if restored := store.Restore(); restored < 200 {
		t.Fatalf("concurrent checkpoints regressed committed position: %d", restored)
	}
}
