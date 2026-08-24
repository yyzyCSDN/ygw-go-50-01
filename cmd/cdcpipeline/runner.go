package main

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"cdcpipeline/internal/mapper"
	"cdcpipeline/internal/model"
	"cdcpipeline/internal/offset"
	"cdcpipeline/internal/parser"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/sink"
	"cdcpipeline/internal/source"
	"cdcpipeline/internal/transport"
)

// Runner owns the end-to-end pipeline loop: read, parse, map, deliver and
// advance the committed offset only after the target acknowledges.
type Runner struct {
	cfg        *Config
	store      *offset.Store
	reader     *source.Reader
	parser     *parser.Parser
	mapper     *mapper.Mapper
	coordinator *offset.Coordinator
	dispatcher *transport.Dispatcher
	phase      *source.PhaseCoordinator
	target     *sink.InMemorySink
	checkpoint *offset.CheckpointManager
	stats      *model.PipelineStats
	filterRevision atomic.Uint64
	pos        uint64
	phaseState atomic.Value
	running    atomic.Bool
}

// NewRunner wires every component of the pipeline.
func NewRunner(
	cfg *Config,
	changeLog *source.InMemoryLog,
	registry *schema.Registry,
	rules *mapper.RuleStore,
	planner *mapper.FKPlanner,
	target *sink.InMemorySink,
	stats *model.PipelineStats,
) *Runner {
	store := offset.NewStore(0)
	mapperInstance := mapper.NewMapper(registry, rules, planner)
	parserInstance := parser.NewParser(registry, cfg.MaxTxnSize)
	reader := source.NewReader(changeLog, cfg.BatchSize, stats)
	reader.SetBackpressureSource(target.Backpressured)
	reader.SetPauseDelay(cfg.PollInterval)
	dispatcher := transport.NewDispatcher(target, transport.DefaultRetryPolicy(), stats)
	dispatcher.SetSplitter(parserInstance)
	dispatcher.SetRollback(func(pos uint64) error { return store.Rollback(pos) })
	coordinator := offset.NewCoordinator(store, dispatcher.Deliver, stats)
	scanner := source.NewFullScanner(changeLog, cfg.Tables)
	phaseCoordinator := source.NewPhaseCoordinator(scanner, store, target)
	checkpoint := offset.NewCheckpointManager(store, cfg.CheckpointInterval)
	runner := &Runner{
		cfg:         cfg,
		store:       store,
		reader:      reader,
		parser:      parserInstance,
		mapper:      mapperInstance,
		coordinator: coordinator,
		dispatcher:  dispatcher,
		phase:       phaseCoordinator,
		target:      target,
		checkpoint:  checkpoint,
		stats:       stats,
	}
	runner.filterRevision.Store(rules.Revision())
	return runner
}

// Run executes the pipeline until the context is cancelled.
func (r *Runner) Run(ctx context.Context) {
	if !r.running.CompareAndSwap(false, true) {
		return
	}
	defer r.running.Store(false)
	for {
		pcp, err := r.phase.RunFull(ctx)
		if err == nil {
			r.phaseState.Store(pcp.Phase)
			r.pos = uint64(pcp.NextPos)
			break
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.cfg.PollInterval):
		}
	}
	r.checkpoint.Start()
	defer r.checkpoint.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		batch, err := r.reader.Read(ctx, r.pos)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.pause(ctx)
			continue
		}
		if batch.IsEmpty() {
			r.pause(ctx)
			continue
		}
		events, err := r.parser.Parse(batch.Entries)
		if err != nil {
			log.Printf("parse batch at %d: %v", r.pos, err)
			r.pause(ctx)
			continue
		}
		for i := range events {
			events[i].FilterRevision = r.filterRevision.Load()
		}
		mapped, err := r.mapper.MapBatch(events)
		if err != nil {
			log.Printf("map batch at %d: %v", r.pos, err)
			r.pause(ctx)
			continue
		}
		delivery := model.NewBatch(mapped)
		if err := r.coordinator.ProcessBatch(ctx, delivery); err != nil {
			log.Printf("deliver batch at %d failed: %v", r.pos, err)
			r.pos = r.store.ReplayFrom()
			r.pause(ctx)
			continue
		}
		r.pos = delivery.MaxPosition()
	}
}

func (r *Runner) pause(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(r.cfg.PollInterval):
	}
}

// Phase returns the current synchronization phase.
func (r *Runner) Phase() model.SyncPhase {
	value := r.phaseState.Load()
	if value == nil {
		return r.store.Phase()
	}
	return value.(model.SyncPhase)
}

// Store exposes the offset store for the monitor and control endpoints.
func (r *Runner) Store() *offset.Store { return r.store }

// Dispatcher exposes the transport dispatcher for the monitor endpoint.
func (r *Runner) Dispatcher() *transport.Dispatcher { return r.dispatcher }

// CheckpointPos returns the last persisted checkpoint position.
func (r *Runner) CheckpointPos() uint64 { return r.checkpoint.Last() }
