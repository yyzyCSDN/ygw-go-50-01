package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"cdcpipeline/internal/mapper"
	"cdcpipeline/internal/model"
	"cdcpipeline/internal/offset"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/sink"
	"cdcpipeline/internal/source"
)

// Server exposes the pipeline control plane and the monitor page.
type Server struct {
	cfg            *Config
	runner         *Runner
	store          *offset.Store
	registry       *schema.Registry
	mapperInstance *mapper.Mapper
	stats          *model.PipelineStats
	changeLog      *source.InMemoryLog
	target         *sink.InMemorySink
	httpServer     *http.Server
}

// NewServer wires the HTTP handlers to the pipeline components.
func NewServer(
	cfg *Config,
	runner *Runner,
	registry *schema.Registry,
	rules *mapper.RuleStore,
	planner *mapper.FKPlanner,
	stats *model.PipelineStats,
	changeLog *source.InMemoryLog,
	target *sink.InMemorySink,
) *Server {
	return &Server{
		cfg:            cfg,
		runner:         runner,
		store:          runner.Store(),
		registry:       registry,
		mapperInstance: mapper.NewMapper(registry, rules, planner),
		stats:          stats,
		changeLog:      changeLog,
		target:         target,
	}
}

// Handler builds the HTTP route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/tables", s.handleTables)
	mux.HandleFunc("POST /api/simulate", s.handleSimulate)
	mux.HandleFunc("POST /api/schema", s.handleSchema)
	mux.HandleFunc("POST /api/filter", s.handleFilter)
	mux.HandleFunc("GET /monitor", s.handleMonitor)
	return logRequests(mux)
}

// Serve starts the HTTP server and blocks until it stops.
func (s *Server) Serve() error {
	s.httpServer = &http.Server{
		Addr:              s.cfg.Addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s.httpServer.ListenAndServe()
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	payload := s.Status()
	payload.Phase = latestPhase(s.runner.Phase())
	writeJSON(w, http.StatusOK, payload)
}

func (s *Server) handleTables(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"tables":   tableNames(s.cfg.Tables),
		"rows":     deliveredSummary(s.target, s.cfg.Tables),
		"versions": schemaChangeCount(s.registry),
	})
}

type simulateRequest struct {
	Table string            `json:"table"`
	Op    string            `json:"op"`
	Row   map[string]string `json:"row"`
	TxnID string            `json:"txn_id"`
}

func (s *Server) handleSimulate(w http.ResponseWriter, r *http.Request) {
	var request simulateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if request.Table == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "table is required"})
		return
	}
	op := model.OpType(request.Op)
	if op == model.OpDDL {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "use /api/schema for DDL"})
		return
	}
	if op != model.OpInsert && op != model.OpUpdate && op != model.OpDelete {
		op = model.OpInsert
	}
	entry := source.LogEntry{Table: request.Table, Op: op, Row: request.Row, TxnID: request.TxnID}
	if err := s.changeLog.Append(entry); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

type schemaRequest struct {
	Table   string          `json:"table"`
	Columns []model.ColumnDef `json:"columns"`
	At      uint64          `json:"position"`
	ID      string          `json:"change_id"`
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	var request schemaRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if request.ID == "" {
		request.ID = "manual-" + time.Now().Format("20060102150405")
	}
	if err := s.mapperInstance.ApplySchemaChange(model.SchemaChange{
		Table:     request.Table,
		Columns:   request.Columns,
		AppliedAt: model.Position(request.At),
		ChangeID:  request.ID,
	}); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": s.registry.CurrentVersion(request.Table)})
}

type filterRequest struct {
	Table string   `json:"table"`
	Keep  []string `json:"keep"`
}

func (s *Server) handleFilter(w http.ResponseWriter, r *http.Request) {
	var request filterRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := s.mapperInstance.ApplyFilterUpdate(model.FilterRule{
		Table: request.Table,
		Keep:  request.Keep,
	}); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"applied": true})
}

func (s *Server) handleMonitor(w http.ResponseWriter, _ *http.Request) {
	content, err := os.ReadFile(filepath.Join("web", "monitor.html"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "monitor page unavailable"})
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}
