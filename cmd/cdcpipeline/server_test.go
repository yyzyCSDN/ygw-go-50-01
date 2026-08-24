package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"cdcpipeline/internal/mapper"
	"cdcpipeline/internal/model"
	"cdcpipeline/internal/schema"
	"cdcpipeline/internal/sink"
	"cdcpipeline/internal/source"
)

func TestHealthEndpoint(t *testing.T) {
	cfg := LoadConfig()
	stats := model.NewStats()
	changeLog := source.NewInMemoryLog()
	registry := schema.NewRegistry()
	rules := mapper.NewRuleStore()
	planner := mapper.NewFKPlanner()
	target := sink.NewInMemorySink(nil)
	runner := NewRunner(cfg, changeLog, registry, rules, planner, target, stats)
	server := NewServer(cfg, runner, registry, rules, planner, stats, changeLog, target)
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if recorder.Body.String() != "ok" {
		t.Fatalf("unexpected body: %q", recorder.Body.String())
	}
}

func TestStatusEndpoint(t *testing.T) {
	cfg := LoadConfig()
	stats := model.NewStats()
	changeLog := source.NewInMemoryLog()
	registry := schema.NewRegistry()
	rules := mapper.NewRuleStore()
	planner := mapper.NewFKPlanner()
	target := sink.NewInMemorySink(nil)
	runner := NewRunner(cfg, changeLog, registry, rules, planner, target, stats)
	server := NewServer(cfg, runner, registry, rules, planner, stats, changeLog, target)
	request := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
}
