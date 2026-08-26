package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the runtime settings of the CDC pipeline server.
type Config struct {
	Addr               string
	BatchSize          int
	MaxTxnSize         int
	CheckpointInterval time.Duration
	PollInterval       time.Duration
	SinkWriteDelay     time.Duration
	SinkWriteTimeout   time.Duration
	Tables             []string
}

// LoadConfig reads the configuration from environment variables with sane
// defaults for the demo server.
func LoadConfig() *Config {
	cfg := &Config{
		Addr:               ":8123",
		BatchSize:          100,
		MaxTxnSize:         200,
		CheckpointInterval: time.Second,
		PollInterval:       50 * time.Millisecond,
		SinkWriteDelay:     0,
		SinkWriteTimeout:   0,
		Tables:             []string{"accounts", "orders", "items"},
	}
	if value := os.Getenv("CDC_ADDR"); value != "" {
		cfg.Addr = value
	}
	if value := os.Getenv("CDC_BATCH_SIZE"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.BatchSize = parsed
		}
	}
	if value := os.Getenv("CDC_MAX_TXN_SIZE"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.MaxTxnSize = parsed
		}
	}
	if value := os.Getenv("CDC_CHECKPOINT_MS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.CheckpointInterval = time.Duration(parsed) * time.Millisecond
		}
	}
	if value := os.Getenv("CDC_POLL_MS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.PollInterval = time.Duration(parsed) * time.Millisecond
		}
	}
	if value := os.Getenv("CDC_SINK_DELAY_MS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.SinkWriteDelay = time.Duration(parsed) * time.Millisecond
		}
	}
	if value := os.Getenv("CDC_SINK_TIMEOUT_MS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			cfg.SinkWriteTimeout = time.Duration(parsed) * time.Millisecond
		}
	}
	if value := os.Getenv("CDC_TABLES"); value != "" {
		parts := strings.Split(value, ",")
		tables := make([]string, 0, len(parts))
		for _, part := range parts {
			if table := strings.TrimSpace(part); table != "" {
				tables = append(tables, table)
			}
		}
		if len(tables) > 0 {
			cfg.Tables = tables
		}
	}
	return cfg
}
