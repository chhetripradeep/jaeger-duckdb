package main

import (
	"flag"
	"os"
	"path/filepath"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	yaml "gopkg.in/yaml.v3"

	"github.com/chhetripradeep/jaeger-duckdb/storage"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "", "Absolute path of the DuckDB's Jaeger plugin")
	flag.Parse()

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "jaeger-duckdb",
		Level:      hclog.Trace,
		JSONFormat: true,
	})

	cfgFile, err := os.ReadFile(filepath.Clean(cfgPath))
	if err != nil {
		logger.Error("Failed to read config file", "config", cfgPath, "error", err)
		os.Exit(1)
	}

	var cfg storage.Configuration
	err = yaml.Unmarshal(cfgFile, &cfg)
	if err != nil {
		logger.Error("Failed to parse config file", "config", cfgPath, "error", err)
		os.Exit(1)
	}

	var pluginServices shared.PluginServices
	store, err := storage.NewStore(logger, cfg)
	if err != nil {
		logger.Error("Failed to create a storage plugin", "error", err)
		os.Exit(1)
	}

	pluginServices.Store = store
	pluginServices.ArchiveStore = store

	grpc.Serve(&pluginServices)
	err = store.Close()
	if err != nil {
		logger.Error("Failed to close store", "error", err)
		os.Exit(1)
	}
}
