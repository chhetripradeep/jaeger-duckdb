package storage

import "time"

const (
	defaultBatchSize         = 1_000
	defaultBatchDelay        = time.Second * 1
	defaultDataFile          = "./jaeger.db"
	defaultEncoding          = "json"
	defaultIndexTable        = "jaeger_index"
	defaultInitSQLScriptsDir = "./schema"
	defaultOperationsTable   = "jaeger_operations"
	defaultSpansTable        = "jaeger_spans"
	defaultSpansArchiveTable = "jaeger_spans_archive"
)

type Configuration struct {
	BatchWriteSize     int64         `yaml:"batch_write_size"`
	BatchFlushInterval time.Duration `yaml:"batch_flush_interval"`
	DataFile           string        `yaml:"datafile"`
	Encoding           string        `yaml:"encoding"`
	IndexTable         string        `yaml:"index_table"`
	InitSQLScriptsDir  string        `yaml:"init_sql_scripts_dir"`
	OperationsTable    string        `yaml:"operations_table"`
	SpansTable         string        `yaml:"spans_table"`
	SpansArchiveTable  string        `yaml:"spans_archive_table"`
}

func (cfg *Configuration) setDefaults() {
	if cfg.BatchWriteSize == 0 {
		cfg.BatchWriteSize = defaultBatchSize
	}
	if cfg.BatchFlushInterval == 0 {
		cfg.BatchFlushInterval = defaultBatchDelay
	}
	if cfg.DataFile == "" {
		cfg.DataFile = defaultDataFile
	}
	if cfg.Encoding == "" {
		cfg.Encoding = defaultEncoding
	}
	if cfg.IndexTable == "" {
		cfg.IndexTable = defaultIndexTable
	}
	if cfg.InitSQLScriptsDir == "" {
		cfg.InitSQLScriptsDir = defaultInitSQLScriptsDir
	}
	if cfg.OperationsTable == "" {
		cfg.OperationsTable = defaultOperationsTable
	}
	if cfg.SpansTable == "" {
		cfg.SpansTable = defaultSpansTable
	}
	if cfg.SpansArchiveTable == "" {
		cfg.SpansArchiveTable = defaultSpansArchiveTable
	}
}
