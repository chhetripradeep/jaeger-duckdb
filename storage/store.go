package storage

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/chhetripradeep/jaeger-duckdb/storage/duckdbdependencystore"
	"github.com/chhetripradeep/jaeger-duckdb/storage/duckdbspanstore"
)

type Store struct {
	db            *sql.DB
	writer        spanstore.Writer
	reader        spanstore.Reader
	archiveWriter spanstore.Writer
	archiveReader spanstore.Reader
}

var (
	_ shared.StoragePlugin        = (*Store)(nil)
	_ shared.ArchiveStoragePlugin = (*Store)(nil)
	_ io.Closer                   = (*Store)(nil)
)

func NewStore(logger hclog.Logger, cfg Configuration) (*Store, error) {
	cfg.setDefaults()

	db, err := connector(cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %q", err)
	}

	if err := runInitScripts(logger, db, cfg); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{
		db:            db,
		writer:        duckdbspanstore.NewSpanWriter(logger, db, cfg.IndexTable, cfg.SpansTable, duckdbspanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize),
		reader:        duckdbspanstore.NewTraceReader(db, cfg.IndexTable, cfg.OperationsTable, cfg.SpansTable),
		archiveWriter: duckdbspanstore.NewSpanWriter(logger, db, "", cfg.SpansArchiveTable, duckdbspanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize),
		archiveReader: duckdbspanstore.NewTraceReader(db, "", "", cfg.SpansArchiveTable),
	}, nil
}

func connector(cfg Configuration) (*sql.DB, error) {
	db, err := sql.Open("duckdb", cfg.DataFile)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *Store) SpanReader() spanstore.Reader {
	return s.reader
}

func (s *Store) SpanWriter() spanstore.Writer {
	return s.writer
}

func (s *Store) DependencyReader() dependencystore.Reader {
	return duckdbdependencystore.NewDependencyStore()
}

func (s *Store) ArchiveSpanReader() spanstore.Reader {
	return s.archiveReader
}

func (s *Store) ArchiveSpanWriter() spanstore.Writer {
	return s.archiveWriter
}

func (s *Store) Close() error {
	return s.db.Close()
}

func runInitScripts(logger hclog.Logger, db *sql.DB, cfg Configuration) error {
	var ddlStatements []string
	filePaths, err := walkMatch(cfg.InitSQLScriptsDir, "*.sql")
	if err != nil {
		return fmt.Errorf("could not list sql files: %q", err)
	}
	sort.Strings(filePaths)
	for _, f := range filePaths {
		ddlStatement, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			return err
		}
		ddlStatements = append(ddlStatements, string(ddlStatement))
	}
	return executeScripts(logger, ddlStatements, db)
}

func executeScripts(logger hclog.Logger, ddlStatements []string, db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for _, statement := range ddlStatements {
		logger.Debug("Running SQL statement", "statement", statement)
		_, err = tx.Exec(statement)
		if err != nil {
			return fmt.Errorf("could not run sql %q: %q", statement, err)
		}
	}
	committed = true
	return tx.Commit()
}

func walkMatch(root, pattern string) ([]string, error) {
	var matches []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if matched, err := filepath.Match(pattern, filepath.Base(path)); err != nil {
			return err
		} else if matched {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}
