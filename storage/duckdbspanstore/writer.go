package duckdbspanstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type Encoding string

const (
	// EncodingJSON is used for spans encoded as JSON
	EncodingJSON Encoding = "json"
)

type SpanWriter struct {
	logger     hclog.Logger
	db         *sql.DB
	indexTable string
	spansTable string
	encoding   Encoding
	delay      time.Duration
	size       int64
	spans      chan *model.Span
	finish     chan bool
	done       sync.WaitGroup
}

var _ spanstore.Writer = (*SpanWriter)(nil)

func NewSpanWriter(logger hclog.Logger, db *sql.DB, indexTable, spansTable string, encoding Encoding, delay time.Duration, size int64) *SpanWriter {
	writer := &SpanWriter{
		logger:     logger,
		db:         db,
		indexTable: indexTable,
		spansTable: spansTable,
		encoding:   encoding,
		delay:      delay,
		size:       size,
		spans:      make(chan *model.Span, size),
		finish:     make(chan bool),
	}

	go writer.backgroundWriter()

	return writer
}

func (w *SpanWriter) backgroundWriter() {
	batch := make([]*model.Span, 0, w.size)

	timer := time.After(w.delay)
	last := time.Now()

	for {
		w.done.Add(1)

		flush := false
		finish := false

		select {
		case span := <-w.spans:
			batch = append(batch, span)
			flush = len(batch) == cap(batch)
			if flush {
				w.logger.Debug("Flush due to batch size", "size", len(batch))
			}
		case <-timer:
			timer = time.After(w.delay)
			flush = time.Since(last) > w.delay && len(batch) > 0
			if flush {
				w.logger.Debug("Flush due to timer")
			}
		case <-w.finish:
			finish = true
			flush = len(batch) > 0
			w.logger.Debug("Finish channel")
		}

		if flush {
			if err := w.writeBatch(batch); err != nil {
				w.logger.Error("Could not write a batch of spans", "error", err)
			}

			batch = make([]*model.Span, 0, w.size)
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

func (w *SpanWriter) writeBatch(batch []*model.Span) error {
	w.logger.Debug("Writing spans", "size", len(batch))
	if err := w.writeModelBatch(batch); err != nil {
		return err
	}

	if w.indexTable != "" {
		if err := w.writeIndexBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (w *SpanWriter) writeModelBatch(batch []*model.Span) error {
	var err error
	for _, span := range batch {
		var serialized []byte

		if w.encoding == EncodingJSON {
			serialized, err = json.Marshal(span)
		} else {
			serialized, err = proto.Marshal(span)
		}
		if err != nil {
			return err
		}

		_, err = w.db.Exec(
			fmt.Sprintf(
				"INSERT INTO %s (timestamp, traceID, model) VALUES ('%s', '%s', '%s')",
				w.spansTable,
				span.StartTime.Format(time.RFC3339),
				span.TraceID.String(),
				serialized,
			),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *SpanWriter) writeIndexBatch(batch []*model.Span) error {
	var err error
	for _, span := range batch {
		_, err = w.db.Exec(
			fmt.Sprintf(
				"INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags) VALUES ('%s', '%s', '%s', '%s', %d, [%s])",
				w.indexTable,
				span.StartTime.Format(time.RFC3339),
				span.TraceID.String(),
				span.Process.ServiceName,
				span.OperationName,
				span.Duration.Microseconds(),
				strings.Join(uniqueTagsForSpan(span), ","),
			),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *SpanWriter) WriteSpan(_ context.Context, span *model.Span) error {
	w.spans <- span
	return nil
}

func (w *SpanWriter) Close() error {
	w.finish <- true
	w.done.Wait()
	w.db.Close()
	return nil
}

func uniqueTagsForSpan(span *model.Span) []string {
	uniqueTags := make(map[string]struct{}, len(span.Tags)+len(span.Process.Tags))

	for i := range span.Tags {
		uniqueTags[tagString(&span.GetTags()[i])] = struct{}{}
	}

	for i := range span.Process.Tags {
		uniqueTags[tagString(&span.GetProcess().GetTags()[i])] = struct{}{}
	}

	for _, event := range span.Logs {
		for i := range event.Fields {
			uniqueTags[tagString(&event.GetFields()[i])] = struct{}{}
		}
	}

	tags := make([]string, 0, len(uniqueTags))

	for kv := range uniqueTags {
		tags = append(tags, kv)
	}

	sort.Strings(tags)

	return tags
}

func tagString(kv *model.KeyValue) string {
	return fmt.Sprintf("'%s=%s'", kv.Key, kv.AsString())
}
