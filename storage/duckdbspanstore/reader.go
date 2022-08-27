package duckdbspanstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
)

const (
	minTimespanForProgressiveSearch       = time.Hour
	minTimespanForProgressiveSearchMargin = time.Minute
	maxProgressiveSteps                   = 4
)

var (
	errNoOperationsTable = errors.New("no operations table supplied")
	errNoIndexTable      = errors.New("no index table supplied")
	errStartTimeRequired = errors.New("start time is required for search queries")
)

type TraceReader struct {
	db              *sql.DB
	indexTable      string
	operationsTable string
	spansTable      string
}

var _ spanstore.Reader = (*TraceReader)(nil)

func NewTraceReader(db *sql.DB, indexTable, operationsTable, spansTable string) *TraceReader {
	return &TraceReader{
		db:              db,
		indexTable:      indexTable,
		operationsTable: operationsTable,
		spansTable:      spansTable,
	}
}

func (r *TraceReader) getTraces(ctx context.Context, traceIDS []model.TraceID) ([]*model.Trace, error) {
	result := make([]*model.Trace, 0, len(traceIDS))
	if len(traceIDS) == 0 {
		return result, nil
	}

	span, _ := opentracing.StartSpanFromContext(ctx, "getTraces")
	defer span.Finish()

	values := make([]interface{}, len(traceIDS))
	for i, traceId := range traceIDS {
		values[i] = traceId.String()
	}

	query := fmt.Sprintf("SELECT model FROM %s WHERE traceID IN (%s)", r.spansTable, "?"+strings.Repeat(",?", len(values)-1))

	span.SetTag("db.statement", query)
	span.SetTag("db.args", values)

	rows, err := r.db.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	traces := map[model.TraceID]*model.Trace{}

	for rows.Next() {
		var serialized string

		err = rows.Scan(&serialized)
		if err != nil {
			return nil, err
		}

		span := model.Span{}

		if serialized[0] == '{' {
			err = json.Unmarshal([]byte(serialized), &span)
		} else {
			err = proto.Unmarshal([]byte(serialized), &span)
		}

		if err != nil {
			return nil, err
		}

		if _, ok := traces[span.TraceID]; !ok {
			traces[span.TraceID] = &model.Trace{}
		}

		traces[span.TraceID].Spans = append(traces[span.TraceID].Spans, &span)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, traceID := range traceIDS {
		if trace, ok := traces[traceID]; ok {
			result = append(result, trace)
		}
	}

	return result, nil
}

func (r *TraceReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer span.Finish()

	traces, err := r.getTraces(ctx, []model.TraceID{traceID})
	if err != nil {
		return nil, err
	}

	if len(traces) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	return traces[0], nil
}

func (r *TraceReader) getStrings(ctx context.Context, sql string, args ...interface{}) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	values := make([]string, 0)

	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return values, nil
}

func (r *TraceReader) GetServices(ctx context.Context) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetServices")
	defer span.Finish()

	if r.operationsTable == "" {
		return nil, errNoOperationsTable
	}

	query := fmt.Sprintf("SELECT service FROM %s GROUP BY service", r.operationsTable)

	span.SetTag("db.statement", query)

	return r.getStrings(ctx, query)
}

func (r *TraceReader) GetOperations(
	ctx context.Context,
	params spanstore.OperationQueryParameters,
) ([]spanstore.Operation, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	if r.operationsTable == "" {
		return nil, errNoOperationsTable
	}

	query := fmt.Sprintf("SELECT operation FROM %s WHERE service = ? GROUP BY operation", r.operationsTable)
	args := []interface{}{params.ServiceName}

	span.SetTag("db.statement", query)
	span.SetTag("db.args", args)

	names, err := r.getStrings(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	operations := make([]spanstore.Operation, len(names))
	for i, name := range names {
		operations[i].Name = name
	}

	return operations, nil
}

func (r *TraceReader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	traceIDs, err := r.FindTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	return r.getTraces(ctx, traceIDs)
}

func (r *TraceReader) FindTraceIDs(ctx context.Context, params *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	if params.StartTimeMin.IsZero() {
		return nil, errStartTimeRequired
	}

	end := params.StartTimeMax
	if end.IsZero() {
		end = time.Now()
	}

	fullTimeSpan := end.Sub(params.StartTimeMin)

	if fullTimeSpan < minTimespanForProgressiveSearch+minTimespanForProgressiveSearchMargin {
		return r.findTraceIDsInRange(ctx, params, params.StartTimeMin, end, nil)
	}

	timeSpan := fullTimeSpan
	for step := 0; step < maxProgressiveSteps; step++ {
		timeSpan /= 2
	}

	if timeSpan < minTimespanForProgressiveSearch {
		timeSpan = minTimespanForProgressiveSearch
	}

	found := make([]model.TraceID, 0)

	for step := 0; step < maxProgressiveSteps; step++ {
		if len(found) >= params.NumTraces {
			break
		}

		// last step has to take care of the whole remainder
		if step == maxProgressiveSteps-1 {
			timeSpan = fullTimeSpan
		}

		start := end.Add(-timeSpan)
		if start.Before(params.StartTimeMin) {
			start = params.StartTimeMin
		}

		if start.After(end) {
			break
		}

		foundInRange, err := r.findTraceIDsInRange(ctx, params, start, end, found)
		if err != nil {
			return nil, err
		}

		found = append(found, foundInRange...)

		end = start
		timeSpan *= 2
	}

	return found, nil
}

func (r *TraceReader) findTraceIDsInRange(ctx context.Context, params *spanstore.TraceQueryParameters, start, end time.Time, skip []model.TraceID) ([]model.TraceID, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "findTraceIDsInRange")
	defer span.Finish()

	if end.Before(start) || end == start {
		return []model.TraceID{}, nil
	}

	span.SetTag("range", end.Sub(start).String())

	if r.indexTable == "" {
		return nil, errNoIndexTable
	}

	query := fmt.Sprintf("SELECT DISTINCT traceID FROM %s WHERE service = ?", r.indexTable)
	args := []interface{}{params.ServiceName}

	if params.OperationName != "" {
		query += " AND operation = ?"
		args = append(args, params.OperationName)
	}

	query += " AND timestamp >= to_timestamp(?)"
	args = append(args, start)

	query += " AND timestamp <= to_timestamp(?)"
	args = append(args, end)

	if params.DurationMin != 0 {
		query += " AND durationUs >= ?"
		args = append(args, params.DurationMin.Microseconds())
	}

	if params.DurationMax != 0 {
		query += " AND durationUs <= ?"
		args = append(args, params.DurationMax.Microseconds())
	}

	for key, value := range params.Tags {
		query += " AND has(tags, ?)"
		args = append(args, fmt.Sprintf("%s=%s", key, value))
	}

	if len(skip) > 0 {
		query += fmt.Sprintf(" AND traceID NOT IN (%s)", "?"+strings.Repeat(",?", len(skip)-1))
		for _, traceID := range skip {
			args = append(args, traceID.String())
		}
	}

	query += " ORDER BY service, timestamp DESC LIMIT ?"
	args = append(args, params.NumTraces-len(skip))

	span.SetTag("db.statement", query)
	span.SetTag("db.args", args)

	traceIDStrings, err := r.getStrings(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	traceIDs := make([]model.TraceID, len(traceIDStrings))
	for i, traceIDString := range traceIDStrings {
		traceID, err := model.TraceIDFromString(traceIDString)
		if err != nil {
			return nil, err
		}
		traceIDs[i] = traceID
	}

	return traceIDs, nil
}
