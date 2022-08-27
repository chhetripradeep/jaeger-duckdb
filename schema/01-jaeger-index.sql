CREATE TABLE IF NOT EXISTS jaeger_index (
     timestamp Timestamp,
     traceID String,
     service String,
     operation String,
     durationUs UInt64,
     tags String[],
);
