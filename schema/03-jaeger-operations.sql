CREATE VIEW jaeger_operations 
AS
SELECT
    CAST(timestamp AS DATE) AS date,
    service,
    operation,
    count() as count,
FROM
jaeger_index
GROUP BY date, service, operation;
