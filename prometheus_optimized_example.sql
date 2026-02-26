-- SQL queries to use with the prometheus_optimized_source
-- (after creating the source via the JSON API config)

-- Example 1: Simple aggregation over tumbling windows
CREATE TABLE prometheus_agg AS
SELECT
    instance,
    job,
    label1,
    label2,
    label3,
    SUM(value) as total_value,
    AVG(value) as avg_value,
    COUNT(*) as metric_count
FROM prometheus_optimized_source
WHERE metric_name = 'your_metric_name'
GROUP BY
    TUMBLE(INTERVAL '10' SECONDS),
    instance,
    job,
    label1,
    label2,
    label3;

-- Example 2: Filter by specific labels and aggregate
CREATE TABLE filtered_metrics AS
SELECT
    label1,
    label2,
    SUM(value) as sums,
    AVG(value) as avgs,
    MAX(value) as maxs,
    MIN(value) as mins
FROM prometheus_optimized_source
WHERE
    instance LIKE 'server%'
    AND job = 'my_job'
    AND metric_name = 'http_requests_total'
GROUP BY
    TUMBLE(INTERVAL '5' SECONDS),
    label1,
    label2;

-- Example 3: Real-time monitoring - detect high values
CREATE TABLE alerts AS
SELECT
    metric_name,
    instance,
    job,
    value,
    timestamp
FROM prometheus_optimized_source
WHERE value > 1000  -- Alert threshold
    AND metric_name = 'cpu_usage';

-- Example 4: Group by all label dimensions
SELECT
    instance,
    job,
    label1,
    label2,
    label3,
    COUNT(*) as count,
    SUM(value) as total
FROM prometheus_optimized_source
GROUP BY
    TUMBLE(INTERVAL '1' MINUTE),
    instance,
    job,
    label1,
    label2,
    label3;
