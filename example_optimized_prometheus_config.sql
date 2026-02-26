-- Example configuration for Prometheus Remote Write Optimized Source
-- This is for your workload: 80K unique time series with 5 labels

-- Create the optimized Prometheus source table
CREATE TABLE prometheus_metrics (
    -- Fixed columns (always present)
    metric_name TEXT,
    timestamp TIMESTAMP,  -- Prometheus timestamp in milliseconds
    value DOUBLE,         -- Metric value

    -- Label columns (flattened for performance)
    -- ORDER MATTERS: Must match the order in label_names array
    instance TEXT,
    job TEXT,
    label1 TEXT,
    label2 TEXT,
    label3 TEXT,

    -- System timestamp (ingestion time in nanoseconds)
    _timestamp TIMESTAMP
) WITH (
    connector = 'prometheus_remote_write_optimized',
    base_port = '9090',
    path = '/receive',
    bind_address = '0.0.0.0',

    -- CRITICAL: This defines which labels to extract as columns
    -- Must match the order of label columns above
    label_names = '["instance", "job", "label1", "label2", "label3"]'
);

-- Example queries you can run

-- 1. Get all metrics for a specific instance
SELECT * FROM prometheus_metrics
WHERE instance = 'my-instance-name'
LIMIT 100;

-- 2. Aggregate by instance over 1-minute windows
SELECT
    instance,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(*) as count,
    TUMBLE_START(INTERVAL '1' MINUTE) as window_start
FROM prometheus_metrics
WHERE metric_name = 'your_metric_name'
GROUP BY
    instance,
    TUMBLE(INTERVAL '1' MINUTE);

-- 3. Filter by multiple labels (very fast with flattened schema)
SELECT
    instance,
    label1,
    label2,
    value,
    timestamp
FROM prometheus_metrics
WHERE label1 = 'some_value'
  AND label2 = 'other_value'
  AND instance LIKE 'server%';

-- 4. Real-time aggregation by all label dimensions
SELECT
    label1,
    label2,
    label3,
    COUNT(*) as metric_count,
    AVG(value) as avg_value,
    TUMBLE_START(INTERVAL '10' SECOND) as window_start
FROM prometheus_metrics
GROUP BY
    label1,
    label2,
    label3,
    TUMBLE(INTERVAL '10' SECOND);
