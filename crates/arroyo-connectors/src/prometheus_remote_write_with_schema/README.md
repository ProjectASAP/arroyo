# Prometheus Remote Write Connector

This connector enables Arroyo to receive metrics from Prometheus via the remote_write protocol. It acts as an HTTP server that accepts Prometheus remote_write requests and converts them into Arroyo stream records.

## Overview

The Prometheus Remote Write connector:
- Listens on a configurable HTTP endpoint for Prometheus remote_write requests
- Handles snappy-compressed protobuf data from Prometheus
- Extracts metric names, timestamps, values, and labels
- Converts data to Arrow RecordBatch format for stream processing
- Supports Arroyo's checkpointing mechanism for fault tolerance

## Configuration

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `port` | integer | 9090 | Port to listen on for remote_write requests |
| `path` | string | "/receive" | HTTP path to listen on |
| `bind_address` | string | "0.0.0.0" | Address to bind the HTTP server to |

All parameters are optional and will use defaults if not specified.

## Usage Examples

### 1. SQL CREATE CONNECTION

```sql
CREATE CONNECTION prometheus_source WITH (
  connector = 'prometheus_remote_write',
  port = '8080',
  path = '/api/v1/write',
  bind_address = '127.0.0.1'
);
```

### 2. Creating a Table

```sql
CREATE TABLE prometheus_metrics (
  metric_name TEXT,
  timestamp TIMESTAMP,
  value DOUBLE,
  labels TEXT
) WITH (
  connector = 'prometheus_remote_write',
  port = '8080', 
  path = '/metrics/write'
);
```

### 3. REST API Configuration

```json
{
  "connector": "prometheus_remote_write",
  "config": {
    "port": 8080,
    "path": "/api/v1/write", 
    "bind_address": "127.0.0.1"
  }
}
```

## Output Schema

The connector produces records with the following schema:

| Field | Type | Description |
|-------|------|-------------|
| `metric_name` | String | The metric name (extracted from `__name__` label) |
| `timestamp` | Timestamp | The metric timestamp in milliseconds |
| `value` | Float64 | The metric value |
| `labels` | String | JSON string containing all labels except `__name__` |

### Example Output

```json
{
  "metric_name": "cpu_usage_percent",
  "timestamp": "2023-12-07T10:30:00.000Z",
  "value": 45.2,
  "labels": "{\"instance\":\"localhost:9090\",\"job\":\"node-exporter\"}"
}
```

## Prometheus Configuration

Configure Prometheus to send metrics to Arroyo by adding a `remote_write` section to your `prometheus.yml`:

```yaml
# prometheus.yml
remote_write:
  - url: "http://your-arroyo-host:8080/api/v1/write"
    # Optional configuration:
    # basic_auth:
    #   username: user
    #   password: pass
    # tls_config:
    #   insecure_skip_verify: true
```

## Query Examples

Once data is flowing, you can query the metrics in real-time:

```sql
-- Get all CPU metrics
SELECT * FROM prometheus_metrics 
WHERE metric_name = 'cpu_usage_percent';

-- Calculate average CPU usage per instance over 1-minute windows
SELECT 
  JSON_EXTRACT(labels, '$.instance') as instance,
  AVG(value) as avg_cpu,
  TUMBLE_START(INTERVAL '1' MINUTE) as window_start
FROM prometheus_metrics 
WHERE metric_name = 'cpu_usage_percent'
GROUP BY 
  JSON_EXTRACT(labels, '$.instance'),
  TUMBLE(INTERVAL '1' MINUTE);

-- Alert on high CPU usage
SELECT *
FROM prometheus_metrics 
WHERE metric_name = 'cpu_usage_percent' 
  AND value > 80.0;
```

## Protocol Details

### HTTP Endpoint

- **Method**: POST
- **Content-Type**: `application/x-protobuf`
- **Content-Encoding**: `snappy`
- **Response**: 204 No Content (success) or appropriate error codes

### Data Processing

1. **Decompression**: Incoming snappy-compressed data is decompressed
2. **Protobuf Parsing**: Data is parsed as Prometheus `WriteRequest` protobuf
3. **Label Extraction**: The `__name__` label becomes `metric_name`, others go to `labels` JSON
4. **Timestamp Conversion**: Prometheus timestamps (milliseconds) are preserved
5. **Record Creation**: Data is converted to Arrow RecordBatch for stream processing

## Error Handling

The connector handles various error conditions:

- **Invalid HTTP method**: Returns 405 Method Not Allowed
- **Wrong path**: Returns 404 Not Found  
- **Decompression failure**: Returns 400 Bad Request
- **Invalid protobuf**: Returns 400 Bad Request
- **Server errors**: Returns 500 Internal Server Error

All errors are logged with appropriate detail levels.

## Performance Considerations

- The connector uses async I/O for handling multiple concurrent connections
- Metrics are batched and processed efficiently through Arrow RecordBatch
- Memory usage is managed through proper streaming and buffering
- Checkpointing ensures fault tolerance without significant performance impact

## Troubleshooting

### Common Issues

1. **Port already in use**: Ensure the configured port is available
2. **Connection refused**: Check firewall settings and bind address
3. **No data flowing**: Verify Prometheus remote_write configuration
4. **High memory usage**: Monitor batch sizes and processing rates

### Debugging

Enable debug logging to see detailed information about:
- Incoming HTTP requests
- Protobuf parsing
- Metric processing
- Batch creation and forwarding

```rust
// Example debug output
DEBUG Starting Prometheus remote_write server on 0.0.0.0:9090 with path /receive
DEBUG Processed 150 metrics
```