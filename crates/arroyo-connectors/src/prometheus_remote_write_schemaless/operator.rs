use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use arrow::array::{Float64Array, RecordBatch, StringArray, TimestampMillisecondArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use http_body_util::{BodyExt, Full};
use bytes::Bytes;
use prost::Message;
use serde_json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{SignalMessage, to_nanos};

// Include generated protobuf code
include!(concat!(env!("OUT_DIR"), "/prometheus_proto.rs"));

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct PrometheusRemoteWriteSchemalessState {
    pub messages_received: u64,
}

pub struct PrometheusRemoteWriteSchemalessSourceFunc {
    bind_address: String,
    port: u16,
    path: String,
    state: PrometheusRemoteWriteSchemalessState,
}

impl PrometheusRemoteWriteSchemalessSourceFunc {
    pub fn new(bind_address: String, port: u16, path: String) -> Self {
        Self {
            bind_address,
            port,
            path,
            state: PrometheusRemoteWriteSchemalessState::default(),
        }
    }

    #[allow(dead_code)]
    fn create_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
            Field::new("labels", DataType::Utf8, false),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]))
    }

    async fn handle_request(
        req: Request<Incoming>,
        tx: mpsc::UnboundedSender<Vec<PrometheusMetric>>,
        path: String,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        if req.method() != Method::POST {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(Bytes::from("Only POST method is allowed")))
                .unwrap());
        }

        if req.uri().path() != path {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("Not found")))
                .unwrap());
        }

        // Check content type
        let content_type = req
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok());

        if content_type != Some("application/x-protobuf") {
            warn!("Unexpected content-type: {:?}", content_type);
        }

        // Check content encoding
        let content_encoding = req
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok());

        if content_encoding != Some("snappy") {
            warn!("Unexpected content-encoding: {:?}", content_encoding);
        }

        // Read the request body
        let body = req.collect().await?.to_bytes();

        // Decompress snappy data
        let decompressed = match snap::raw::Decoder::new().decompress_vec(&body) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to decompress snappy data: {}", e);
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Failed to decompress data")))
                    .unwrap());
            }
        };

        // Parse protobuf WriteRequest
        let write_request = match prometheus::WriteRequest::decode(&decompressed[..]) {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to parse protobuf WriteRequest: {}", e);
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Failed to parse protobuf")))
                    .unwrap());
            }
        };

        // Convert to our internal format
        let mut metrics = Vec::new();
        for timeseries in write_request.timeseries {
            let mut metric_name = String::new();
            let mut labels_map = HashMap::new();

            // Extract labels
            for label in timeseries.labels {
                if label.name == "__name__" {
                    metric_name = label.value;
                } else {
                    labels_map.insert(label.name, label.value);
                }
            }

            // Convert labels to JSON string
            let labels_json = serde_json::to_string(&labels_map).unwrap_or_default();

            // Process samples
            for sample in timeseries.samples {
                metrics.push(PrometheusMetric {
                    metric_name: metric_name.clone(),
                    timestamp: sample.timestamp,
                    value: sample.value,
                    labels: labels_json.clone(),
                });
            }
        }

        // Send metrics to processing channel
        if !metrics.is_empty() {
            if let Err(e) = tx.send(metrics) {
                error!("Failed to send metrics to processor: {}", e);
            }
        }

        // Return 204 No Content (successful ingestion)
        Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap())
    }

    async fn handle_connection(
        stream: TcpStream,
        tx: mpsc::UnboundedSender<Vec<PrometheusMetric>>,
        path: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let io = TokioIo::new(stream);
        let path_clone = path.clone();

        let service = service_fn(move |req| {
            Self::handle_request(req, tx.clone(), path_clone.clone())
        });

        if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
            error!("Error serving HTTP connection: {:?}", err);
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PrometheusMetric {
    pub metric_name: String,
    pub timestamp: i64,
    pub value: f64,
    pub labels: String,
}

#[async_trait]
impl SourceOperator for PrometheusRemoteWriteSchemalessSourceFunc {
    fn name(&self) -> String {
        "PrometheusRemoteWriteSchemalessSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("s", "prometheus remote write schemaless source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) {
        let s: &mut GlobalKeyedView<(), PrometheusRemoteWriteSchemalessState> = ctx
            .table_manager
            .get_global_keyed_state("s")
            .await
            .expect("should be able to read prometheus remote write schemaless state");

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
        }
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        self.run_int(ctx, collector).await
    }
}

impl PrometheusRemoteWriteSchemalessSourceFunc {
    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        let addr: SocketAddr = match format!("{}:{}", self.bind_address, self.port).parse() {
            Ok(addr) => addr,
            Err(e) => {
                error!("Invalid bind address: {}", e);
                return SourceFinishType::Immediate;
            }
        };

        info!(
            "Starting Prometheus remote_write schemaless server on {} with path {}",
            addr, self.path
        );

        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to {}: {}", addr, e);
                return SourceFinishType::Immediate;
            }
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<PrometheusMetric>>();
        let path = self.path.clone();

        // Spawn the HTTP server
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let tx_clone = tx.clone();
                        let path_clone = path.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                Self::handle_connection(stream, tx_clone, path_clone).await
                            {
                                error!("Connection handling error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
        });

        let schema = ctx.out_schema.schema.clone();

        // Main processing loop
        loop {
            tokio::select! {
                // Handle control messages
                msg = ctx.control_rx.recv() => {
                    match msg {
                        Some(arroyo_rpc::ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let state = self.state.clone();
                            let s = ctx
                                .table_manager
                                .get_global_keyed_state("s")
                                .await
                                .expect("should be able to get prometheus remote write schemaless state");
                            s.insert((), state).await;

                            if self.start_checkpoint(c, ctx, collector).await {
                                return SourceFinishType::Immediate;
                            }
                        }
                        Some(arroyo_rpc::ControlMessage::Stop { mode }) => {
                            info!("Stopping prometheus remote write schemaless source");
                            return match mode {
                                StopMode::Graceful => SourceFinishType::Graceful,
                                StopMode::Immediate => SourceFinishType::Immediate,
                            };
                        }
                        Some(arroyo_rpc::ControlMessage::Commit { epoch: _, commit_data: _ }) => {
                            collector.broadcast(SignalMessage::EndOfData).await;
                        }
                        Some(arroyo_rpc::ControlMessage::LoadCompacted { compacted }) => {
                            ctx.load_compacted(compacted).await;
                        }
                        Some(arroyo_rpc::ControlMessage::NoOp) => {
                            // No operation needed
                        }
                        None => {
                            return SourceFinishType::Final;
                        }
                    }
                },

                // Handle incoming metrics
                metrics = rx.recv() => {
                    if let Some(metrics) = metrics {
                        if !metrics.is_empty() {
                            self.state.messages_received += metrics.len() as u64;

                            // Convert to Arrow RecordBatch
                            let len = metrics.len();
                            let metric_names: Vec<String> = metrics.iter().map(|m| m.metric_name.clone()).collect();
                            let timestamps: Vec<i64> = metrics.iter().map(|m| m.timestamp).collect(); // Prometheus timestamps are already in ms
                            let values: Vec<f64> = metrics.iter().map(|m| m.value).collect();
                            let labels: Vec<String> = metrics.iter().map(|m| m.labels.clone()).collect();
                            
                            // Create _timestamp field with current system time in nanoseconds
                            let now = SystemTime::now();
                            let event_timestamps: Vec<i64> = vec![to_nanos(now) as i64; len];

                            let batch = RecordBatch::try_new(
                                schema.clone(),
                                vec![
                                    Arc::new(StringArray::from(metric_names)),
                                    Arc::new(TimestampMillisecondArray::from(timestamps)),
                                    Arc::new(Float64Array::from(values)),
                                    Arc::new(StringArray::from(labels)),
                                    Arc::new(TimestampNanosecondArray::from(event_timestamps)),
                                ],
                            );

                            let batch = match batch {
                                Ok(b) => b,
                                Err(e) => {
                                    error!("Failed to create record batch: {}", e);
                                    continue;
                                }
                            };

                            collector.collect(batch).await;

                            debug!("Processed {} metrics", len);
                        }
                    }
                }
            }
        }
    }
}