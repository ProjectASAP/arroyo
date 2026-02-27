use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::SystemTime;

use async_trait::async_trait;
use bincode::{Decode, Encode};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prost::Message;
use serde_json;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::SignalMessage;

// Include generated protobuf code
include!(concat!(env!("OUT_DIR"), "/prometheus_proto.rs"));

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct PrometheusRemoteWriteWithSchemaState {
    pub messages_received: u64,
}

pub struct PrometheusRemoteWriteWithSchemaSourceFunc {
    bind_address: String,
    base_port: u16,
    path: String,
    state: PrometheusRemoteWriteWithSchemaState,
    format: Format,
    framing: Option<Framing>,
    bad_data: Option<BadData>,
}

impl PrometheusRemoteWriteWithSchemaSourceFunc {
    pub fn new(
        bind_address: String,
        base_port: u16,
        path: String,
        format: Format,
        framing: Option<Framing>,
        bad_data: Option<BadData>,
    ) -> Self {
        Self {
            bind_address,
            base_port,
            path,
            state: PrometheusRemoteWriteWithSchemaState::default(),
            format,
            framing,
            bad_data,
        }
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

            // Process samples
            for sample in timeseries.samples {
                metrics.push(PrometheusMetric {
                    metric_name: metric_name.clone(),
                    timestamp: sample.timestamp,
                    value: sample.value,
                    labels: labels_map.clone(),
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

        let service =
            service_fn(move |req| Self::handle_request(req, tx.clone(), path_clone.clone()));

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
    pub labels: HashMap<String, String>,
}

#[async_trait]
impl SourceOperator for PrometheusRemoteWriteWithSchemaSourceFunc {
    fn name(&self) -> String {
        "PrometheusRemoteWriteWithSchemaSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("s", "prometheus remote write with schema source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) {
        let s: &mut GlobalKeyedView<(), PrometheusRemoteWriteWithSchemaState> = ctx
            .table_manager
            .get_global_keyed_state("s")
            .await
            .expect("should be able to read prometheus remote write with schema state");

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

impl PrometheusRemoteWriteWithSchemaSourceFunc {
    fn metric_to_json(&self, metric: &PrometheusMetric) -> Result<String, serde_json::Error> {
        // Use timestamp as milliseconds integer to match JSON schema
        let timestamp_ms = metric.timestamp as u64;

        // Convert ms to ns for internal timestamp field
        let timestamp_ns = timestamp_ms * 1_000_000;

        // Debug: Log the actual labels received and value status
        debug!("Processing metric '{}' with labels: {:?}, timestamp={}, value={}, is_finite={}, is_nan={}",
               metric.metric_name, metric.labels, metric.timestamp, metric.value,
               metric.value.is_finite(), metric.value.is_nan());

        // Warn about problematic values
        if metric.value.is_nan() {
            warn!(
                "Metric '{}' has NaN value! timestamp={}, labels={:?}",
                metric.metric_name, metric.timestamp, metric.labels
            );
        }
        if metric.value.is_infinite() {
            warn!(
                "Metric '{}' has infinite value: {} timestamp={}, labels={:?}",
                metric.metric_name, metric.value, metric.timestamp, metric.labels
            );
        }

        let json_value = serde_json::json!({
            "labels": metric.labels,
            "name": metric.metric_name,
            "timestamp": timestamp_ms,
            "value": metric.value,
            "_timestamp": timestamp_ns
        });

        let json_str = serde_json::to_string(&json_value)?;

        // Debug: Log the generated JSON
        debug!("Generated JSON: {}", json_str);

        // Check if NaN was serialized as null in the JSON (this causes deserialization errors!)
        if metric.value.is_nan() && json_str.contains("\"value\":null") {
            error!("CRITICAL: NaN value was serialized as null in JSON! This will cause deserialization to fail.");
            error!(
                "Metric: {}, timestamp: {}, labels: {:?}",
                metric.metric_name, metric.timestamp, metric.labels
            );
            error!("JSON contains: {}", json_str);
        }

        Ok(json_str)
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        // Calculate actual port based on task index
        let task_index = ctx.task_info.task_index as u16;
        let actual_port = self.base_port + task_index;

        let addr: SocketAddr = match format!("{}:{}", self.bind_address, actual_port).parse() {
            Ok(addr) => addr,
            Err(e) => {
                error!("Invalid bind address: {}", e);
                return SourceFinishType::Immediate;
            }
        };

        info!(
            "Starting Prometheus remote_write with schema server on {} with path {} (task_index: {})",
            addr, self.path, task_index
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

        // Initialize deserializer like Kafka does
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );

        // Debug: Log schema information from out_schema
        debug!(
            "Prometheus source initialized with schema: {:?}",
            collector.out_schema.schema
        );

        // Check if labels field is structured
        if let Ok(labels_field) = collector.out_schema.schema.field_with_name("labels") {
            debug!(
                "Labels field definition: name={}, type={:?}, nullable={}",
                labels_field.name(),
                labels_field.data_type(),
                labels_field.is_nullable()
            );
        } else {
            debug!("No 'labels' field found in schema");
        }

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
                                .expect("should be able to get prometheus remote write with schema state");
                            s.insert((), state).await;

                            if self.start_checkpoint(c, ctx, collector).await {
                                return SourceFinishType::Immediate;
                            }
                        }
                        Some(arroyo_rpc::ControlMessage::Stop { mode }) => {
                            info!("Stopping prometheus remote write with schema source");
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

                            // Convert each metric to JSON and deserialize like Kafka
                            debug!("Processing batch of {} metrics", metrics.len());
                            for (i, metric) in metrics.iter().enumerate() {
                                match self.metric_to_json(metric) {
                                    Ok(json_str) => {
                                        // Debug: Log JSON before deserialization
                                        debug!("Deserializing metric {}/{}: {}", i+1, metrics.len(), json_str);

                                        // Use deserializer like Kafka does
                                        match collector.deserialize_slice(
                                            json_str.as_bytes(),
                                            SystemTime::now(),
                                            None
                                        ).await {
                                            Ok(_) => {
                                                debug!("Successfully deserialized metric {}/{}", i+1, metrics.len());
                                            }
                                            Err(e) => {
                                                error!("===== DESERIALIZATION ERROR =====");
                                                error!("Failed to deserialize metric {}/{}", i+1, metrics.len());
                                                error!("Metric name: {}", metric.metric_name);
                                                error!("Metric timestamp: {}", metric.timestamp);
                                                error!("Metric value: {}", metric.value);
                                                error!("Metric value is_nan: {}", metric.value.is_nan());
                                                error!("Metric value is_infinite: {}", metric.value.is_infinite());
                                                error!("Metric value is_finite: {}", metric.value.is_finite());
                                                error!("Metric labels: {:?}", metric.labels);
                                                error!("Generated JSON: {}", json_str);
                                                error!("Deserialization error: {:?}", e);
                                                error!("================================");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to convert metric {} to JSON: {}", metric.metric_name, e);
                                        error!("Metric details: timestamp={}, value={}, is_nan={}, labels={:?}",
                                               metric.timestamp, metric.value, metric.value.is_nan(), metric.labels);
                                    }
                                }
                            }

                            // Flush buffer if needed, like Kafka does
                            if collector.should_flush() {
                                debug!("Flushing buffer for {} metrics", metrics.len());
                                if let Err(e) = collector.flush_buffer().await {
                                    error!("Failed to flush buffer after processing {} metrics: {:?}", metrics.len(), e);
                                    error!("Last batch metrics: {:?}", metrics.iter().map(|m| {
                                        format!("name={}, value={}, is_nan={}, labels={:?}",
                                                m.metric_name, m.value, m.value.is_nan(), m.labels)
                                    }).collect::<Vec<_>>());
                                }
                            }

                            debug!("Processed {} metrics via deserializer", metrics.len());
                        }
                    }
                }
            }
        }
    }
}
