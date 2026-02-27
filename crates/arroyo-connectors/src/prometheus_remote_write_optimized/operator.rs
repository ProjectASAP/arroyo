use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::SignalMessage;

// Include generated protobuf code
include!(concat!(env!("OUT_DIR"), "/prometheus_proto.rs"));

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct PrometheusRemoteWriteOptimizedState {
    pub messages_received: u64,
}

pub struct PrometheusRemoteWriteOptimizedSourceFunc {
    bind_address: String,
    port: u16,
    path: String,
    state: PrometheusRemoteWriteOptimizedState,

    // Union of all labels across all metrics
    all_labels: Vec<String>,

    // Metrics to filter for (only emit these metrics)
    metric_filter: HashSet<String>,

    // Map from metric name to its specific labels (for debugging/validation)
    metric_label_map: HashMap<String, Vec<String>>,
}

impl PrometheusRemoteWriteOptimizedSourceFunc {
    pub fn new(
        bind_address: String,
        port: u16,
        path: String,
        all_labels: Vec<String>,
        metric_filter: HashSet<String>,
        metric_label_map: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            bind_address,
            port,
            path,
            state: PrometheusRemoteWriteOptimizedState::default(),
            all_labels,
            metric_filter,
            metric_label_map,
        }
    }

    pub fn create_schema(label_names: &[String]) -> Arc<Schema> {
        let mut fields = vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ];

        // Add one column per label
        for label_name in label_names {
            fields.push(Field::new(label_name, DataType::Utf8, true));
        }

        fields.push(Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

        Arc::new(Schema::new(fields))
    }

    async fn handle_request(
        req: Request<Incoming>,
        tx: mpsc::UnboundedSender<prometheus::WriteRequest>,
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

        // Send WriteRequest directly - no conversion, no clones!
        if !write_request.timeseries.is_empty() {
            if let Err(e) = tx.send(write_request) {
                error!("Failed to send write request to processor: {}", e);
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
        tx: mpsc::UnboundedSender<prometheus::WriteRequest>,
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

#[async_trait]
impl SourceOperator for PrometheusRemoteWriteOptimizedSourceFunc {
    fn name(&self) -> String {
        "PrometheusRemoteWriteOptimizedSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("s", "prometheus remote write optimized source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) {
        let s: &mut GlobalKeyedView<(), PrometheusRemoteWriteOptimizedState> = ctx
            .table_manager
            .get_global_keyed_state("s")
            .await
            .expect("should be able to read prometheus remote write optimized state");

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

impl PrometheusRemoteWriteOptimizedSourceFunc {
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
            "Starting Prometheus remote_write optimized server on {} with path {} filtering {} metrics (union labels: {:?})",
            addr, self.path, self.metric_filter.len(), self.all_labels
        );

        let listener = match TcpListener::bind(addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to {}: {}", addr, e);
                return SourceFinishType::Immediate;
            }
        };

        let (tx, mut rx) = mpsc::unbounded_channel::<prometheus::WriteRequest>();
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
        let all_labels = self.all_labels.clone();
        let metric_filter = self.metric_filter.clone();

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
                                .expect("should be able to get prometheus remote write optimized state");
                            s.insert((), state).await;

                            if self.start_checkpoint(c, ctx, collector).await {
                                return SourceFinishType::Immediate;
                            }
                        }
                        Some(arroyo_rpc::ControlMessage::Stop { mode }) => {
                            info!("Stopping prometheus remote write optimized source");
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

                // Handle incoming write requests - zero-copy processing WITH FILTERING
                write_request = rx.recv() => {
                    if let Some(write_request) = write_request {
                        // Filter timeseries by configured metrics
                        let filtered_timeseries: Vec<_> = write_request.timeseries.iter()
                            .filter(|ts| {
                                // Extract metric name from __name__ label
                                let metric_name = ts.labels.iter()
                                    .find(|l| l.name == "__name__")
                                    .map(|l| l.value.as_str())
                                    .unwrap_or("");

                                // Only include if in filter set
                                metric_filter.contains(metric_name)
                            })
                            .collect();

                        if filtered_timeseries.is_empty() {
                            continue;
                        }

                        // Calculate total samples across filtered timeseries
                        let total_samples: usize = filtered_timeseries.iter()
                            .map(|ts| ts.samples.len())
                            .sum();

                        if total_samples == 0 {
                            continue;
                        }

                        self.state.messages_received += total_samples as u64;

                        // Pre-allocate vectors with exact capacity
                        let mut metric_names = Vec::with_capacity(total_samples);
                        let mut timestamps = Vec::with_capacity(total_samples);
                        let mut values = Vec::with_capacity(total_samples);

                        let mut label_columns: Vec<Vec<Option<&str>>> =
                            vec![Vec::with_capacity(total_samples); all_labels.len()];

                        // Single pass: iterate filtered timeseries and samples (zero clones!)
                        for timeseries in filtered_timeseries {
                            // Extract metric name and labels ONCE per timeseries
                            let mut metric_name = "";
                            let mut labels_map: HashMap<&str, &str> = HashMap::with_capacity(timeseries.labels.len());

                            for label in &timeseries.labels {
                                if label.name == "__name__" {
                                    metric_name = &label.value;
                                } else {
                                    labels_map.insert(&label.name, &label.value);
                                }
                            }

                            // Process all samples in this timeseries - NO CLONES!
                            for sample in &timeseries.samples {
                                metric_names.push(metric_name);  // Just a reference
                                timestamps.push(sample.timestamp);
                                values.push(sample.value);

                                // Extract label values by looking up in the map
                                // Use all_labels (union) instead of per-metric labels
                                for (idx, label_name) in all_labels.iter().enumerate() {
                                    label_columns[idx].push(labels_map.get(label_name.as_str()).copied());
                                }
                            }
                        }

                        // Create _timestamp field by converting Prometheus timestamps from ms to ns
                        let event_timestamps: Vec<i64> = timestamps.iter()
                            .map(|&ts_ms| ts_ms * 1_000_000)
                            .collect();

                        // Build arrays (StringArray copies strings here, but only once)
                        let mut arrays: Vec<ArrayRef> = vec![
                            Arc::new(StringArray::from(metric_names)),
                            Arc::new(TimestampMillisecondArray::from(timestamps)),
                            Arc::new(Float64Array::from(values)),
                        ];

                        // Add label columns
                        for label_values in label_columns.iter() {
                            arrays.push(Arc::new(StringArray::from(label_values.clone())));
                        }

                        arrays.push(Arc::new(TimestampNanosecondArray::from(event_timestamps)));

                        let batch = match RecordBatch::try_new(schema.clone(), arrays) {
                            Ok(b) => b,
                            Err(e) => {
                                error!("Failed to create record batch: {}", e);
                                error!("Schema: {:?}", schema);
                                error!("Total samples: {}", total_samples);
                                continue;
                            }
                        };

                        collector.collect(batch).await;
                    }
                }
            }
        }
    }
}
