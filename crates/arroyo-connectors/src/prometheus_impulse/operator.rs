use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::builder::{StringBuilder, Float64Builder, TimestampNanosecondBuilder};
use arrow::array::RecordBatch;
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::ControlMessage;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use rand::{SeedableRng, Rng};
use rand::rngs::SmallRng;

use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_types::{to_nanos};
use tracing::{debug, info};

const RNG_SEED: u64 = 0;

thread_local! {
    static RNG: std::cell::RefCell<SmallRng> = std::cell::RefCell::new(SmallRng::seed_from_u64(RNG_SEED));
}

#[derive(Encode, Decode, Debug, Clone, PartialEq)]
pub struct PrometheusImpulseSourceState {
    pub counter: usize,
    pub start_time: SystemTime,
    pub counter_value: f64, // For counter type metrics
    pub total_samples: u64, // For dynamic distribution
}

#[derive(Debug, Clone)]
pub struct PrometheusSpec {
    pub metric_name: Arc<str>,
    pub metric_type: Arc<str>, // "gauge" or "counter"
    pub value_scale: f64,
    pub label_combinations: Vec<String>, // Pre-serialized label strings
}

#[derive(Debug, Clone, Copy)]
pub enum ImpulseSpec {
    EventsPerSecond(f32),
}

#[derive(Debug)]
pub struct PrometheusImpulseSourceFunc {
    #[allow(dead_code)]
    pub interval: Option<Duration>,
    pub spec: ImpulseSpec,
    pub limit: usize,
    pub state: PrometheusImpulseSourceState,
    pub prometheus_spec: PrometheusSpec,
}

impl PrometheusImpulseSourceFunc {
    pub fn new(
        interval: Option<Duration>,
        spec: ImpulseSpec,
        limit: usize,
        start_time: SystemTime,
        prometheus_spec: PrometheusSpec,
    ) -> Self {
        // Only support uniform distribution for simplicity

        Self {
            interval,
            spec,
            limit,
            state: PrometheusImpulseSourceState {
                counter: 0,
                start_time,
                counter_value: 0.0,
                total_samples: 0,
            },
            prometheus_spec,
        }
    }

    fn get_sample(&self, _samples_count: u64) -> f64 {
        RNG.with(|rng| {
            let mut rng = rng.borrow_mut();
            // Simple uniform random between 0 and value_scale
            rng.random::<f64>() * self.prometheus_spec.value_scale
        })
    }

    fn get_counter_value(&mut self) -> f64 {
        let sample = self.get_sample(self.state.total_samples);
        self.state.counter_value += sample;
        self.state.counter_value
    }

    fn get_gauge_value(&self) -> f64 {
        self.get_sample(self.state.total_samples)
    }

    fn generate_prometheus_record(&mut self, _labels: &str) -> (f64, i64) {
        let value = if self.prometheus_spec.metric_type.as_ref() == "counter" {
            self.get_counter_value()
        } else {
            self.get_gauge_value()
        };

        self.state.total_samples += 1;

        (value, to_nanos(SystemTime::now()) as i64)
    }

    fn batch_size(&self, ctx: &mut SourceContext) -> usize {
        let duration_micros = self.delay(ctx).as_micros();
        if duration_micros == 0 {
            return 8192;
        }
        let batch_size = Duration::from_millis(100).as_micros() / duration_micros;
        batch_size.clamp(1, 8192) as usize
    }

    fn delay(&self, ctx: &mut SourceContext) -> Duration {
        match self.spec {
            ImpulseSpec::EventsPerSecond(eps) => {
                Duration::from_secs_f32(1.0 / (eps / ctx.task_info.parallelism as f32))
            }
        }
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        let delay = self.delay(ctx);
        info!(
            "Starting prometheus impulse source with delay {:?} and limit {}",
            delay,
            self.limit
        );

        if let Some(state) = ctx
            .table_manager
            .get_global_keyed_state::<u32, PrometheusImpulseSourceState>("p")
            .await
            .unwrap()
            .get(&ctx.task_info.task_index)
        {
            self.state = state.clone();
            info!("Restored state: {:?}", self.state);
        }

        let start_time = SystemTime::now() - delay * self.state.counter as u32;
        let schema = ctx.out_schema.schema.clone();
        let batch_size = self.batch_size(ctx);

        let mut items = 0;
        let metric_name = self.prometheus_spec.metric_name.clone();
        let metric_type = self.prometheus_spec.metric_type.clone();
        let mut metric_name_builder = StringBuilder::with_capacity(batch_size, batch_size * metric_name.len());
        let mut metric_type_builder = StringBuilder::with_capacity(batch_size, batch_size * metric_type.len());
        let mut value_builder = Float64Builder::with_capacity(batch_size);
        let mut labels_builder = StringBuilder::with_capacity(batch_size, batch_size * 100);
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(batch_size);

        let label_combinations = self.prometheus_spec.label_combinations.clone();
        let mut combo_idx = 0;

        while self.state.counter < self.limit {
            // Cycle through label combinations
            let labels = &label_combinations[combo_idx % label_combinations.len()];
            let (value, timestamp) = self.generate_prometheus_record(labels);

            metric_name_builder.append_value(&metric_name);
            metric_type_builder.append_value(&metric_type);
            value_builder.append_value(value);
            labels_builder.append_value(labels);
            timestamp_builder.append_value(timestamp);

            items += 1;
            combo_idx += 1;

            if items == batch_size {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(metric_name_builder.finish()),
                        Arc::new(metric_type_builder.finish()),
                        Arc::new(value_builder.finish()),
                        Arc::new(labels_builder.finish()),
                        Arc::new(timestamp_builder.finish()),
                    ],
                ).unwrap();

                collector.collect(batch).await;
                items = 0;
                // Rebuild builders for next batch
                metric_name_builder = StringBuilder::with_capacity(batch_size, batch_size * metric_name.len());
                metric_type_builder = StringBuilder::with_capacity(batch_size, batch_size * metric_type.len());
                value_builder = Float64Builder::with_capacity(batch_size);
                labels_builder = StringBuilder::with_capacity(batch_size, batch_size * 100);
                timestamp_builder = TimestampNanosecondBuilder::with_capacity(batch_size);
            }

            self.state.counter += 1;

            // Handle control messages
            match ctx.control_rx.try_recv() {
                Ok(ControlMessage::Checkpoint(c)) => {
                    debug!("Starting checkpointing {}", ctx.task_info.task_index);
                    if items > 0 {
                        let batch = RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                Arc::new(metric_name_builder.finish()),
                                Arc::new(metric_type_builder.finish()),
                                Arc::new(value_builder.finish()),
                                Arc::new(labels_builder.finish()),
                                Arc::new(timestamp_builder.finish()),
                            ],
                        ).unwrap();
                        collector.collect(batch).await;
                        items = 0;
                        // Rebuild builders for next batch
                        metric_name_builder = StringBuilder::with_capacity(batch_size, batch_size * metric_name.len());
                        metric_type_builder = StringBuilder::with_capacity(batch_size, batch_size * metric_type.len());
                        value_builder = Float64Builder::with_capacity(batch_size);
                        labels_builder = StringBuilder::with_capacity(batch_size, batch_size * 100);
                        timestamp_builder = TimestampNanosecondBuilder::with_capacity(batch_size);
                    }
                    ctx.table_manager
                        .get_global_keyed_state::<u32, PrometheusImpulseSourceState>("p")
                        .await
                        .unwrap()
                        .insert(ctx.task_info.task_index, self.state.clone())
                        .await;
                    if self.start_checkpoint(c, ctx, collector).await {
                        return SourceFinishType::Immediate;
                    }
                }
                Ok(ControlMessage::Stop { mode }) => {
                    info!("Stopping prometheus impulse source {:?}", mode);
                    match mode {
                        StopMode::Graceful => return SourceFinishType::Graceful,
                        StopMode::Immediate => return SourceFinishType::Immediate,
                    }
                }
                Ok(ControlMessage::Commit { .. }) => {
                    unreachable!("sources shouldn't receive commit messages");
                }
                Ok(ControlMessage::LoadCompacted { compacted }) => {
                    ctx.table_manager.load_compacted(&compacted).await.unwrap();
                }
                Ok(ControlMessage::NoOp) => {}
                Err(_) => {
                    // no messages
                }
            }

            if !delay.is_zero() {
                let next_sleep = start_time + delay * self.state.counter as u32;
                if let Ok(sleep_time) = next_sleep.duration_since(SystemTime::now()) {
                    tokio::time::sleep(sleep_time).await;
                }
            }
        }

        if items > 0 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(metric_name_builder.finish()),
                    Arc::new(metric_type_builder.finish()),
                    Arc::new(value_builder.finish()),
                    Arc::new(labels_builder.finish()),
                    Arc::new(timestamp_builder.finish()),
                ],
            ).unwrap();
            collector.collect(batch).await;
        }

        SourceFinishType::Final
    }
}

#[async_trait]
impl SourceOperator for PrometheusImpulseSourceFunc {
    fn name(&self) -> String {
        "prometheus-impulse-source".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("p", "prometheus impulse source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) {
        let s: &mut arroyo_state::tables::global_keyed_map::GlobalKeyedView<u32, PrometheusImpulseSourceState> = ctx
            .table_manager
            .get_global_keyed_state("p")
            .await
            .expect("should have table p in prometheus impulse source");

        if let Some(state) = s.get(&ctx.task_info.task_index) {
            self.state = state.clone();
        }
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        self.run(ctx, collector).await
    }
}
