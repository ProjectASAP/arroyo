mod operator;

use anyhow::bail;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::FieldType::Primitive;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, PrimitiveType, TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;

use crate::prometheus_impulse::operator::{
    ImpulseSpec, PrometheusImpulseSourceFunc, PrometheusSpec,
};
use crate::{source_field, ConnectionType, EmptyConfig};

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./prometheus_impulse.svg");

// Simplified table structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusImpulseTable {
    pub event_rate: f64,
    pub event_time_interval: Option<i64>,
    pub message_count: Option<i64>,
    pub metric_name: String,
    pub metric_type: String,
    pub value_scale: f64,
    pub distribution: String,
    pub num_labels: u64,
    pub cardinality_per_label: String,
}

pub fn prometheus_impulse_schema() -> ConnectionSchema {
    ConnectionSchema {
        format: None,
        framing: None,
        bad_data: None,
        struct_name: Some("PrometheusMetric".to_string()),
        fields: vec![
            source_field("metric_name", Primitive(PrimitiveType::String)),
            source_field("metric_type", Primitive(PrimitiveType::String)),
            source_field("value", Primitive(PrimitiveType::F64)),
            source_field("labels", Primitive(PrimitiveType::String)),
        ],
        definition: None,
        inferred: None,
        primary_keys: Default::default(),
    }
}

fn compute_label_combinations(num_labels: usize, cardinality_per_label: &str) -> Vec<String> {
    if num_labels == 0 {
        return vec!["".to_string()];
    }

    // Parse cardinality specification
    let cardinalities: Vec<usize> = if cardinality_per_label.contains(',') {
        let parsed: Result<Vec<usize>, _> = cardinality_per_label
            .split(',')
            .map(|s| s.trim().parse::<usize>())
            .collect();
        match parsed {
            Ok(cards) => {
                if cards.len() != num_labels {
                    panic!(
                        "Number of cardinalities must match num_labels ({} vs {})",
                        cards.len(),
                        num_labels
                    );
                }
                cards
            }
            Err(_) => panic!(
                "Failed to parse cardinality_per_label: {}",
                cardinality_per_label
            ),
        }
    } else {
        let single_card: usize = cardinality_per_label
            .parse()
            .expect("Failed to parse single cardinality value");
        vec![single_card; num_labels]
    };

    // Generate label values for each label
    let mut label_values = Vec::with_capacity(num_labels);
    for label_idx in 0..num_labels {
        let cardinality = cardinalities[label_idx];
        let mut values = Vec::with_capacity(cardinality);
        for value_idx in 0..cardinality {
            values.push(format!("value_{}_value_{}", label_idx, value_idx));
        }
        label_values.push(values);
    }

    // Generate cartesian product of label values and serialize to strings
    let mut result = vec![Vec::new()];
    for label_pool in &label_values {
        let mut next_result = Vec::new();
        for existing in &result {
            for value in label_pool {
                let mut new_combination = existing.clone();
                new_combination.push(value.clone());
                next_result.push(new_combination);
            }
        }
        result = next_result;
    }

    // Convert to serialized label strings
    result
        .into_iter()
        .map(|combo| {
            if combo.is_empty() {
                String::new()
            } else {
                combo
                    .iter()
                    .enumerate()
                    .map(|(i, value)| format!("label_{}={}", i, value))
                    .collect::<Vec<_>>()
                    .join(",")
            }
        })
        .collect()
}

pub struct PrometheusImpulseConnector {}

impl Connector for PrometheusImpulseConnector {
    type ProfileT = EmptyConfig;
    type TableT = PrometheusImpulseTable;

    fn name(&self) -> &'static str {
        "prometheus_impulse"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "prometheus_impulse".to_string(),
            name: "Prometheus Impulse".to_string(),
            icon: ICON.to_string(),
            description: "Generates Prometheus metrics with configurable labels and cardinality"
                .to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: false,
            hidden: false,
            custom_schemas: false,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn get_schema(
        &self,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        Some(prometheus_impulse_schema())
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated Prometheus impulse connection".to_string(),
            };
            tx.send(message).await.unwrap();
        });
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let event_rate = options.pull_f64("event_rate")?;
        let event_time_interval = options.pull_opt_i64("event_time_interval")?;
        let message_count = options.pull_opt_i64("message_count")?;

        let metric_name = options
            .pull_opt_str("metric_name")?
            .map(|s| s.to_string())
            .unwrap_or_else(|| "fake_metric".to_string());
        let metric_type = options
            .pull_opt_str("metric_type")?
            .map(|s| s.to_string())
            .unwrap_or_else(|| "gauge".to_string());
        let value_scale = options.pull_opt_f64("value_scale")?.unwrap_or(100.0);
        let distribution = options
            .pull_opt_str("distribution")?
            .map(|s| s.to_string())
            .unwrap_or_else(|| "uniform".to_string());
        let num_labels = options.pull_opt_i64("num_labels")?.unwrap_or(2) as u64;
        let cardinality_per_label = options
            .pull_opt_str("cardinality_per_label")?
            .map(|s| s.to_string())
            .unwrap_or_else(|| "3".to_string());

        // validate the schema
        if let Some(s) = schema {
            if !s.fields.is_empty() && s.fields != prometheus_impulse_schema().fields {
                bail!("invalid schema for prometheus impulse source");
            }
        }

        self.from_config(
            None,
            name,
            EmptyConfig {},
            PrometheusImpulseTable {
                event_rate,
                event_time_interval,
                message_count,
                metric_name,
                metric_type,
                value_scale,
                distribution,
                num_labels,
                cardinality_per_label,
            },
            None,
        )
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let description = format!(
            "PrometheusImpulse<{} eps, {} {}, {} labels>",
            table.event_rate, table.metric_name, table.metric_type, table.num_labels
        );

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: None,
            bad_data: None,
            framing: None,
            metadata_fields: vec![],
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Source,
            prometheus_impulse_schema(),
            &config,
            description,
        ))
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        _: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        let label_combinations =
            compute_label_combinations(table.num_labels as usize, &table.cardinality_per_label);

        let prometheus_spec = PrometheusSpec {
            metric_name: Arc::from(table.metric_name.as_str()),
            metric_type: Arc::from(table.metric_type.as_str()),
            value_scale: table.value_scale,
            label_combinations,
        };

        Ok(ConstructedOperator::from_source(Box::new(
            PrometheusImpulseSourceFunc::new(
                table
                    .event_time_interval
                    .map(|i| std::time::Duration::from_nanos(i as u64)),
                ImpulseSpec::EventsPerSecond(table.event_rate as f32),
                table
                    .message_count
                    .map(|n| n as usize)
                    .unwrap_or(usize::MAX),
                SystemTime::now(),
                prometheus_spec,
            ),
        )))
    }
}
