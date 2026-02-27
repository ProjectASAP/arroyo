mod operator;

use std::collections::HashSet;

use crate::{source_field, EmptyConfig};
use anyhow::anyhow;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, FieldType::Primitive, PrimitiveType,
    TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use operator::PrometheusRemoteWriteOptimizedSourceFunc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./prometheus_remote_write.svg");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricConfig {
    pub name: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusRemoteWriteOptimizedTable {
    pub base_port: Option<u16>,
    pub path: Option<String>,
    pub bind_address: Option<String>,
    pub metrics: Vec<MetricConfig>,
}

pub struct PrometheusRemoteWriteOptimizedConnector {}

impl PrometheusRemoteWriteOptimizedConnector {
    fn collect_all_labels(config: &PrometheusRemoteWriteOptimizedTable) -> Vec<String> {
        use std::collections::BTreeSet;

        let mut all_labels = BTreeSet::new();

        // Collect union of all labels from all metrics
        for metric in &config.metrics {
            for label in &metric.labels {
                all_labels.insert(label.clone());
            }
        }

        // Return sorted for deterministic schema
        all_labels.into_iter().collect()
    }

    fn get_metric_filter(config: &PrometheusRemoteWriteOptimizedTable) -> HashSet<String> {
        config.metrics.iter().map(|m| m.name.clone()).collect()
    }

    fn prometheus_schema(label_names: &[String]) -> ConnectionSchema {
        /* Hardcoded schema - replaced with dynamic generation
        let fields = vec![
            source_field("metric_name", Primitive(PrimitiveType::String)),
            source_field("timestamp", Primitive(PrimitiveType::UnixMillis)),
            source_field("value", Primitive(PrimitiveType::F64)),
            source_field("instance", Primitive(PrimitiveType::String)),
            source_field("job", Primitive(PrimitiveType::String)),
            source_field("label_0", Primitive(PrimitiveType::String)),
            source_field("label_1", Primitive(PrimitiveType::String)),
            source_field("label_2", Primitive(PrimitiveType::String)),
        ];
        */

        // Dynamic schema generation based on label_names
        let mut fields = vec![
            source_field("metric_name", Primitive(PrimitiveType::String)),
            source_field("timestamp", Primitive(PrimitiveType::UnixMillis)),
            source_field("value", Primitive(PrimitiveType::F64)),
        ];

        // Add one field per label (nullable because of union schema - not all metrics have all labels)
        for label_name in label_names {
            let mut field = source_field(label_name, Primitive(PrimitiveType::String));
            field.nullable = true; // CRITICAL: Labels must be nullable for union schema
            fields.push(field);
        }

        // NOTE: _timestamp is added automatically by Arroyo, don't include it here!

        ConnectionSchema {
            format: None,
            framing: None,
            bad_data: None,
            struct_name: Some("PrometheusMetric".to_string()),
            fields,
            definition: None,
            // by setting this to true, we don't need to supply a schema when using the Arroyo API to create a source
            inferred: Some(true),
            primary_keys: Default::default(),
        }
    }
}

impl Connector for PrometheusRemoteWriteOptimizedConnector {
    type ProfileT = EmptyConfig;
    type TableT = PrometheusRemoteWriteOptimizedTable;

    fn name(&self) -> &'static str {
        "prometheus_remote_write_optimized"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "prometheus_remote_write_optimized".to_string(),
            name: "Prometheus Remote Write (Optimized)".to_string(),
            icon: ICON.to_string(),
            description: "Receive metrics from Prometheus remote_write protocol with high-performance flattened label schema".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
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
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        let all_labels = Self::collect_all_labels(&table);
        Some(Self::prometheus_schema(&all_labels))
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        tokio::task::spawn(async move {
            let message = match Self::test_connection(&table).await {
                Ok(_) => {
                    let all_labels = Self::collect_all_labels(&table);
                    TestSourceMessage {
                        error: false,
                        done: true,
                        message: format!(
                            "Successfully validated Prometheus remote_write optimized endpoint on {}:{}{} with {} metrics and {} unique labels",
                            table.bind_address.as_deref().unwrap_or("0.0.0.0"),
                            table.base_port.unwrap_or(9090),
                            table.path.as_deref().unwrap_or("/receive"),
                            table.metrics.len(),
                            all_labels.len()
                        ),
                    }
                }
                Err(err) => TestSourceMessage {
                    error: true,
                    done: true,
                    message: format!("Failed to validate connection: {err}"),
                },
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
        let base_port = options.pull_opt_i64("base_port")?.map(|p| p as u16);
        let path = options.pull_opt_str("path")?.map(|s| s.to_string());
        let bind_address = options.pull_opt_str("bind_address")?.map(|s| s.to_string());

        let metrics = if let Some(metrics_str) = options.pull_opt_str("metrics")? {
            serde_json::from_str(&metrics_str)
                .map_err(|e| anyhow!("Failed to parse metrics: {}", e))?
        } else {
            return Err(anyhow!("metrics field is required"));
        };

        let table = PrometheusRemoteWriteOptimizedTable {
            base_port,
            path,
            bind_address,
            metrics,
        };

        self.from_config(None, name, EmptyConfig {}, table, schema)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        // Validate metrics array
        if table.metrics.is_empty() {
            return Err(anyhow!("metrics array cannot be empty"));
        }

        for metric in &table.metrics {
            if metric.name.is_empty() {
                return Err(anyhow!("metric name cannot be empty"));
            }
            if metric.labels.is_empty() {
                return Err(anyhow!(
                    "metric '{}' must have at least one label",
                    metric.name
                ));
            }
        }

        let port = table.base_port.unwrap_or(9090);
        let path = table.path.as_deref().unwrap_or("/receive");
        let bind_address = table.bind_address.as_deref().unwrap_or("0.0.0.0");

        let metric_names: Vec<&str> = table.metrics.iter().map(|m| m.name.as_str()).collect();
        let all_labels = Self::collect_all_labels(&table);
        let description = format!(
            "PrometheusRemoteWriteOptimized<{}:{}{}, metrics=[{}], union_labels=[{}]>",
            bind_address,
            port,
            path,
            metric_names.join(", "),
            all_labels.join(", ")
        );

        let config_value = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(&table).unwrap(),
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
            Self::prometheus_schema(&all_labels),
            &config_value,
            description,
        ))
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        _config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        let all_labels = Self::collect_all_labels(&table);
        let metric_filter = Self::get_metric_filter(&table);

        let port = table.base_port.unwrap_or(9090);
        let path = table.path.unwrap_or_else(|| "/receive".to_string());
        let bind_address = table.bind_address.unwrap_or_else(|| "0.0.0.0".to_string());

        Ok(ConstructedOperator::from_source(Box::new(
            PrometheusRemoteWriteOptimizedSourceFunc::new(
                bind_address,
                port,
                path,
                all_labels,
                metric_filter,
            ),
        )))
    }
}

impl PrometheusRemoteWriteOptimizedConnector {
    async fn test_connection(table: &PrometheusRemoteWriteOptimizedTable) -> anyhow::Result<()> {
        let port = table.base_port.unwrap_or(9090);
        let bind_address = table.bind_address.as_deref().unwrap_or("0.0.0.0");

        // Test if we can bind to the address and port
        let addr = format!("{bind_address}:{port}");
        tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| anyhow!("Cannot bind to {}: {}", addr, e))?;

        Ok(())
    }
}
