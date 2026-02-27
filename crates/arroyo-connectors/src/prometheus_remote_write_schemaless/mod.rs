mod operator;

use crate::{source_field, EmptyConfig};
use anyhow::anyhow;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, FieldType::Primitive, PrimitiveType,
    TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use operator::PrometheusRemoteWriteSchemalessSourceFunc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./prometheus_remote_write.svg");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusRemoteWriteSchemalessTable {
    pub base_port: Option<u16>,
    pub path: Option<String>,
    pub bind_address: Option<String>,
}

pub struct PrometheusRemoteWriteSchemalessConnector {}

impl PrometheusRemoteWriteSchemalessConnector {
    fn prometheus_schema() -> ConnectionSchema {
        ConnectionSchema {
            format: None,
            framing: None,
            bad_data: None,
            struct_name: Some("PrometheusMetric".to_string()),
            fields: vec![
                source_field("metric_name", Primitive(PrimitiveType::String)),
                source_field("timestamp", Primitive(PrimitiveType::UnixMillis)),
                source_field("value", Primitive(PrimitiveType::F64)),
                source_field("labels", Primitive(PrimitiveType::String)),
            ],
            definition: None,
            inferred: None,
            primary_keys: Default::default(),
        }
    }
}

impl Connector for PrometheusRemoteWriteSchemalessConnector {
    type ProfileT = EmptyConfig;
    type TableT = PrometheusRemoteWriteSchemalessTable;

    fn name(&self) -> &'static str {
        "prometheus_remote_write_schemaless"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "prometheus_remote_write_schemaless".to_string(),
            name: "Prometheus Remote Write (Schemaless)".to_string(),
            icon: ICON.to_string(),
            description: "Receive metrics from Prometheus remote_write protocol (schemaless)"
                .to_string(),
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
        _: Self::TableT,
        _: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        Some(Self::prometheus_schema())
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
                Ok(_) => TestSourceMessage {
                    error: false,
                    done: true,
                    message: format!(
                        "Successfully validated Prometheus remote_write endpoint on {}:{}{}",
                        table.bind_address.as_deref().unwrap_or("0.0.0.0"),
                        table.base_port.unwrap_or(9090),
                        table.path.as_deref().unwrap_or("/receive")
                    ),
                },
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

        let table = PrometheusRemoteWriteSchemalessTable {
            base_port,
            path,
            bind_address,
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
        let port = table.base_port.unwrap_or(9090);
        let path = table.path.as_deref().unwrap_or("/receive");
        let bind_address = table.bind_address.as_deref().unwrap_or("0.0.0.0");

        let description = format!("PrometheusRemoteWriteSchemaless<{bind_address}:{port}{path}>");

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
            Self::prometheus_schema(),
            &config,
            description,
        ))
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        _config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        let port = table.base_port.unwrap_or(9090);
        let path = table.path.unwrap_or_else(|| "/receive".to_string());
        let bind_address = table.bind_address.unwrap_or_else(|| "0.0.0.0".to_string());

        Ok(ConstructedOperator::from_source(Box::new(
            PrometheusRemoteWriteSchemalessSourceFunc::new(bind_address, port, path),
        )))
    }
}

impl PrometheusRemoteWriteSchemalessConnector {
    async fn test_connection(table: &PrometheusRemoteWriteSchemalessTable) -> anyhow::Result<()> {
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
