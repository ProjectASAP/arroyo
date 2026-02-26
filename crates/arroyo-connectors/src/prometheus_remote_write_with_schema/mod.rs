mod operator;

use crate::EmptyConfig;
use anyhow::anyhow;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use operator::PrometheusRemoteWriteWithSchemaSourceFunc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./prometheus_remote_write.svg");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusRemoteWriteWithSchemaTable {
    pub base_port: Option<u16>,
    pub parallelism: Option<u32>,
    pub path: Option<String>,
    pub bind_address: Option<String>,
}

pub struct PrometheusRemoteWriteWithSchemaConnector {}

impl PrometheusRemoteWriteWithSchemaConnector {}

impl Connector for PrometheusRemoteWriteWithSchemaConnector {
    type ProfileT = EmptyConfig;
    type TableT = PrometheusRemoteWriteWithSchemaTable;

    fn name(&self) -> &'static str {
        "prometheus_remote_write_with_schema"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "prometheus_remote_write_with_schema".to_string(),
            name: "Prometheus Remote Write (With Schema)".to_string(),
            icon: ICON.to_string(),
            description: "Receive metrics from Prometheus remote_write protocol (with schema)".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: true,
            hidden: false,
            custom_schemas: true,
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
        schema: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        // Only return custom schema if provided, no default fallback like Kafka
        schema.cloned()
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
                        "Successfully validated Prometheus remote_write endpoint on {}:{}-{}{}",
                        table.bind_address.as_deref().unwrap_or("0.0.0.0"),
                        table.base_port.unwrap_or(9090),
                        table.base_port.unwrap_or(9090) + table.parallelism.unwrap_or(1) as u16 - 1,
                        table.path.as_deref().unwrap_or("/receive")
                    ),
                },
                Err(err) => TestSourceMessage {
                    error: true,
                    done: true,
                    message: format!("Failed to validate connection: {}", err),
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
        let parallelism = options.pull_opt_i64("parallelism")?.map(|p| p as u32);
        let path = options.pull_opt_str("path")?.map(|s| s.to_string());
        let bind_address = options.pull_opt_str("bind_address")?.map(|s| s.to_string());

        let table = PrometheusRemoteWriteWithSchemaTable {
            base_port,
            parallelism,
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
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        // Require schema like Kafka does
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for Prometheus connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Prometheus connection"))?;
        let base_port = table.base_port.unwrap_or(9090);
        let parallelism = table.parallelism.unwrap_or(1);
        let path = table.path.as_deref().unwrap_or("/receive");
        let bind_address = table.bind_address.as_deref().unwrap_or("0.0.0.0");

        let description = format!(
            "PrometheusRemoteWriteWithSchema<{}:{}-{}{}>", 
            bind_address, 
            base_port, 
            base_port + parallelism as u16 - 1, 
            path
        );

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: vec![],
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Source,
            schema,
            &config,
            description,
        ))
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        let base_port = table.base_port.unwrap_or(9090);
        let path = table.path.unwrap_or_else(|| "/receive".to_string());
        let bind_address = table.bind_address.unwrap_or_else(|| "0.0.0.0".to_string());

        let format = config
            .format
            .ok_or_else(|| anyhow!("Format must be set for Prometheus source"))?;

        Ok(ConstructedOperator::from_source(Box::new(
            PrometheusRemoteWriteWithSchemaSourceFunc::new(
                bind_address,
                base_port,
                path,
                format,
                config.framing,
                config.bad_data,
            ),
        )))
    }
}

impl PrometheusRemoteWriteWithSchemaConnector {
    async fn test_connection(table: &PrometheusRemoteWriteWithSchemaTable) -> anyhow::Result<()> {
        let base_port = table.base_port.unwrap_or(9090);
        let parallelism = table.parallelism.unwrap_or(1);
        let bind_address = table.bind_address.as_deref().unwrap_or("0.0.0.0");

        // Test if we can bind to all ports in the range
        for i in 0..parallelism {
            let port = base_port + i as u16;
            let addr = format!("{}:{}", bind_address, port);
            tokio::net::TcpListener::bind(&addr)
                .await
                .map_err(|e| anyhow!("Cannot bind to {}: {}", addr, e))?;
        }

        Ok(())
    }
}
