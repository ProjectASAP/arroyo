use anyhow::{anyhow, bail, Result};
use typify::import_types;

use arroyo_operator::connector::Connection;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage,
};
use arroyo_rpc::formats::{
    BadData, Format, Framing, FramingMethod, JsonFormat, NewlineDelimitedFraming,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use serde::{Deserialize, Serialize};

use crate::EmptyConfig;

use crate::single_file_custom::source::SingleFileCustomSourceFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

const TABLE_SCHEMA: &str = include_str!("./table.json");

import_types!(schema = "src/single_file_custom/table.json");

mod source;

pub struct SingleFileCustomConnector {}

impl Connector for SingleFileCustomConnector {
    type ProfileT = EmptyConfig;

    type TableT = SingleFileCustomTable;

    fn name(&self) -> &'static str {
        "single_file_custom"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "single_file_custom".to_string(),
            name: "Single File Custom".to_string(),
            icon: "".to_string(),
            description: "Read a single local file with custom timestamp extraction".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: None,
            table_config: TABLE_SCHEMA.to_owned(),
        }
    }

    fn test(
        &self,
        _: &str,
        _: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: tokio::sync::mpsc::Sender<TestSourceMessage>,
    ) {
        let schema = schema.cloned();
        let path = table.path.clone();
        let timestamp_field = table.timestamp_field.clone();

        tokio::task::spawn(async move {
            if !std::path::Path::new(&path).exists() {
                let message = TestSourceMessage {
                    error: true,
                    done: true,
                    message: format!("File not found: {path}"),
                };
                let _ = tx.send(message).await;
                return;
            }

            if let Some(schema) = &schema {
                let field_exists = schema
                    .fields
                    .iter()
                    .any(|f| f.field_name == timestamp_field);
                if !field_exists {
                    let message = TestSourceMessage {
                        error: true,
                        done: true,
                        message: format!("Timestamp field '{timestamp_field}' not found in schema"),
                    };
                    let _ = tx.send(message).await;
                    return;
                }
            }

            let message = TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully validated connection".to_string(),
            };
            let _ = tx.send(message).await;
        });
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<arroyo_operator::connector::Connection> {
        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("no schema defined for Single File Custom connection"))?;

        let field_exists = schema
            .fields
            .iter()
            .any(|f| f.field_name == table.timestamp_field);
        if !field_exists {
            bail!(
                "Timestamp field '{}' not found in schema. Available fields: {:?}",
                table.timestamp_field,
                schema
                    .fields
                    .iter()
                    .map(|f| &f.field_name)
                    .collect::<Vec<_>>()
            );
        }

        let format = match table.file_format {
            FileFormat::Json => Format::Json(JsonFormat::default()),
            FileFormat::Parquet => {
                Format::Json(JsonFormat::default()) // placeholder, not used for parquet
            }
        };

        let framing = match table.file_format {
            FileFormat::Json => Some(Framing {
                method: FramingMethod::Newline(NewlineDelimitedFraming {
                    max_line_length: None,
                }),
            }),
            FileFormat::Parquet => None,
        };

        let bad_data = match table.bad_data_mode {
            Some(BadDataMode::Skip) => Some(BadData::Drop {}),
            Some(BadDataMode::Fail) | None => Some(BadData::Fail {}),
        };

        let op_config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(&table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data,
            framing,
            metadata_fields: vec![],
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            ConnectionType::Source,
            schema,
            &op_config,
            "Single File Custom".to_string(),
        ))
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let path = options.pull_str("path")?;
        let format_str = options.pull_str("file_format")?;
        let file_format = match format_str.as_str() {
            "json" => FileFormat::Json,
            "parquet" => FileFormat::Parquet,
            _ => bail!(
                "Invalid file_format '{}'. Expected 'json' or 'parquet'",
                format_str
            ),
        };

        let compression = match options.pull_opt_str("compression")?.as_deref() {
            Some("none") | None => Some(Compression::None),
            Some("gzip") => Some(Compression::Gzip),
            Some("zstd") => Some(Compression::Zstd),
            Some(other) => bail!(
                "Invalid compression '{}'. Expected 'none', 'gzip', or 'zstd'",
                other
            ),
        };

        let timestamp_field = options.pull_str("timestamp_field")?;

        let ts_format = match options.pull_opt_str("ts_format")?.as_deref() {
            Some("unix_millis") | None => Some(TsFormat::UnixMillis),
            Some("unix_seconds") => Some(TsFormat::UnixSeconds),
            Some("rfc3339") => Some(TsFormat::Rfc3339),
            Some(other) => bail!(
                "Invalid ts_format '{}'. Expected 'unix_millis', 'unix_seconds', or 'rfc3339'",
                other
            ),
        };

        let bad_data_mode = match options.pull_opt_str("bad_data_mode")?.as_deref() {
            Some("skip") => Some(BadDataMode::Skip),
            Some("fail") | None => Some(BadDataMode::Fail),
            Some(other) => bail!(
                "Invalid bad_data_mode '{}'. Expected 'skip' or 'fail'",
                other
            ),
        };

        self.from_config(
            None,
            name,
            EmptyConfig {},
            SingleFileCustomTable {
                path,
                file_format,
                compression,
                timestamp_field,
                ts_format,
                bad_data_mode,
            },
            schema,
        )
    }

    fn make_operator(
        &self,
        _: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> Result<ConstructedOperator> {
        Ok(ConstructedOperator::from_source(Box::new(
            SingleFileCustomSourceFunc::new(
                table.path,
                table.file_format,
                table.compression.unwrap_or(Compression::None),
                table.timestamp_field,
                table.ts_format.unwrap_or(TsFormat::UnixMillis),
                config.format.expect("Format must be set"),
                config.framing,
                config.bad_data,
            ),
        )))
    }
}
