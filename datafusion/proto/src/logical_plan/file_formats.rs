// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::{
    config::{CsvOptions, JsonOptions},
    datasource::file_format::{
        arrow::ArrowFormatFactory, csv::CsvFormatFactory, json::JsonFormatFactory,
        parquet::ParquetFormatFactory, FileFormatFactory,
    },
    prelude::SessionContext,
};
use datafusion_common::{
    exec_err, not_impl_err, parsers::CompressionTypeVariant, DataFusionError,
    TableReference,
};
use prost::Message;

use crate::protobuf::{CsvOptions as CsvOptionsProto, JsonOptions as JsonOptionsProto};

use super::LogicalExtensionCodec;

#[derive(Debug)]
pub struct CsvLogicalExtensionCodec;

impl CsvOptionsProto {
    fn from_factory(factory: &CsvFormatFactory) -> Self {
        if let Some(options) = &factory.options {
            CsvOptionsProto {
                has_header: options.has_header.map_or(vec![], |v| vec![v as u8]),
                delimiter: vec![options.delimiter],
                quote: vec![options.quote],
                escape: options.escape.map_or(vec![], |v| vec![v]),
                double_quote: options.double_quote.map_or(vec![], |v| vec![v as u8]),
                compression: options.compression as i32,
                schema_infer_max_rec: options.schema_infer_max_rec as u64,
                date_format: options.date_format.clone().unwrap_or_default(),
                datetime_format: options.datetime_format.clone().unwrap_or_default(),
                timestamp_format: options.timestamp_format.clone().unwrap_or_default(),
                timestamp_tz_format: options
                    .timestamp_tz_format
                    .clone()
                    .unwrap_or_default(),
                time_format: options.time_format.clone().unwrap_or_default(),
                null_value: options.null_value.clone().unwrap_or_default(),
                comment: options.comment.map_or(vec![], |v| vec![v]),
                newlines_in_values: options
                    .newlines_in_values
                    .map_or(vec![], |v| vec![v as u8]),
            }
        } else {
            CsvOptionsProto::default()
        }
    }
}

impl From<&CsvOptionsProto> for CsvOptions {
    fn from(proto: &CsvOptionsProto) -> Self {
        CsvOptions {
            has_header: if !proto.has_header.is_empty() {
                Some(proto.has_header[0] != 0)
            } else {
                None
            },
            delimiter: proto.delimiter.first().copied().unwrap_or(b','),
            quote: proto.quote.first().copied().unwrap_or(b'"'),
            escape: if !proto.escape.is_empty() {
                Some(proto.escape[0])
            } else {
                None
            },
            double_quote: if !proto.double_quote.is_empty() {
                Some(proto.double_quote[0] != 0)
            } else {
                None
            },
            compression: match proto.compression {
                0 => CompressionTypeVariant::GZIP,
                1 => CompressionTypeVariant::BZIP2,
                2 => CompressionTypeVariant::XZ,
                3 => CompressionTypeVariant::ZSTD,
                _ => CompressionTypeVariant::UNCOMPRESSED,
            },
            schema_infer_max_rec: proto.schema_infer_max_rec as usize,
            date_format: if proto.date_format.is_empty() {
                None
            } else {
                Some(proto.date_format.clone())
            },
            datetime_format: if proto.datetime_format.is_empty() {
                None
            } else {
                Some(proto.datetime_format.clone())
            },
            timestamp_format: if proto.timestamp_format.is_empty() {
                None
            } else {
                Some(proto.timestamp_format.clone())
            },
            timestamp_tz_format: if proto.timestamp_tz_format.is_empty() {
                None
            } else {
                Some(proto.timestamp_tz_format.clone())
            },
            time_format: if proto.time_format.is_empty() {
                None
            } else {
                Some(proto.time_format.clone())
            },
            null_value: if proto.null_value.is_empty() {
                None
            } else {
                Some(proto.null_value.clone())
            },
            comment: if !proto.comment.is_empty() {
                Some(proto.comment[0])
            } else {
                None
            },
            newlines_in_values: if proto.newlines_in_values.is_empty() {
                None
            } else {
                Some(proto.newlines_in_values[0] != 0)
            },
        }
    }
}

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for CsvLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<
        std::sync::Arc<dyn datafusion::datasource::TableProvider>,
    > {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        let proto = CsvOptionsProto::decode(buf).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to decode CsvOptionsProto: {:?}",
                e
            ))
        })?;
        let options: CsvOptions = (&proto).into();
        Ok(Arc::new(CsvFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let options =
            if let Some(csv_factory) = node.as_any().downcast_ref::<CsvFormatFactory>() {
                csv_factory.options.clone().unwrap_or_default()
            } else {
                return exec_err!("{}", "Unsupported FileFormatFactory type".to_string());
            };

        let proto = CsvOptionsProto::from_factory(&CsvFormatFactory {
            options: Some(options),
        });

        proto.encode(buf).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode CsvOptions: {:?}", e))
        })?;

        Ok(())
    }
}

impl JsonOptionsProto {
    fn from_factory(factory: &JsonFormatFactory) -> Self {
        if let Some(options) = &factory.options {
            JsonOptionsProto {
                compression: options.compression as i32,
                schema_infer_max_rec: options.schema_infer_max_rec as u64,
            }
        } else {
            JsonOptionsProto::default()
        }
    }
}

impl From<&JsonOptionsProto> for JsonOptions {
    fn from(proto: &JsonOptionsProto) -> Self {
        JsonOptions {
            compression: match proto.compression {
                0 => CompressionTypeVariant::GZIP,
                1 => CompressionTypeVariant::BZIP2,
                2 => CompressionTypeVariant::XZ,
                3 => CompressionTypeVariant::ZSTD,
                _ => CompressionTypeVariant::UNCOMPRESSED,
            },
            schema_infer_max_rec: proto.schema_infer_max_rec as usize,
        }
    }
}

#[derive(Debug)]
pub struct JsonLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for JsonLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<
        std::sync::Arc<dyn datafusion::datasource::TableProvider>,
    > {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        _ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        let proto = JsonOptionsProto::decode(buf).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to decode JsonOptionsProto: {:?}",
                e
            ))
        })?;
        let options: JsonOptions = (&proto).into();
        Ok(Arc::new(JsonFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let options = if let Some(json_factory) =
            node.as_any().downcast_ref::<JsonFormatFactory>()
        {
            json_factory.options.clone().unwrap_or_default()
        } else {
            return Err(DataFusionError::Execution(
                "Unsupported FileFormatFactory type".to_string(),
            ));
        };

        let proto = JsonOptionsProto::from_factory(&JsonFormatFactory {
            options: Some(options),
        });

        proto.encode(buf).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode JsonOptions: {:?}", e))
        })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ParquetLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for ParquetLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<
        std::sync::Arc<dyn datafusion::datasource::TableProvider>,
    > {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        __buf: &[u8],
        __ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(ParquetFormatFactory::new()))
    }

    fn try_encode_file_format(
        &self,
        __buf: &mut Vec<u8>,
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ArrowLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for ArrowLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<
        std::sync::Arc<dyn datafusion::datasource::TableProvider>,
    > {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        __buf: &[u8],
        __ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(ArrowFormatFactory::new()))
    }

    fn try_encode_file_format(
        &self,
        __buf: &mut Vec<u8>,
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct AvroLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for AvroLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<datafusion_expr::Extension> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode(
        &self,
        _node: &datafusion_expr::Extension,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: arrow::datatypes::SchemaRef,
        _cts: &datafusion::prelude::SessionContext,
    ) -> datafusion_common::Result<
        std::sync::Arc<dyn datafusion::datasource::TableProvider>,
    > {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: std::sync::Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        __buf: &[u8],
        __ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(ArrowFormatFactory::new()))
    }

    fn try_encode_file_format(
        &self,
        __buf: &mut Vec<u8>,
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
}
