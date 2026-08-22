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

use super::LogicalExtensionCodec;
use crate::protobuf::{CsvOptions as CsvOptionsProto, JsonOptions as JsonOptionsProto};
use datafusion_common::config::{CsvOptions, JsonOptions};
use datafusion_common::{TableReference, exec_datafusion_err, exec_err, not_impl_err};
use datafusion_datasource::file_format::FileFormatFactory;
use datafusion_datasource_arrow::file_format::ArrowFormatFactory;
use datafusion_datasource_csv::file_format::CsvFormatFactory;
use datafusion_datasource_json::file_format::JsonFormatFactory;
use datafusion_execution::TaskContext;
use prost::Message;

#[derive(Debug)]
pub struct CsvLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for CsvLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &TaskContext,
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
        _ctx: &TaskContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion_catalog::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion_catalog::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        _ctx: &TaskContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        let proto = CsvOptionsProto::decode(buf).map_err(|e| {
            exec_datafusion_err!("Failed to decode CsvOptionsProto: {e:?}")
        })?;
        let options = CsvOptions::from(&proto);
        Ok(Arc::new(CsvFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let options = if let Some(csv_factory) = node.downcast_ref::<CsvFormatFactory>() {
            csv_factory.options.clone().unwrap_or_default()
        } else {
            return exec_err!("{}", "Unsupported FileFormatFactory type".to_string());
        };

        let proto = CsvOptionsProto::from(&CsvFormatFactory {
            options: Some(options),
        });

        proto
            .encode(buf)
            .map_err(|e| exec_datafusion_err!("Failed to encode CsvOptions: {e:?}"))?;

        Ok(())
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
        _ctx: &TaskContext,
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
        _ctx: &TaskContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion_catalog::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion_catalog::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        _ctx: &TaskContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        let proto = JsonOptionsProto::decode(buf).map_err(|e| {
            exec_datafusion_err!("Failed to decode JsonOptionsProto: {e:?}")
        })?;
        let options = JsonOptions::from(&proto);
        Ok(Arc::new(JsonFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let options = if let Some(json_factory) = node.downcast_ref::<JsonFormatFactory>()
        {
            json_factory.options.clone().unwrap_or_default()
        } else {
            return exec_err!("Unsupported FileFormatFactory type");
        };

        let proto = JsonOptionsProto::from(&JsonFormatFactory {
            options: Some(options),
        });

        proto
            .encode(buf)
            .map_err(|e| exec_datafusion_err!("Failed to encode JsonOptions: {e:?}"))?;

        Ok(())
    }
}

#[cfg(feature = "parquet")]
mod parquet {
    use super::*;

    use crate::protobuf::TableParquetOptions as TableParquetOptionsProto;
    use datafusion_common::config::TableParquetOptions;
    use datafusion_datasource_parquet::file_format::ParquetFormatFactory;

    #[derive(Debug)]
    pub struct ParquetLogicalExtensionCodec;

    // TODO! This is a placeholder for now and needs to be implemented for real.
    impl LogicalExtensionCodec for ParquetLogicalExtensionCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[datafusion_expr::LogicalPlan],
            _ctx: &TaskContext,
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
            _ctx: &TaskContext,
        ) -> datafusion_common::Result<Arc<dyn datafusion_catalog::TableProvider>>
        {
            not_impl_err!("Method not implemented")
        }

        fn try_encode_table_provider(
            &self,
            _table_ref: &TableReference,
            _node: Arc<dyn datafusion_catalog::TableProvider>,
            _buf: &mut Vec<u8>,
        ) -> datafusion_common::Result<()> {
            not_impl_err!("Method not implemented")
        }

        fn try_decode_file_format(
            &self,
            buf: &[u8],
            _ctx: &TaskContext,
        ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
            let proto = TableParquetOptionsProto::decode(buf).map_err(|e| {
                exec_datafusion_err!("Failed to decode TableParquetOptionsProto: {e:?}")
            })?;
            let options = TableParquetOptions::try_from(&proto)?;
            Ok(Arc::new(ParquetFormatFactory {
                options: Some(options),
            }))
        }

        fn try_encode_file_format(
            &self,
            buf: &mut Vec<u8>,
            node: Arc<dyn FileFormatFactory>,
        ) -> datafusion_common::Result<()> {
            use datafusion_datasource_parquet::file_format::ParquetFormatFactory;

            let options = if let Some(parquet_factory) =
                node.downcast_ref::<ParquetFormatFactory>()
            {
                parquet_factory.options.clone().unwrap_or_default()
            } else {
                return exec_err!("Unsupported FileFormatFactory type");
            };

            let proto = TableParquetOptionsProto::from(&ParquetFormatFactory {
                options: Some(options),
            });

            proto.encode(buf).map_err(|e| {
                exec_datafusion_err!("Failed to encode TableParquetOptionsProto: {e:?}")
            })?;

            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::protobuf::ParquetOptions as ParquetOptionsProto;
        use datafusion_common::config::ParquetOptions;

        fn encode_table_options(proto: TableParquetOptionsProto) -> Vec<u8> {
            let mut buf = Vec::new();
            proto.encode(&mut buf).expect("encode parquet options");
            buf
        }

        #[test]
        fn try_decode_file_format_errors_on_invalid_writer_version() {
            let proto = TableParquetOptionsProto {
                global: Some(ParquetOptionsProto {
                    writer_version: "3.0".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            };

            let result = ParquetLogicalExtensionCodec.try_decode_file_format(
                &encode_table_options(proto),
                &TaskContext::default(),
            );

            let err = result.expect_err("invalid writer version should error");
            assert!(
                err.to_string()
                    .contains("Invalid parquet writer version: 3.0"),
                "{err}"
            );
        }

        #[test]
        fn try_decode_file_format_defaults_empty_writer_version() {
            let proto = TableParquetOptionsProto {
                global: Some(ParquetOptionsProto::default()),
                ..Default::default()
            };

            let factory = ParquetLogicalExtensionCodec
                .try_decode_file_format(
                    &encode_table_options(proto),
                    &TaskContext::default(),
                )
                .expect("decode parquet options");
            let parquet_factory = factory
                .downcast_ref::<ParquetFormatFactory>()
                .expect("parquet format factory");
            let options = parquet_factory.options.as_ref().expect("parquet options");

            assert_eq!(
                options.global.writer_version,
                ParquetOptions::default().writer_version
            );
        }

        #[test]
        fn enable_rle_to_dictionary_round_trips_through_codec() {
            use datafusion_common::config::TableParquetOptions;
            let mut options = TableParquetOptions::default();
            options.global.enable_rle_to_dictionary = true;
            let original: Arc<dyn FileFormatFactory> = Arc::new(ParquetFormatFactory {
                options: Some(options),
            });

            let mut buf = Vec::new();
            ParquetLogicalExtensionCodec
                .try_encode_file_format(&mut buf, Arc::clone(&original))
                .expect("encode parquet options");

            let decoded = ParquetLogicalExtensionCodec
                .try_decode_file_format(&buf, &TaskContext::default())
                .expect("decode parquet options");
            let decoded_options = decoded
                .downcast_ref::<ParquetFormatFactory>()
                .expect("parquet format factory")
                .options
                .as_ref()
                .expect("parquet options");

            assert!(decoded_options.global.enable_rle_to_dictionary);
        }
    }
}
#[cfg(feature = "parquet")]
pub use parquet::ParquetLogicalExtensionCodec;

#[derive(Debug)]
pub struct ArrowLogicalExtensionCodec;

// TODO! This is a placeholder for now and needs to be implemented for real.
impl LogicalExtensionCodec for ArrowLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion_expr::LogicalPlan],
        _ctx: &TaskContext,
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
        _ctx: &TaskContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion_catalog::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion_catalog::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        __buf: &[u8],
        __ctx: &TaskContext,
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
        _ctx: &TaskContext,
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
        _cts: &TaskContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion_catalog::TableProvider>> {
        not_impl_err!("Method not implemented")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion_catalog::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Method not implemented")
    }

    fn try_decode_file_format(
        &self,
        __buf: &[u8],
        __ctx: &TaskContext,
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
