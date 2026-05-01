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
        let options: CsvOptions = (&proto).try_into()?;
        Ok(Arc::new(CsvFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let factory = if let Some(csv_factory) = node.downcast_ref::<CsvFormatFactory>() {
            csv_factory
        } else {
            return exec_err!("{}", "Unsupported FileFormatFactory type".to_string());
        };

        let proto: CsvOptionsProto = factory.into();

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
        let options: JsonOptions = (&proto).try_into()?;
        Ok(Arc::new(JsonFormatFactory {
            options: Some(options),
        }))
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        let factory = if let Some(json_factory) = node.downcast_ref::<JsonFormatFactory>()
        {
            json_factory
        } else {
            return exec_err!("Unsupported FileFormatFactory type");
        };

        let proto: JsonOptionsProto = factory.into();

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
            let options: TableParquetOptions = (&proto).try_into()?;
            Ok(Arc::new(
                datafusion_datasource_parquet::file_format::ParquetFormatFactory {
                    options: Some(options),
                },
            ))
        }

        fn try_encode_file_format(
            &self,
            buf: &mut Vec<u8>,
            node: Arc<dyn FileFormatFactory>,
        ) -> datafusion_common::Result<()> {
            use datafusion_datasource_parquet::file_format::ParquetFormatFactory;

            let factory = if let Some(parquet_factory) =
                node.downcast_ref::<ParquetFormatFactory>()
            {
                parquet_factory
            } else {
                return exec_err!("Unsupported FileFormatFactory type");
            };

            let proto: TableParquetOptionsProto = factory.into();

            proto.encode(buf).map_err(|e| {
                exec_datafusion_err!("Failed to encode TableParquetOptionsProto: {e:?}")
            })?;

            Ok(())
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
