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
    datasource::file_format::{
        arrow::ArrowFormatFactory, csv::CsvFormatFactory, json::JsonFormatFactory,
        parquet::ParquetFormatFactory, FileFormatFactory,
    },
    prelude::SessionContext,
};
use datafusion_common::{not_impl_err, TableReference};

use super::LogicalExtensionCodec;

#[derive(Debug)]
pub struct CsvLogicalExtensionCodec;

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
        __buf: &[u8],
        __ctx: &SessionContext,
    ) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
        Ok(Arc::new(CsvFormatFactory::new()))
    }

    fn try_encode_file_format(
        &self,
        __buf: &[u8],
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }

    fn try_decode_udf(
        &self,
        name: &str,
        __buf: &[u8],
    ) -> datafusion_common::Result<Arc<datafusion_expr::ScalarUDF>> {
        not_impl_err!("LogicalExtensionCodec is not provided for scalar function {name}")
    }

    fn try_encode_udf(
        &self,
        __node: &datafusion_expr::ScalarUDF,
        __buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
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
        Ok(Arc::new(JsonFormatFactory::new()))
    }

    fn try_encode_file_format(
        &self,
        __buf: &[u8],
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }

    fn try_decode_udf(
        &self,
        name: &str,
        __buf: &[u8],
    ) -> datafusion_common::Result<Arc<datafusion_expr::ScalarUDF>> {
        not_impl_err!("LogicalExtensionCodec is not provided for scalar function {name}")
    }

    fn try_encode_udf(
        &self,
        __node: &datafusion_expr::ScalarUDF,
        __buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
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
        __buf: &[u8],
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }

    fn try_decode_udf(
        &self,
        name: &str,
        __buf: &[u8],
    ) -> datafusion_common::Result<Arc<datafusion_expr::ScalarUDF>> {
        not_impl_err!("LogicalExtensionCodec is not provided for scalar function {name}")
    }

    fn try_encode_udf(
        &self,
        __node: &datafusion_expr::ScalarUDF,
        __buf: &mut Vec<u8>,
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
        __buf: &[u8],
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }

    fn try_decode_udf(
        &self,
        name: &str,
        __buf: &[u8],
    ) -> datafusion_common::Result<Arc<datafusion_expr::ScalarUDF>> {
        not_impl_err!("LogicalExtensionCodec is not provided for scalar function {name}")
    }

    fn try_encode_udf(
        &self,
        __node: &datafusion_expr::ScalarUDF,
        __buf: &mut Vec<u8>,
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
        __buf: &[u8],
        __node: Arc<dyn FileFormatFactory>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }

    fn try_decode_udf(
        &self,
        name: &str,
        __buf: &[u8],
    ) -> datafusion_common::Result<Arc<datafusion_expr::ScalarUDF>> {
        not_impl_err!("LogicalExtensionCodec is not provided for scalar function {name}")
    }

    fn try_encode_udf(
        &self,
        __node: &datafusion_expr::ScalarUDF,
        __buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
}
