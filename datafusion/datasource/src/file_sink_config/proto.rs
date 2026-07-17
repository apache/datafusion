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

//! Shared serialization of the format-agnostic [`FileSinkConfig`] spine.
//!
//! This is the relocated body of `datafusion-proto`'s `FileSinkConfig` <-> proto
//! conversion (the `TryFromProto<&FileSinkConfig>` / `TryFromProto<&protobuf::FileSinkConfig>`
//! impls). Every concrete sink's `DataSink::try_to_proto` hook (CSV, JSON,
//! Parquet) builds its `*SinkExecNode` around [`FileSinkConfig::to_proto`] and
//! decodes with [`FileSinkConfig::from_proto`], keeping a single copy of the
//! shared wire logic. The wire format is byte-for-byte identical to the old
//! central serializer.
//!
//! Unlike the scan spine, [`FileSinkConfig`] carries no child physical
//! expressions, so this conversion needs neither the `PhysicalExtensionCodec`
//! nor the [`ExecutionPlanEncodeCtx`]/[`ExecutionPlanDecodeCtx`]: `Schema`,
//! `DataType`, and `PartitionedFile` all go through `datafusion-proto-common`
//! (and the shared scan helpers). The sink's optional required-ordering *does*
//! carry exprs, so [`parse_sink_sort_order`] rides the decode ctx (the encode
//! side lives in `DataSinkExec::try_to_proto`).
//!
//! [`ExecutionPlanEncodeCtx`]: datafusion_physical_plan::proto::ExecutionPlanEncodeCtx
//! [`ExecutionPlanDecodeCtx`]: datafusion_physical_plan::proto::ExecutionPlanDecodeCtx

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use datafusion_physical_plan::proto::ExecutionPlanDecodeCtx;
use datafusion_proto_models::protobuf;

use crate::ListingTableUrl;
use crate::file_groups::FileGroup;
use crate::file_scan_config::proto::{
    partitioned_file_from_proto, partitioned_file_to_proto,
};
use crate::file_sink_config::{FileOutputMode, FileSinkConfig};

impl FileSinkConfig {
    /// Serialize the shared, format-agnostic part of a file sink into a
    /// [`protobuf::FileSinkConfig`].
    ///
    /// Each concrete sink's `DataSink::try_to_proto` wraps the returned value in
    /// its own `*Sink` node. Byte-compatible with the former
    /// `TryFromProto<&FileSinkConfig> for protobuf::FileSinkConfig` in
    /// `datafusion-proto`.
    pub fn to_proto(&self) -> Result<protobuf::FileSinkConfig> {
        let file_groups = self
            .file_group
            .iter()
            .map(partitioned_file_to_proto)
            .collect::<Result<Vec<_>>>()?;
        let table_paths = self
            .table_paths
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let table_partition_cols = self
            .table_partition_cols
            .iter()
            .map(|(name, data_type)| {
                Ok(protobuf::PartitionColumn {
                    name: name.to_owned(),
                    arrow_type: Some(data_type.try_into()?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let file_output_mode = match self.file_output_mode {
            FileOutputMode::Automatic => protobuf::FileOutputMode::Automatic,
            FileOutputMode::SingleFile => protobuf::FileOutputMode::SingleFile,
            FileOutputMode::Directory => protobuf::FileOutputMode::Directory,
        };
        Ok(protobuf::FileSinkConfig {
            object_store_url: self.object_store_url.to_string(),
            file_groups,
            table_paths,
            output_schema: Some(self.output_schema.as_ref().try_into()?),
            table_partition_cols,
            keep_partition_by_columns: self.keep_partition_by_columns,
            insert_op: self.insert_op as i32,
            file_extension: self.file_extension.to_string(),
            file_output_mode: file_output_mode.into(),
        })
    }

    /// Reconstruct a [`FileSinkConfig`] from a [`protobuf::FileSinkConfig`].
    ///
    /// Byte-compatible with the former
    /// `TryFromProto<&protobuf::FileSinkConfig> for FileSinkConfig` in
    /// `datafusion-proto`.
    pub fn from_proto(conf: &protobuf::FileSinkConfig) -> Result<FileSinkConfig> {
        let file_group = FileGroup::new(
            conf.file_groups
                .iter()
                .map(partitioned_file_from_proto)
                .collect::<Result<Vec<_>>>()?,
        );
        let table_paths = conf
            .table_paths
            .iter()
            .map(ListingTableUrl::parse)
            .collect::<Result<Vec<_>>>()?;
        let table_partition_cols = conf
            .table_partition_cols
            .iter()
            .map(|protobuf::PartitionColumn { name, arrow_type }| {
                let data_type = arrow_type
                    .as_ref()
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "PartitionColumn is missing required field 'arrow_type'"
                        )
                    })?
                    .try_into()?;
                Ok((name.clone(), data_type))
            })
            .collect::<Result<Vec<_>>>()?;
        let insert_op = match conf.insert_op() {
            protobuf::InsertOp::Append => InsertOp::Append,
            protobuf::InsertOp::Overwrite => InsertOp::Overwrite,
            protobuf::InsertOp::Replace => InsertOp::Replace,
        };
        let file_output_mode = match conf.file_output_mode() {
            protobuf::FileOutputMode::Automatic => FileOutputMode::Automatic,
            protobuf::FileOutputMode::SingleFile => FileOutputMode::SingleFile,
            protobuf::FileOutputMode::Directory => FileOutputMode::Directory,
        };
        let output_schema = conf.output_schema.as_ref().ok_or_else(|| {
            internal_datafusion_err!(
                "FileSinkConfig is missing required field 'output_schema'"
            )
        })?;
        Ok(FileSinkConfig {
            original_url: String::default(),
            object_store_url: ObjectStoreUrl::parse(&conf.object_store_url)?,
            file_group,
            table_paths,
            output_schema: Arc::new(output_schema.try_into()?),
            table_partition_cols,
            insert_op,
            keep_partition_by_columns: conf.keep_partition_by_columns,
            file_extension: conf.file_extension.clone(),
            file_output_mode,
        })
    }
}

/// Decode a sink's optional required output ordering from its serialized
/// `PhysicalSortExprNodeCollection`, resolving the child expressions against
/// `schema` (the decoded input's schema). Byte-compatible with the former
/// `parse_physical_sort_exprs` + `LexRequirement::new` dance in the central
/// `try_into_*_sink_physical_plan` decoders.
///
/// The encode counterpart is inlined in
/// [`DataSinkExec::try_to_proto`](crate::sink::DataSinkExec) (it needs the
/// [`ExecutionPlanEncodeCtx`](datafusion_physical_plan::proto::ExecutionPlanEncodeCtx)).
pub fn parse_sink_sort_order(
    collection: Option<&protobuf::PhysicalSortExprNodeCollection>,
    ctx: &ExecutionPlanDecodeCtx<'_>,
    schema: &Schema,
) -> Result<Option<LexRequirement>> {
    let Some(collection) = collection else {
        return Ok(None);
    };
    let sort_exprs = collection
        .physical_sort_expr_nodes
        .iter()
        .map(|node| {
            let expr = node.expr.as_ref().ok_or_else(|| {
                internal_datafusion_err!("Unexpected empty physical expression")
            })?;
            Ok(PhysicalSortExpr {
                expr: ctx.decode_expr(expr, schema)?,
                options: SortOptions {
                    descending: !node.asc,
                    nulls_first: node.nulls_first,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LexRequirement::new(sort_exprs.into_iter().map(Into::into)))
}
