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

//! Shared serialization of the format-agnostic [`FileScanConfig`] spine.
//!
//! This is the relocated body of `datafusion-proto`'s
//! `serialize_file_scan_config` / `parse_protobuf_file_scan_config`, ported to
//! ride the
//! [`ExecutionPlanEncodeCtx`](datafusion_physical_plan::proto::ExecutionPlanEncodeCtx) /
//! [`ExecutionPlanDecodeCtx`](datafusion_physical_plan::proto::ExecutionPlanDecodeCtx)
//! instead of the raw `PhysicalExtensionCodec` +
//! `PhysicalProtoConverterExtension`. Every
//! `FileSource::try_to_proto` hook (CSV, JSON, Arrow, Parquet, Avro) builds its
//! `*ScanExecNode` around [`FileScanConfig::try_to_proto`] and decodes with
//! [`FileScanConfig::try_from_proto`], keeping a single copy of the shared
//! wire logic. Existing fields remain wire-compatible with the old central
//! serializer; new options use optional fields with legacy defaults.
//!
//! Child physical expressions (sort orderings, hash/range partitioning, and
//! projection expressions) are (de)serialized through `ctx.encode_expr` /
//! `ctx.decode_expr`; `Schema`, `Statistics`, `Constraints`, and `ScalarValue`
//! go through `datafusion-proto-common`. Nothing here needs the raw codec.

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::utils::{usize_from_wire, usize_to_wire};
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr::{LexOrdering, Partitioning};
use datafusion_physical_expr_common::sort_expr::{
    sort_exprs_try_from_proto, sort_exprs_try_to_proto,
};
use datafusion_physical_plan::proto::{ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx};
use datafusion_proto_models::datafusion_common::CompressionTypeVariant as ProtoCompressionTypeVariant;
use datafusion_proto_models::protobuf;

use crate::file::FileSource;
use crate::file_compression_type::FileCompressionType;
use crate::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use crate::table_schema::TableSchema;

impl FileScanConfig {
    /// Serialize the shared, format-agnostic part of a file scan into a
    /// [`protobuf::FileScanExecConf`].
    ///
    /// Each concrete [`FileSource::try_to_proto`]
    /// wraps the returned value in its own `*ScanExecNode`. Existing fields are
    /// byte-compatible with the former `serialize_file_scan_config` in
    /// `datafusion-proto`.
    pub fn try_to_proto(
        &self,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<protobuf::FileScanExecConf> {
        let file_groups = self
            .file_groups
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;

        let mut output_ordering = vec![];
        for order in &self.output_ordering {
            let nodes = sort_exprs_try_to_proto(order.iter(), &ctx.expr_ctx())?;
            output_ordering.push(protobuf::PhysicalSortExprNodeCollection {
                physical_sort_expr_nodes: nodes,
            });
        }

        let output_partitioning = self
            .output_partitioning
            .as_ref()
            .map(|partitioning| partitioning.try_to_proto(&ctx.expr_ctx()))
            .transpose()?;

        // Fields must be added to the schema so that they can persist in the
        // protobuf, and then removed from the schema in `try_from_proto`.
        let mut fields = self
            .file_schema()
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        fields.extend(self.table_partition_cols().iter().cloned());
        let schema =
            Schema::new(fields).with_metadata(self.file_schema().metadata.clone());

        let projection_exprs = self
            .file_source()
            .projection()
            .as_ref()
            .map(|projection_exprs| {
                Ok::<_, DataFusionError>(protobuf::ProjectionExprs {
                    projections: projection_exprs
                        .iter()
                        .map(|expr| {
                            Ok(protobuf::ProjectionExpr {
                                alias: expr.alias.to_string(),
                                expr: Some(ctx.encode_expr(&expr.expr)?),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                })
            })
            .transpose()?;

        let file_compression_type =
            self.file_compression_type.is_compressed().then(|| {
                let compression: ProtoCompressionTypeVariant =
                    (*self.file_compression_type.get_variant()).into();
                compression as i32
            });

        Ok(protobuf::FileScanExecConf {
            file_groups,
            statistics: Some((&self.statistics()).into()),
            limit: self
                .limit
                .map(|limit| usize_to_wire::<u32>(limit, "FileScanConfig", "limit"))
                .transpose()?
                .map(|limit| protobuf::ScanLimit { limit }),
            projection: vec![],
            schema: Some((&schema).try_into()?),
            table_partition_cols: self
                .table_partition_cols()
                .iter()
                .map(|x| x.name().clone())
                .collect::<Vec<_>>(),
            object_store_url: self.object_store_url.to_string(),
            output_ordering,
            constraints: Some(self.constraints.clone().into()),
            batch_size: self.batch_size.map(|s| s as u64),
            projection_exprs,
            output_partitioning,
            file_compression_type,
        })
    }

    /// Reconstruct a [`FileScanConfig`] from a [`protobuf::FileScanExecConf`]
    /// and a `file_source` the caller has already rebuilt (typically from the
    /// table schema via [`FileScanConfig::parse_table_schema_from_proto`]).
    ///
    /// Existing fields are byte-compatible with the former
    /// `parse_protobuf_file_scan_config`.
    pub fn try_from_proto(
        conf: &protobuf::FileScanExecConf,
        ctx: &ExecutionPlanDecodeCtx<'_>,
        file_source: Arc<dyn FileSource>,
    ) -> Result<FileScanConfig> {
        let schema = parse_file_scan_schema(conf)?;

        let constraints = conf
            .constraints
            .as_ref()
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "FileScanExecConf is missing required field 'constraints'"
                )
            })?
            .try_into()?;
        let statistics = conf
            .statistics
            .as_ref()
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "FileScanExecConf is missing required field 'statistics'"
                )
            })?
            .try_into()?;

        let file_groups = conf
            .file_groups
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;

        let object_store_url = match conf.object_store_url.is_empty() {
            false => ObjectStoreUrl::parse(&conf.object_store_url)?,
            true => ObjectStoreUrl::local_filesystem(),
        };

        let mut output_ordering = vec![];
        for node_collection in &conf.output_ordering {
            let sort_exprs = sort_exprs_try_from_proto(
                &node_collection.physical_sort_expr_nodes,
                &ctx.expr_ctx(&schema),
            )?;
            output_ordering.extend(LexOrdering::new(sort_exprs));
        }

        let output_partitioning = conf
            .output_partitioning
            .as_ref()
            .map(|partitioning| {
                Partitioning::try_from_proto(partitioning, &ctx.expr_ctx(&schema))
            })
            .transpose()?
            .flatten();

        let file_compression_type = conf
            .file_compression_type
            .map(|value| {
                let compression =
                    ProtoCompressionTypeVariant::try_from(value).map_err(|_| {
                        internal_datafusion_err!("Unknown file compression type: {value}")
                    })?;
                let compression: CompressionTypeVariant = compression.into();
                Ok::<_, DataFusionError>(FileCompressionType::from(compression))
            })
            .transpose()?
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        // Parse projection expressions if present and apply to the file source.
        let file_source = if let Some(proto_projection_exprs) = &conf.projection_exprs {
            let projection_exprs: Vec<ProjectionExpr> = proto_projection_exprs
                .projections
                .iter()
                .map(|proto_expr| {
                    let expr = ctx.decode_expr(
                        proto_expr.expr.as_ref().ok_or_else(|| {
                            internal_datafusion_err!("ProjectionExpr missing expr field")
                        })?,
                        &schema,
                    )?;
                    Ok(ProjectionExpr::new(expr, proto_expr.alias.clone()))
                })
                .collect::<Result<Vec<_>>>()?;

            let projection_exprs = ProjectionExprs::new(projection_exprs);

            file_source
                .try_pushdown_projection(&projection_exprs)?
                .unwrap_or(file_source)
        } else {
            file_source
        };

        let limit = conf
            .limit
            .as_ref()
            .map(|limit| usize_from_wire(limit.limit, "FileScanConfig", "limit"))
            .transpose()?;
        let batch_size = conf
            .batch_size
            .map(|size| usize_from_wire(size, "FileScanConfig", "batch_size"))
            .transpose()?;
        if batch_size == Some(0) {
            return datafusion_common::plan_err!(
                "FileScanConfig: batch_size must be greater than 0"
            );
        }
        let config_builder = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file_groups(file_groups)
            .with_constraints(constraints)
            .with_statistics(statistics)
            .with_limit(limit)
            .with_output_ordering(output_ordering)
            .with_output_partitioning(output_partitioning)
            .with_batch_size(batch_size)
            .with_file_compression_type(file_compression_type);
        Ok(config_builder.build())
    }

    /// Parse a [`TableSchema`] (file schema + partition columns) from a
    /// [`protobuf::FileScanExecConf`]. File sources use this to rebuild their
    /// concrete source before calling [`FileScanConfig::try_from_proto`].
    ///
    /// Byte-compatible with the former `parse_table_schema_from_proto`.
    pub fn parse_table_schema_from_proto(
        conf: &protobuf::FileScanExecConf,
    ) -> Result<TableSchema> {
        let schema = parse_file_scan_schema(conf)?;

        // Reacquire the partition column types from the schema before removing
        // them below.
        let table_partition_cols = conf
            .table_partition_cols
            .iter()
            .map(|col| Ok(Arc::new(schema.field_with_name(col)?.clone())))
            .collect::<Result<Vec<_>>>()?;

        // Remove partition columns from the schema after recreating
        // table_partition_cols because the partition columns are not in the
        // file. They are present to allow the partition column types to be
        // reconstructed after serde.
        let file_schema = Arc::new(
            Schema::new(
                schema
                    .fields()
                    .iter()
                    .filter(|field| !table_partition_cols.contains(field))
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .with_metadata(schema.metadata.clone()),
        );

        Ok(TableSchema::builder(file_schema)
            .with_table_partition_cols(table_partition_cols)
            .build())
    }
}

/// Parse the full (file + partition columns) schema off the base conf.
fn parse_file_scan_schema(conf: &protobuf::FileScanExecConf) -> Result<Arc<Schema>> {
    let schema: Schema = conf
        .schema
        .as_ref()
        .ok_or_else(|| {
            internal_datafusion_err!(
                "FileScanExecConf is missing required field 'schema'"
            )
        })?
        .try_into()?;
    Ok(Arc::new(schema))
}
