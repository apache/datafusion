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
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr_common::sort_expr::{
    optional_ordering_try_from_proto, sort_exprs_try_to_proto,
};
use datafusion_physical_plan::proto::{ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx};
use datafusion_proto_models::datafusion_common::{
    CompressionTypeVariant as ProtoCompressionTypeVariant, Schema as ProtoSchema,
};
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
        // Exhaustive destructure: adding a field to `FileScanConfig` without
        // deciding how it is serialized is a compile error, not a silent
        // round-trip gap.
        let Self {
            object_store_url,
            file_groups,
            constraints,
            limit,
            preserve_order,
            output_ordering,
            file_compression_type,
            file_source,
            batch_size,
            expr_adapter_factory,
            // Serialized through `statistics()` so its pushed-filter policy
            // remains centralized.
            statistics: _,
            output_partitioning,
        } = self;

        // Non-default factories are executable behavior with no protobuf
        // representation; silently replacing one can change scan results.
        if expr_adapter_factory
            .as_ref()
            .is_some_and(|factory| !factory.is_equivalent_to_default())
        {
            return datafusion_common::not_impl_err!(
                "FileScanConfig with a non-default expr_adapter_factory cannot be serialized"
            );
        }

        let proto_file_groups = file_groups
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;

        let mut proto_output_ordering = vec![];
        for order in output_ordering {
            let nodes = sort_exprs_try_to_proto(order.iter(), &ctx.expr_ctx())?;
            proto_output_ordering.push(protobuf::PhysicalSortExprNodeCollection {
                physical_sort_expr_nodes: nodes,
            });
        }

        let proto_output_partitioning = output_partitioning
            .as_ref()
            .map(|partitioning| partitioning.try_to_proto(&ctx.expr_ctx()))
            .transpose()?;

        let table_schema = file_source.table_schema();
        let file_schema = table_schema.file_schema();
        let table_partition_cols = table_schema.table_partition_cols();

        // Partition fields must be added to the schema so they can persist in
        // protobuf and then be removed again in `parse_table_schema_from_proto`.
        let mut fields = file_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.extend(table_partition_cols.iter().cloned());
        let schema = Schema::new(fields).with_metadata(file_schema.metadata.clone());

        let projection_exprs = file_source
            .projection()
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

        let proto_file_compression_type =
            file_compression_type.is_compressed().then(|| {
                let compression: ProtoCompressionTypeVariant =
                    (*file_compression_type.get_variant()).into();
                compression as i32
            });
        let statistics = self.statistics();

        Ok(protobuf::FileScanExecConf {
            file_groups: proto_file_groups,
            statistics: Some((&statistics).into()),
            limit: limit
                .map(|limit| usize_to_wire::<u32>(limit, "FileScanConfig", "limit"))
                .transpose()?
                .map(|limit| protobuf::ScanLimit { limit }),
            // Superseded by `projection_exprs`; kept empty for wire compatibility.
            projection: vec![],
            schema: Some((&schema).try_into()?),
            table_partition_cols: table_partition_cols
                .iter()
                .map(|x| x.name().clone())
                .collect::<Vec<_>>(),
            object_store_url: object_store_url.to_string(),
            output_ordering: proto_output_ordering,
            constraints: Some(constraints.clone().into()),
            batch_size: batch_size.map(|size| size as u64),
            projection_exprs,
            output_partitioning: proto_output_partitioning,
            file_compression_type: proto_file_compression_type,
            preserve_order: Some(*preserve_order),
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
        // Destructure exhaustively so a newly added protobuf field must be
        // handled here instead of being silently ignored.
        let protobuf::FileScanExecConf {
            file_groups,
            schema: proto_schema,
            // Superseded by `projection_exprs`; current encoders leave this
            // legacy column-index projection empty.
            projection: _,
            limit,
            statistics,
            // Used to construct `file_source` via `parse_table_schema_from_proto`
            // before this hook is called.
            table_partition_cols: _,
            object_store_url,
            output_ordering,
            constraints,
            batch_size,
            projection_exprs,
            output_partitioning,
            file_compression_type,
            preserve_order,
        } = conf;

        let expression_schema = parse_file_scan_schema(proto_schema)?;

        let decoded_constraints = constraints
            .as_ref()
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "FileScanExecConf is missing required field 'constraints'"
                )
            })?
            .try_into()?;
        let decoded_statistics = statistics
            .as_ref()
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "FileScanExecConf is missing required field 'statistics'"
                )
            })?
            .try_into()?;

        let decoded_file_groups = file_groups
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;

        let decoded_object_store_url = match object_store_url.is_empty() {
            false => ObjectStoreUrl::parse(object_store_url)?,
            true => ObjectStoreUrl::local_filesystem(),
        };

        let mut decoded_output_ordering = vec![];
        for node_collection in output_ordering {
            let protobuf::PhysicalSortExprNodeCollection {
                physical_sort_expr_nodes,
            } = node_collection;
            if let Some(ordering) = optional_ordering_try_from_proto(
                physical_sort_expr_nodes,
                &ctx.expr_ctx(&expression_schema),
            )? {
                decoded_output_ordering.push(ordering);
            }
        }

        let decoded_output_partitioning = output_partitioning
            .as_ref()
            .map(|partitioning| {
                Partitioning::try_from_proto(
                    partitioning,
                    &ctx.expr_ctx(&expression_schema),
                )
            })
            .transpose()?
            .flatten();

        let decoded_file_compression_type = file_compression_type
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
        let decoded_file_source = if let Some(proto_projection_exprs) = projection_exprs {
            let protobuf::ProjectionExprs { projections } = proto_projection_exprs;
            let decoded_projection_exprs: Vec<ProjectionExpr> = projections
                .iter()
                .map(|proto_expr| {
                    let protobuf::ProjectionExpr { alias, expr } = proto_expr;
                    let expr = ctx.decode_expr(
                        expr.as_ref().ok_or_else(|| {
                            internal_datafusion_err!("ProjectionExpr missing expr field")
                        })?,
                        &expression_schema,
                    )?;
                    Ok(ProjectionExpr::new(expr, alias.clone()))
                })
                .collect::<Result<Vec<_>>>()?;

            let projection = ProjectionExprs::new(decoded_projection_exprs);

            file_source
                .try_pushdown_projection(&projection)?
                .unwrap_or(file_source)
        } else {
            file_source
        };

        let decoded_limit = limit
            .as_ref()
            .map(|limit| {
                let protobuf::ScanLimit { limit } = limit;
                usize_from_wire(*limit, "FileScanConfig", "limit")
            })
            .transpose()?;
        let decoded_batch_size = batch_size
            .map(|size| usize_from_wire(size, "FileScanConfig", "batch_size"))
            .transpose()?;
        if decoded_batch_size == Some(0) {
            return datafusion_common::plan_err!(
                "FileScanConfig: batch_size must be greater than 0"
            );
        }
        let mut config =
            FileScanConfigBuilder::new(decoded_object_store_url, decoded_file_source)
                .with_file_groups(decoded_file_groups)
                .with_constraints(decoded_constraints)
                .with_statistics(decoded_statistics)
                .with_limit(decoded_limit)
                .with_output_ordering(decoded_output_ordering)
                .with_output_partitioning(decoded_output_partitioning)
                .with_batch_size(decoded_batch_size)
                .with_file_compression_type(decoded_file_compression_type)
                .build();

        // Presence distinguishes a new explicit `false` from a legacy payload,
        // which must retain the builder's ordering-derived behavior.
        if let Some(preserve_order) = preserve_order {
            config.preserve_order = *preserve_order;
        }
        Ok(config)
    }

    /// Parse a [`TableSchema`] (file schema + partition columns) from a
    /// [`protobuf::FileScanExecConf`]. File sources use this to rebuild their
    /// concrete source before calling [`FileScanConfig::try_from_proto`].
    ///
    /// Byte-compatible with the former `parse_table_schema_from_proto`.
    pub fn parse_table_schema_from_proto(
        conf: &protobuf::FileScanExecConf,
    ) -> Result<TableSchema> {
        let schema = parse_file_scan_schema(&conf.schema)?;

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
fn parse_file_scan_schema(schema: &Option<ProtoSchema>) -> Result<Arc<Schema>> {
    let proto_schema = schema.as_ref().ok_or_else(|| {
        internal_datafusion_err!("FileScanExecConf is missing required field 'schema'")
    })?;
    Ok(Arc::new(proto_schema.try_into()?))
}
