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
//! ride the [`ExecutionPlanEncodeCtx`] / [`ExecutionPlanDecodeCtx`] instead of
//! the raw `PhysicalExtensionCodec` + `PhysicalProtoConverterExtension`. Every
//! `FileSource::try_to_proto` hook (CSV, JSON, Arrow, Parquet, Avro) builds its
//! `*ScanExecNode` around [`FileScanConfig::to_proto_conf`] and decodes with
//! [`FileScanConfig::from_proto_conf`], keeping a single copy of the shared
//! wire logic. The wire format is byte-for-byte identical to the old central
//! serializer.
//!
//! Child physical expressions (sort orderings, hash/range partitioning, and
//! projection expressions) are (de)serialized through `ctx.encode_expr` /
//! `ctx.decode_expr`; `Schema`, `Statistics`, `Constraints`, and `ScalarValue`
//! go through `datafusion-proto-common`. Nothing here needs the raw codec.

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use chrono::{TimeZone, Utc};
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr::{
    LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning, SplitPoint,
};
use datafusion_physical_plan::proto::{ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx};
use datafusion_proto_models::protobuf;
use object_store::ObjectMeta;
use object_store::path::Path;

use crate::PartitionedFile;
use crate::file::FileSource;
use crate::file_groups::FileGroup;
use crate::file_scan_config::{
    FileScanConfig, FileScanConfigBuilder, output_partitioning_from_partition_fields,
};
use crate::table_schema::TableSchema;

impl FileScanConfig {
    /// Serialize the shared, format-agnostic part of a file scan into a
    /// [`protobuf::FileScanExecConf`].
    ///
    /// Each concrete [`FileSource::try_to_proto`]
    /// wraps the returned value in its own `*ScanExecNode`. Byte-compatible with
    /// the former `serialize_file_scan_config` in `datafusion-proto`.
    pub fn to_proto_conf(
        &self,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<protobuf::FileScanExecConf> {
        let file_groups = self
            .file_groups
            .iter()
            .map(file_group_to_proto)
            .collect::<Result<Vec<_>>>()?;

        // Sort orderings: only the child expressions need the ctx; the
        // asc/nulls_first wrapping is plain data inlined into a
        // `PhysicalSortExprNode` (same shape as `sorts/sort.rs`).
        let mut output_ordering = vec![];
        for order in &self.output_ordering {
            let nodes = order
                .iter()
                .map(|sort_expr| {
                    Ok(protobuf::PhysicalSortExprNode {
                        expr: Some(Box::new(ctx.encode_expr(&sort_expr.expr)?)),
                        asc: !sort_expr.options.descending,
                        nulls_first: sort_expr.options.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            output_ordering.push(protobuf::PhysicalSortExprNodeCollection {
                physical_sort_expr_nodes: nodes,
            });
        }

        let output_partitioning = self
            .output_partitioning
            .as_ref()
            .map(|p| partitioning_to_proto(p, ctx))
            .transpose()?;

        // Fields must be added to the schema so that they can persist in the
        // protobuf, and then removed from the schema in `from_proto_conf`.
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

        Ok(protobuf::FileScanExecConf {
            file_groups,
            statistics: Some((&self.statistics()).into()),
            limit: self.limit.map(|l| protobuf::ScanLimit { limit: l as u32 }),
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
            // Partition grouping is now encoded in `output_partitioning`; this
            // legacy wire field is left unset (readers rely on
            // `output_partitioning`).
            partitioned_by_file_group: None,
            output_partitioning,
        })
    }

    /// Reconstruct a [`FileScanConfig`] from a [`protobuf::FileScanExecConf`]
    /// and a `file_source` the caller has already rebuilt (typically from the
    /// table schema via [`FileScanConfig::parse_table_schema_from_proto`]).
    ///
    /// Byte-compatible with the former `parse_protobuf_file_scan_config`.
    pub fn from_proto_conf(
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
            .map(file_group_from_proto)
            .collect::<Result<Vec<_>>>()?;

        let object_store_url = match conf.object_store_url.is_empty() {
            false => ObjectStoreUrl::parse(&conf.object_store_url)?,
            true => ObjectStoreUrl::local_filesystem(),
        };

        let mut output_ordering = vec![];
        for node_collection in &conf.output_ordering {
            let sort_exprs = parse_sort_exprs(
                &node_collection.physical_sort_expr_nodes,
                ctx,
                &schema,
            )?;
            output_ordering.extend(LexOrdering::new(sort_exprs));
        }

        let output_partitioning =
            partitioning_from_proto(conf.output_partitioning.as_ref(), ctx, &schema)?;
        let output_partitioning = match output_partitioning {
            Some(output_partitioning) => Some(output_partitioning),
            None if conf.partitioned_by_file_group.unwrap_or(false) => {
                // Backward compatibility: older serialized plans used only
                // `partitioned_by_file_group` to declare scan output partitioning.
                let table_schema = Self::parse_table_schema_from_proto(conf)?;
                output_partitioning_from_partition_fields(
                    &schema,
                    table_schema.table_partition_cols(),
                    file_groups.len(),
                )
            }
            None => None,
        };

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

        let config_builder = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file_groups(file_groups)
            .with_constraints(constraints)
            .with_statistics(statistics)
            .with_limit(conf.limit.as_ref().map(|sl| sl.limit as usize))
            .with_output_ordering(output_ordering)
            .with_output_partitioning(output_partitioning)
            .with_batch_size(conf.batch_size.map(|s| s as usize));
        Ok(config_builder.build())
    }

    /// Parse a [`TableSchema`] (file schema + partition columns) from a
    /// [`protobuf::FileScanExecConf`]. File sources use this to rebuild their
    /// concrete source before calling [`FileScanConfig::from_proto_conf`].
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

fn parse_sort_exprs(
    nodes: &[protobuf::PhysicalSortExprNode],
    ctx: &ExecutionPlanDecodeCtx<'_>,
    schema: &Schema,
) -> Result<Vec<PhysicalSortExpr>> {
    nodes
        .iter()
        .map(|sort_expr| {
            let expr = sort_expr.expr.as_ref().ok_or_else(|| {
                internal_datafusion_err!("Unexpected empty physical expression")
            })?;
            Ok(PhysicalSortExpr {
                expr: ctx.decode_expr(expr, schema)?,
                options: SortOptions {
                    descending: !sort_expr.asc,
                    nulls_first: sort_expr.nulls_first,
                },
            })
        })
        .collect()
}

/// Inlined equivalent of `datafusion-proto`'s `serialize_partitioning`. Only
/// child physical expressions and `ScalarValue`s need the ctx; the
/// `protobuf::Partitioning` wrapping is built directly here.
fn partitioning_to_proto(
    partitioning: &Partitioning,
    ctx: &ExecutionPlanEncodeCtx<'_>,
) -> Result<protobuf::Partitioning> {
    let partition_method = match partitioning {
        Partitioning::RoundRobinBatch(n) => {
            protobuf::partitioning::PartitionMethod::RoundRobin(*n as u64)
        }
        Partitioning::Hash(exprs, n) => {
            let hash_expr = ctx.encode_expressions(exprs)?;
            protobuf::partitioning::PartitionMethod::Hash(
                protobuf::PhysicalHashRepartition {
                    hash_expr,
                    partition_count: *n as u64,
                },
            )
        }
        Partitioning::Range(range) => {
            let sort_expr = range
                .ordering()
                .iter()
                .map(|sort_expr| {
                    Ok(protobuf::PhysicalSortExprNode {
                        expr: Some(Box::new(ctx.encode_expr(&sort_expr.expr)?)),
                        asc: !sort_expr.options.descending,
                        nulls_first: sort_expr.options.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let split_point = range
                .split_points()
                .iter()
                .map(|split_point| {
                    let value = split_point
                        .values()
                        .iter()
                        .map(|value| value.try_into().map_err(Into::into))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(protobuf::PhysicalRangeSplitPoint { value })
                })
                .collect::<Result<Vec<_>>>()?;
            protobuf::partitioning::PartitionMethod::Range(
                protobuf::PhysicalRangePartitioning {
                    sort_expr,
                    split_point,
                },
            )
        }
        Partitioning::UnknownPartitioning(n) => {
            protobuf::partitioning::PartitionMethod::Unknown(*n as u64)
        }
    };
    Ok(protobuf::Partitioning {
        partition_method: Some(partition_method),
    })
}

/// Inlined equivalent of `datafusion-proto`'s `parse_protobuf_partitioning`.
fn partitioning_from_proto(
    partitioning: Option<&protobuf::Partitioning>,
    ctx: &ExecutionPlanDecodeCtx<'_>,
    schema: &Schema,
) -> Result<Option<Partitioning>> {
    let Some(partitioning) = partitioning else {
        return Ok(None);
    };
    let Some(partition_method) = partitioning.partition_method.as_ref() else {
        return Ok(None);
    };
    let partitioning = match partition_method {
        protobuf::partitioning::PartitionMethod::RoundRobin(n) => {
            Partitioning::RoundRobinBatch(*n as usize)
        }
        protobuf::partitioning::PartitionMethod::Hash(hash) => {
            let exprs = hash
                .hash_expr
                .iter()
                .map(|expr| ctx.decode_expr(expr, schema))
                .collect::<Result<Vec<_>>>()?;
            Partitioning::Hash(exprs, hash.partition_count as usize)
        }
        protobuf::partitioning::PartitionMethod::Unknown(n) => {
            Partitioning::UnknownPartitioning(*n as usize)
        }
        protobuf::partitioning::PartitionMethod::Range(range) => {
            let sort_exprs = parse_sort_exprs(&range.sort_expr, ctx, schema)?;
            let sort_expr_count = sort_exprs.len();
            let ordering = LexOrdering::new(sort_exprs).ok_or_else(|| {
                internal_datafusion_err!("Range partitioning requires non-empty ordering")
            })?;
            if ordering.len() != sort_expr_count {
                return Err(internal_datafusion_err!(
                    "Range partitioning ordering must not contain duplicate expressions"
                ));
            }
            let split_points = range
                .split_point
                .iter()
                .map(|split_point| {
                    let values = split_point
                        .value
                        .iter()
                        .map(|value| {
                            datafusion_common::ScalarValue::try_from(value)
                                .map_err(Into::into)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(SplitPoint::new(values))
                })
                .collect::<Result<Vec<_>>>()?;
            Partitioning::Range(RangePartitioning::try_new(ordering, split_points)?)
        }
    };
    Ok(Some(partitioning))
}

fn file_group_to_proto(group: &FileGroup) -> Result<protobuf::FileGroup> {
    Ok(protobuf::FileGroup {
        files: group
            .files()
            .iter()
            .map(partitioned_file_to_proto)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn file_group_from_proto(group: &protobuf::FileGroup) -> Result<FileGroup> {
    let files = group
        .files
        .iter()
        .map(partitioned_file_from_proto)
        .collect::<Result<Vec<_>>>()?;
    Ok(FileGroup::new(files))
}

fn partitioned_file_to_proto(pf: &PartitionedFile) -> Result<protobuf::PartitionedFile> {
    let last_modified = pf.object_meta.last_modified;
    let last_modified_ns = last_modified.timestamp_nanos_opt().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "Invalid timestamp on PartitionedFile::ObjectMeta: {last_modified}"
        ))
    })? as u64;
    Ok(protobuf::PartitionedFile {
        arrow_schema: pf
            .arrow_schema
            .as_ref()
            .map(|s| s.as_ref().try_into())
            .transpose()?,
        path: pf.object_meta.location.as_ref().to_owned(),
        size: pf.object_meta.size,
        last_modified_ns,
        partition_values: pf
            .partition_values
            .iter()
            .map(|v| v.try_into())
            .collect::<Result<Vec<_>, _>>()?,
        range: pf.range.as_ref().map(|range| protobuf::FileRange {
            start: range.start,
            end: range.end,
        }),
        statistics: pf.statistics.as_ref().map(|s| s.as_ref().into()),
    })
}

fn partitioned_file_from_proto(
    val: &protobuf::PartitionedFile,
) -> Result<PartitionedFile> {
    let mut pf = PartitionedFile::new_from_meta(ObjectMeta {
        location: Path::parse(val.path.as_str())
            .map_err(|e| internal_datafusion_err!("Invalid object_store path: {e}"))?,
        last_modified: Utc.timestamp_nanos(val.last_modified_ns as i64),
        size: val.size,
        e_tag: None,
        version: None,
    })
    .with_partition_values(
        val.partition_values
            .iter()
            .map(|v| v.try_into())
            .collect::<Result<Vec<_>, _>>()?,
    );
    if let Some(proto_schema) = val.arrow_schema.as_ref() {
        pf = pf.with_arrow_schema(Arc::new(
            proto_schema.try_into().map_err(DataFusionError::from)?,
        ));
    }
    if let Some(range) = val.range.as_ref() {
        pf = pf.with_range(range.start, range.end);
    }
    if let Some(proto_stats) = val.statistics.as_ref() {
        pf = pf.with_statistics(Arc::new(proto_stats.try_into()?));
    }
    Ok(pf)
}
