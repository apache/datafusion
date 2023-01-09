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

use crate::common::{byte_to_string, proto_error, str_to_byte};
use crate::protobuf::logical_plan_node::LogicalPlanType::CustomScan;
use crate::protobuf::CustomTableScanNode;
use crate::{
    convert_required,
    protobuf::{
        self, listing_table_scan_node::FileFormatType,
        logical_plan_node::LogicalPlanType, LogicalExtensionNode, LogicalPlanNode,
    },
};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::{
    datasource::{
        file_format::{
            avro::AvroFormat, csv::CsvFormat, parquet::ParquetFormat, FileFormat,
        },
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        view::ViewTable,
        TableProvider,
    },
    datasource::{provider_as_source, source_as_provider},
    prelude::SessionContext,
};
use datafusion_common::{
    context, parsers::CompressionTypeVariant, DataFusionError, OwnedTableReference,
};
use datafusion_expr::{
    logical_plan::{
        builder::project, Aggregate, CreateCatalog, CreateCatalogSchema,
        CreateExternalTable, CreateView, CrossJoin, Distinct, EmptyRelation, Extension,
        Join, JoinConstraint, Limit, Prepare, Projection, Repartition, Sort,
        SubqueryAlias, TableScan, Values, Window,
    },
    Expr, LogicalPlan, LogicalPlanBuilder,
};
use prost::bytes::BufMut;
use prost::Message;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

pub mod from_proto;
pub mod to_proto;

impl From<from_proto::Error> for DataFusionError {
    fn from(e: from_proto::Error) -> Self {
        DataFusionError::Plan(e.to_string())
    }
}

impl From<to_proto::Error> for DataFusionError {
    fn from(e: to_proto::Error) -> Self {
        DataFusionError::Plan(e.to_string())
    }
}

pub trait AsLogicalPlan: Debug + Send + Sync + Clone {
    fn try_decode(buf: &[u8]) -> Result<Self, DataFusionError>
    where
        Self: Sized;

    fn try_encode<B>(&self, buf: &mut B) -> Result<(), DataFusionError>
    where
        B: BufMut,
        Self: Sized;

    fn try_into_logical_plan(
        &self,
        ctx: &SessionContext,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<LogicalPlan, DataFusionError>;

    fn try_from_logical_plan(
        plan: &LogicalPlan,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<Self, DataFusionError>
    where
        Self: Sized;
}

pub trait LogicalExtensionCodec: Debug + Send + Sync {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> Result<Extension, DataFusionError>;

    fn try_encode(
        &self,
        node: &Extension,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError>;

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        schema: SchemaRef,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError>;

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError>;
}

#[derive(Debug, Clone)]
pub struct DefaultLogicalExtensionCodec {}

impl LogicalExtensionCodec for DefaultLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> Result<Extension, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "LogicalExtensionCodec is not provided".to_string(),
        ))
    }

    fn try_encode(
        &self,
        _node: &Extension,
        _buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "LogicalExtensionCodec is not provided".to_string(),
        ))
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "LogicalExtensionCodec is not provided".to_string(),
        ))
    }

    fn try_encode_table_provider(
        &self,
        _node: Arc<dyn TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "LogicalExtensionCodec is not provided".to_string(),
        ))
    }
}

#[macro_export]
macro_rules! into_logical_plan {
    ($PB:expr, $CTX:expr, $CODEC:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into_logical_plan($CTX, $CODEC)
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

fn from_owned_table_reference(
    table_ref: Option<&protobuf::OwnedTableReference>,
    error_context: &str,
) -> Result<OwnedTableReference, DataFusionError> {
    let table_ref = table_ref.ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Protobuf deserialization error, {error_context} was missing required field name."
        ))
    })?;

    Ok(table_ref.clone().try_into()?)
}

impl AsLogicalPlan for LogicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self, DataFusionError>
    where
        Self: Sized,
    {
        LogicalPlanNode::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to decode logical plan: {e:?}"))
        })
    }

    fn try_encode<B>(&self, buf: &mut B) -> Result<(), DataFusionError>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to encode logical plan: {e:?}"))
        })
    }

    fn try_into_logical_plan(
        &self,
        ctx: &SessionContext,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<LogicalPlan, DataFusionError> {
        let plan = self.logical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "logical_plan::from_proto() Unsupported logical plan '{self:?}'"
            ))
        })?;
        match plan {
            LogicalPlanType::Values(values) => {
                let n_cols = values.n_cols as usize;
                let values: Vec<Vec<Expr>> = if values.values_list.is_empty() {
                    Ok(Vec::new())
                } else if values.values_list.len() % n_cols != 0 {
                    Err(DataFusionError::Internal(format!(
                        "Invalid values list length, expect {} to be divisible by {}",
                        values.values_list.len(),
                        n_cols
                    )))
                } else {
                    values
                        .values_list
                        .chunks_exact(n_cols)
                        .map(|r| {
                            r.iter()
                                .map(|expr| from_proto::parse_expr(expr, ctx))
                                .collect::<Result<Vec<_>, from_proto::Error>>()
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| e.into())
                }?;
                LogicalPlanBuilder::values(values)?.build()
            }
            LogicalPlanType::Projection(projection) => {
                let input: LogicalPlan =
                    into_logical_plan!(projection.input, ctx, extension_codec)?;
                let expr: Vec<Expr> = projection
                    .expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;

                let new_proj = project(input, expr)?;
                match projection.optional_alias.as_ref() {
                    Some(a) => match a {
                        protobuf::projection_node::OptionalAlias::Alias(alias) => {
                            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                                new_proj, alias,
                            )?))
                        }
                    },
                    _ => Ok(new_proj),
                }
            }
            LogicalPlanType::Selection(selection) => {
                let input: LogicalPlan =
                    into_logical_plan!(selection.input, ctx, extension_codec)?;
                let expr: Expr = selection
                    .expr
                    .as_ref()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .transpose()?
                    .ok_or_else(|| {
                        DataFusionError::Internal("expression required".to_string())
                    })?;
                // .try_into()?;
                LogicalPlanBuilder::from(input).filter(expr)?.build()
            }
            LogicalPlanType::Window(window) => {
                let input: LogicalPlan =
                    into_logical_plan!(window.input, ctx, extension_codec)?;
                let window_expr = window
                    .window_expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input).window(window_expr)?.build()
            }
            LogicalPlanType::Aggregate(aggregate) => {
                let input: LogicalPlan =
                    into_logical_plan!(aggregate.input, ctx, extension_codec)?;
                let group_expr = aggregate
                    .group_expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                let aggr_expr = aggregate
                    .aggr_expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .aggregate(group_expr, aggr_expr)?
                    .build()
            }
            LogicalPlanType::ListingScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;

                let mut projection = None;
                if let Some(columns) = &scan.projection {
                    let column_indices = columns
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                let filters = scan
                    .filters
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;

                let file_sort_order = scan
                    .file_sort_order
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;

                // Protobuf doesn't distinguish between "not present"
                // and empty
                let file_sort_order = if file_sort_order.is_empty() {
                    None
                } else {
                    Some(file_sort_order)
                };

                let file_format: Arc<dyn FileFormat> =
                    match scan.file_format_type.as_ref().ok_or_else(|| {
                        proto_error(format!(
                            "logical_plan::from_proto() Unsupported file format '{self:?}'"
                        ))
                    })? {
                        &FileFormatType::Parquet(protobuf::ParquetFormat {}) => {
                            Arc::new(ParquetFormat::default())
                        }
                        FileFormatType::Csv(protobuf::CsvFormat {
                            has_header,
                            delimiter,
                        }) => Arc::new(
                            CsvFormat::default()
                                .with_has_header(*has_header)
                                .with_delimiter(str_to_byte(delimiter)?),
                        ),
                        FileFormatType::Avro(..) => Arc::new(AvroFormat::default()),
                    };

                let table_paths = &scan
                    .paths
                    .iter()
                    .map(ListingTableUrl::parse)
                    .collect::<Result<Vec<_>, _>>()?;

                let options = ListingOptions::new(file_format)
                    .with_file_extension(scan.file_extension.clone())
                    .with_table_partition_cols(
                        scan.table_partition_cols
                            .iter()
                            .map(|col| {
                                (
                                    col.clone(),
                                    schema
                                        .field_with_name(col)
                                        .unwrap()
                                        .data_type()
                                        .clone(),
                                )
                            })
                            .collect(),
                    )
                    .with_collect_stat(scan.collect_stat)
                    .with_target_partitions(scan.target_partitions as usize)
                    .with_file_sort_order(file_sort_order);

                let config =
                    ListingTableConfig::new_with_multi_paths(table_paths.clone())
                        .with_listing_options(options)
                        .with_schema(Arc::new(schema));

                let provider = ListingTable::try_new(config)?;

                LogicalPlanBuilder::scan_with_filters(
                    &scan.table_name,
                    provider_as_source(Arc::new(provider)),
                    projection,
                    filters,
                )?
                .build()
            }
            LogicalPlanType::CustomScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;
                let schema = Arc::new(schema);
                let mut projection = None;
                if let Some(columns) = &scan.projection {
                    let column_indices = columns
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                let filters = scan
                    .filters
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                let provider = extension_codec.try_decode_table_provider(
                    &scan.custom_table_data,
                    schema,
                    ctx,
                )?;

                LogicalPlanBuilder::scan_with_filters(
                    &scan.table_name,
                    provider_as_source(provider),
                    projection,
                    filters,
                )?
                .build()
            }
            LogicalPlanType::Sort(sort) => {
                let input: LogicalPlan =
                    into_logical_plan!(sort.input, ctx, extension_codec)?;
                let sort_expr: Vec<Expr> = sort
                    .expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input).sort(sort_expr)?.build()
            }
            LogicalPlanType::Repartition(repartition) => {
                use datafusion::logical_expr::Partitioning;
                let input: LogicalPlan =
                    into_logical_plan!(repartition.input, ctx, extension_codec)?;
                use protobuf::repartition_node::PartitionMethod;
                let pb_partition_method = repartition.partition_method.clone().ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, RepartitionNode was missing required field 'partition_method'",
                    ))
                })?;

                let partitioning_scheme = match pb_partition_method {
                    PartitionMethod::Hash(protobuf::HashRepartition {
                        hash_expr: pb_hash_expr,
                        partition_count,
                    }) => Partitioning::Hash(
                        pb_hash_expr
                            .iter()
                            .map(|expr| from_proto::parse_expr(expr, ctx))
                            .collect::<Result<Vec<_>, _>>()?,
                        partition_count as usize,
                    ),
                    PartitionMethod::RoundRobin(partition_count) => {
                        Partitioning::RoundRobinBatch(partition_count as usize)
                    }
                };

                LogicalPlanBuilder::from(input)
                    .repartition(partitioning_scheme)?
                    .build()
            }
            LogicalPlanType::EmptyRelation(empty_relation) => {
                LogicalPlanBuilder::empty(empty_relation.produce_one_row).build()
            }
            LogicalPlanType::CreateExternalTable(create_extern_table) => {
                let pb_schema = (create_extern_table.schema.clone()).ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, CreateExternalTableNode was missing required field schema.",
                    ))
                })?;

                let definition = if !create_extern_table.definition.is_empty() {
                    Some(create_extern_table.definition.clone())
                } else {
                    None
                };

                let file_type = create_extern_table.file_type.as_str();
                let env = ctx.runtime_env();
                if !env.table_factories.contains_key(file_type) {
                    Err(DataFusionError::Internal(format!(
                        "No TableProvider for file type: {file_type}"
                    )))?
                }

                Ok(LogicalPlan::CreateExternalTable(CreateExternalTable {
                    schema: pb_schema.try_into()?,
                    name: from_owned_table_reference(create_extern_table.name.as_ref(), "CreateExternalTable")?,
                    location: create_extern_table.location.clone(),
                    file_type: create_extern_table.file_type.clone(),
                    has_header: create_extern_table.has_header,
                    delimiter: create_extern_table.delimiter.chars().next().ok_or_else(|| {
                        DataFusionError::Internal(String::from("Protobuf deserialization error, unable to parse CSV delimiter"))
                    })?,
                    table_partition_cols: create_extern_table
                        .table_partition_cols
                        .clone(),
                    if_not_exists: create_extern_table.if_not_exists,
                    file_compression_type: CompressionTypeVariant::from_str(&create_extern_table.file_compression_type).map_err(|_| DataFusionError::NotImplemented(format!("Unsupported file compression type {}", create_extern_table.file_compression_type)))?,
                    definition,
                    options: create_extern_table.options.clone(),
                }))
            }
            LogicalPlanType::CreateView(create_view) => {
                let plan = create_view
                    .input.clone().ok_or_else(|| DataFusionError::Internal(String::from(
                    "Protobuf deserialization error, CreateViewNode has invalid LogicalPlan input.",
                )))?
                    .try_into_logical_plan(ctx, extension_codec)?;
                let definition = if !create_view.definition.is_empty() {
                    Some(create_view.definition.clone())
                } else {
                    None
                };

                Ok(LogicalPlan::CreateView(CreateView {
                    name: from_owned_table_reference(
                        create_view.name.as_ref(),
                        "CreateView",
                    )?,
                    input: Arc::new(plan),
                    or_replace: create_view.or_replace,
                    definition,
                }))
            }
            LogicalPlanType::CreateCatalogSchema(create_catalog_schema) => {
                let pb_schema = (create_catalog_schema.schema.clone()).ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, CreateCatalogSchemaNode was missing required field schema.",
                    ))
                })?;

                Ok(LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                    schema_name: create_catalog_schema.schema_name.clone(),
                    if_not_exists: create_catalog_schema.if_not_exists,
                    schema: pb_schema.try_into()?,
                }))
            }
            LogicalPlanType::CreateCatalog(create_catalog) => {
                let pb_schema = (create_catalog.schema.clone()).ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, CreateCatalogNode was missing required field schema.",
                    ))
                })?;

                Ok(LogicalPlan::CreateCatalog(CreateCatalog {
                    catalog_name: create_catalog.catalog_name.clone(),
                    if_not_exists: create_catalog.if_not_exists,
                    schema: pb_schema.try_into()?,
                }))
            }
            LogicalPlanType::Analyze(analyze) => {
                let input: LogicalPlan =
                    into_logical_plan!(analyze.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .explain(analyze.verbose, true)?
                    .build()
            }
            LogicalPlanType::Explain(explain) => {
                let input: LogicalPlan =
                    into_logical_plan!(explain.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .explain(explain.verbose, false)?
                    .build()
            }
            LogicalPlanType::SubqueryAlias(aliased_relation) => {
                let input: LogicalPlan =
                    into_logical_plan!(aliased_relation.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .alias(&aliased_relation.alias)?
                    .build()
            }
            LogicalPlanType::Limit(limit) => {
                let input: LogicalPlan =
                    into_logical_plan!(limit.input, ctx, extension_codec)?;
                let skip = limit.skip.max(0) as usize;

                let fetch = if limit.fetch < 0 {
                    None
                } else {
                    Some(limit.fetch as usize)
                };

                LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
            }
            LogicalPlanType::Join(join) => {
                let left_keys: Vec<Expr> = join
                    .left_join_key
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                let right_keys: Vec<Expr> = join
                    .right_join_key
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                let join_type =
                    protobuf::JoinType::from_i32(join.join_type).ok_or_else(|| {
                        proto_error(format!(
                            "Received a JoinNode message with unknown JoinType {}",
                            join.join_type
                        ))
                    })?;
                let join_constraint = protobuf::JoinConstraint::from_i32(
                    join.join_constraint,
                )
                .ok_or_else(|| {
                    proto_error(format!(
                        "Received a JoinNode message with unknown JoinConstraint {}",
                        join.join_constraint
                    ))
                })?;
                let filter: Option<Expr> = join
                    .filter
                    .as_ref()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .map_or(Ok(None), |v| v.map(Some))?;

                let builder = LogicalPlanBuilder::from(into_logical_plan!(
                    join.left,
                    ctx,
                    extension_codec
                )?);
                let builder = match join_constraint.into() {
                    JoinConstraint::On => builder.join_with_expr_keys(
                        into_logical_plan!(join.right, ctx, extension_codec)?,
                        join_type.into(),
                        (left_keys, right_keys),
                        filter,
                    )?,
                    JoinConstraint::Using => {
                        // The equijoin keys in using-join must be column.
                        let using_keys = left_keys
                            .into_iter()
                            .map(|key| key.try_into_col())
                            .collect::<Result<Vec<_>, _>>()?;
                        builder.join_using(
                            into_logical_plan!(join.right, ctx, extension_codec)?,
                            join_type.into(),
                            using_keys,
                        )?
                    }
                };

                builder.build()
            }
            LogicalPlanType::Union(union) => {
                let mut input_plans: Vec<LogicalPlan> = union
                    .inputs
                    .iter()
                    .map(|i| i.try_into_logical_plan(ctx, extension_codec))
                    .collect::<Result<_, DataFusionError>>()?;

                if input_plans.len() < 2 {
                    return  Err( DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, Union was require at least two input.",
                    )));
                }

                let first = input_plans.pop().ok_or_else(|| DataFusionError::Internal(String::from(
                    "Protobuf deserialization error, Union was require at least two input.",
                )))?;
                let mut builder = LogicalPlanBuilder::from(first);
                for plan in input_plans {
                    builder = builder.union(plan)?;
                }
                builder.build()
            }
            LogicalPlanType::CrossJoin(crossjoin) => {
                let left = into_logical_plan!(crossjoin.left, ctx, extension_codec)?;
                let right = into_logical_plan!(crossjoin.right, ctx, extension_codec)?;

                LogicalPlanBuilder::from(left).cross_join(right)?.build()
            }
            LogicalPlanType::Extension(LogicalExtensionNode { node, inputs }) => {
                let input_plans: Vec<LogicalPlan> = inputs
                    .iter()
                    .map(|i| i.try_into_logical_plan(ctx, extension_codec))
                    .collect::<Result<_, DataFusionError>>()?;

                let extension_node =
                    extension_codec.try_decode(node, &input_plans, ctx)?;
                Ok(LogicalPlan::Extension(extension_node))
            }
            LogicalPlanType::Distinct(distinct) => {
                let input: LogicalPlan =
                    into_logical_plan!(distinct.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input).distinct()?.build()
            }
            LogicalPlanType::ViewScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;

                let mut projection = None;
                if let Some(columns) = &scan.projection {
                    let column_indices = columns
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                let input: LogicalPlan =
                    into_logical_plan!(scan.input, ctx, extension_codec)?;

                let definition = if !scan.definition.is_empty() {
                    Some(scan.definition.clone())
                } else {
                    None
                };

                let provider = ViewTable::try_new(input, definition)?;

                LogicalPlanBuilder::scan(
                    &scan.table_name,
                    provider_as_source(Arc::new(provider)),
                    projection,
                )?
                .build()
            }
            LogicalPlanType::Prepare(prepare) => {
                let input: LogicalPlan =
                    into_logical_plan!(prepare.input, ctx, extension_codec)?;
                let data_types: Vec<DataType> = prepare
                    .data_types
                    .iter()
                    .map(DataType::try_from)
                    .collect::<Result<_, _>>()?;
                LogicalPlanBuilder::from(input)
                    .prepare(prepare.name.clone(), data_types)?
                    .build()
            }
        }
    }

    fn try_from_logical_plan(
        plan: &LogicalPlan,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<Self, DataFusionError>
    where
        Self: Sized,
    {
        match plan {
            LogicalPlan::Values(Values { values, .. }) => {
                let n_cols = if values.is_empty() {
                    0
                } else {
                    values[0].len()
                } as u64;
                let values_list = values
                    .iter()
                    .flatten()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Values(
                        protobuf::ValuesNode {
                            n_cols,
                            values_list,
                        },
                    )),
                })
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                filters,
                projection,
                ..
            }) => {
                let provider = source_as_provider(source)?;
                let schema = provider.schema();
                let source = provider.as_any();

                let projection = match projection {
                    None => None,
                    Some(columns) => {
                        let column_names = columns
                            .iter()
                            .map(|i| schema.field(*i).name().to_owned())
                            .collect();
                        Some(protobuf::ProjectionColumns {
                            columns: column_names,
                        })
                    }
                };
                let schema: protobuf::Schema = schema.as_ref().try_into()?;

                let filters: Vec<protobuf::LogicalExprNode> = filters
                    .iter()
                    .map(|filter| filter.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                if let Some(listing_table) = source.downcast_ref::<ListingTable>() {
                    let any = listing_table.options().format.as_any();
                    let file_format_type = if any.is::<ParquetFormat>() {
                        FileFormatType::Parquet(protobuf::ParquetFormat {})
                    } else if let Some(csv) = any.downcast_ref::<CsvFormat>() {
                        FileFormatType::Csv(protobuf::CsvFormat {
                            delimiter: byte_to_string(csv.delimiter())?,
                            has_header: csv.has_header(),
                        })
                    } else if any.is::<AvroFormat>() {
                        FileFormatType::Avro(protobuf::AvroFormat {})
                    } else {
                        return Err(proto_error(format!(
                            "Error converting file format, {:?} is invalid as a datafusion format.",
                            listing_table.options().format
                        )));
                    };

                    let options = listing_table.options();
                    let file_sort_order =
                        if let Some(file_sort_order) = &options.file_sort_order {
                            file_sort_order
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<protobuf::LogicalExprNode>, _>>()?
                        } else {
                            vec![]
                        };

                    Ok(protobuf::LogicalPlanNode {
                        logical_plan_type: Some(LogicalPlanType::ListingScan(
                            protobuf::ListingTableScanNode {
                                file_format_type: Some(file_format_type),
                                table_name: table_name.to_owned(),
                                collect_stat: options.collect_stat,
                                file_extension: options.file_extension.clone(),
                                table_partition_cols: options
                                    .table_partition_cols
                                    .iter()
                                    .map(|x| x.0.clone())
                                    .collect::<Vec<_>>(),
                                paths: listing_table
                                    .table_paths()
                                    .iter()
                                    .map(|x| x.to_string())
                                    .collect(),
                                schema: Some(schema),
                                projection,
                                filters,
                                target_partitions: options.target_partitions as u32,
                                file_sort_order,
                            },
                        )),
                    })
                } else if let Some(view_table) = source.downcast_ref::<ViewTable>() {
                    Ok(protobuf::LogicalPlanNode {
                        logical_plan_type: Some(LogicalPlanType::ViewScan(Box::new(
                            protobuf::ViewTableScanNode {
                                table_name: table_name.to_owned(),
                                input: Some(Box::new(
                                    protobuf::LogicalPlanNode::try_from_logical_plan(
                                        view_table.logical_plan(),
                                        extension_codec,
                                    )?,
                                )),
                                schema: Some(schema),
                                projection,
                                definition: view_table
                                    .definition()
                                    .map(|s| s.to_string())
                                    .unwrap_or_default(),
                            },
                        ))),
                    })
                } else {
                    let mut bytes = vec![];
                    extension_codec
                        .try_encode_table_provider(provider, &mut bytes)
                        .map_err(|e| context!("Error serializing custom table", e))?;
                    let scan = CustomScan(CustomTableScanNode {
                        table_name: table_name.clone(),
                        projection,
                        schema: Some(schema),
                        filters,
                        custom_table_data: bytes,
                    });
                    let node = LogicalPlanNode {
                        logical_plan_type: Some(scan),
                    };
                    Ok(node)
                }
            }
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Projection(Box::new(
                        protobuf::ProjectionNode {
                            input: Some(Box::new(
                                protobuf::LogicalPlanNode::try_from_logical_plan(
                                    input.as_ref(),
                                    extension_codec,
                                )?,
                            )),
                            expr: expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, to_proto::Error>>()?,
                            optional_alias: None,
                        },
                    ))),
                })
            }
            LogicalPlan::Filter(filter) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        filter.input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Selection(Box::new(
                        protobuf::SelectionNode {
                            input: Some(Box::new(input)),
                            expr: Some((&filter.predicate).try_into()?),
                        },
                    ))),
                })
            }
            LogicalPlan::Distinct(Distinct { input }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Distinct(Box::new(
                        protobuf::DistinctNode {
                            input: Some(Box::new(input)),
                        },
                    ))),
                })
            }
            LogicalPlan::Window(Window {
                input, window_expr, ..
            }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Window(Box::new(
                        protobuf::WindowNode {
                            input: Some(Box::new(input)),
                            window_expr: window_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                        },
                    ))),
                })
            }
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                input,
                ..
            }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Aggregate(Box::new(
                        protobuf::AggregateNode {
                            input: Some(Box::new(input)),
                            group_expr: group_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                            aggr_expr: aggr_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                        },
                    ))),
                })
            }
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                null_equals_null,
                ..
            }) => {
                let left: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        left.as_ref(),
                        extension_codec,
                    )?;
                let right: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        right.as_ref(),
                        extension_codec,
                    )?;
                let (left_join_key, right_join_key) = on
                    .iter()
                    .map(|(l, r)| Ok((l.try_into()?, r.try_into()?)))
                    .collect::<Result<Vec<_>, to_proto::Error>>()?
                    .into_iter()
                    .unzip();
                let join_type: protobuf::JoinType = join_type.to_owned().into();
                let join_constraint: protobuf::JoinConstraint =
                    join_constraint.to_owned().into();
                let filter = filter
                    .as_ref()
                    .map(|e| e.try_into())
                    .map_or(Ok(None), |v| v.map(Some))?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Join(Box::new(
                        protobuf::JoinNode {
                            left: Some(Box::new(left)),
                            right: Some(Box::new(right)),
                            join_type: join_type.into(),
                            join_constraint: join_constraint.into(),
                            left_join_key,
                            right_join_key,
                            null_equals_null: *null_equals_null,
                            filter,
                        },
                    ))),
                })
            }
            LogicalPlan::Subquery(_) => Err(DataFusionError::NotImplemented(
                "LogicalPlan serde is not yet implemented for subqueries".to_string(),
            )),
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::SubqueryAlias(Box::new(
                        protobuf::SubqueryAliasNode {
                            input: Some(Box::new(input)),
                            alias: alias.clone(),
                        },
                    ))),
                })
            }
            LogicalPlan::Limit(Limit { input, skip, fetch }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Limit(Box::new(
                        protobuf::LimitNode {
                            input: Some(Box::new(input)),
                            skip: *skip as i64,
                            fetch: fetch.unwrap_or(i64::MAX as usize) as i64,
                        },
                    ))),
                })
            }
            LogicalPlan::Sort(Sort { input, expr, fetch }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                let selection_expr: Vec<protobuf::LogicalExprNode> = expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, to_proto::Error>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Sort(Box::new(
                        protobuf::SortNode {
                            input: Some(Box::new(input)),
                            expr: selection_expr,
                            fetch: fetch.map(|f| f as i64).unwrap_or(-1i64),
                        },
                    ))),
                })
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => {
                use datafusion::logical_expr::Partitioning;
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;

                // Assumed common usize field was batch size
                // Used u64 to avoid any nastyness involving large values, most data clusters are probably uniformly 64 bits any ways
                use protobuf::repartition_node::PartitionMethod;

                let pb_partition_method = match partitioning_scheme {
                    Partitioning::Hash(exprs, partition_count) => {
                        PartitionMethod::Hash(protobuf::HashRepartition {
                            hash_expr: exprs
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, to_proto::Error>>()?,
                            partition_count: *partition_count as u64,
                        })
                    }
                    Partitioning::RoundRobinBatch(partition_count) => {
                        PartitionMethod::RoundRobin(*partition_count as u64)
                    }
                    Partitioning::DistributeBy(_) => {
                        return Err(DataFusionError::NotImplemented(
                            "DistributeBy".to_string(),
                        ))
                    }
                };

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Repartition(Box::new(
                        protobuf::RepartitionNode {
                            input: Some(Box::new(input)),
                            partition_method: Some(pb_partition_method),
                        },
                    ))),
                })
            }
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row, ..
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::EmptyRelation(
                    protobuf::EmptyRelationNode {
                        produce_one_row: *produce_one_row,
                    },
                )),
            }),
            LogicalPlan::CreateExternalTable(CreateExternalTable {
                name,
                location,
                file_type,
                has_header,
                delimiter,
                schema: df_schema,
                table_partition_cols,
                if_not_exists,
                definition,
                file_compression_type,
                options,
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateExternalTable(
                    protobuf::CreateExternalTableNode {
                        name: Some(name.clone().into()),
                        location: location.clone(),
                        file_type: file_type.clone(),
                        has_header: *has_header,
                        schema: Some(df_schema.try_into()?),
                        table_partition_cols: table_partition_cols.clone(),
                        if_not_exists: *if_not_exists,
                        delimiter: String::from(*delimiter),
                        definition: definition.clone().unwrap_or_default(),
                        file_compression_type: file_compression_type.to_string(),
                        options: options.clone(),
                    },
                )),
            }),
            LogicalPlan::CreateView(CreateView {
                name,
                input,
                or_replace,
                definition,
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateView(Box::new(
                    protobuf::CreateViewNode {
                        name: Some(name.clone().into()),
                        input: Some(Box::new(LogicalPlanNode::try_from_logical_plan(
                            input,
                            extension_codec,
                        )?)),
                        or_replace: *or_replace,
                        definition: definition.clone().unwrap_or_default(),
                    },
                ))),
            }),
            LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                schema_name,
                if_not_exists,
                schema: df_schema,
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateCatalogSchema(
                    protobuf::CreateCatalogSchemaNode {
                        schema_name: schema_name.clone(),
                        if_not_exists: *if_not_exists,
                        schema: Some(df_schema.try_into()?),
                    },
                )),
            }),
            LogicalPlan::CreateCatalog(CreateCatalog {
                catalog_name,
                if_not_exists,
                schema: df_schema,
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateCatalog(
                    protobuf::CreateCatalogNode {
                        catalog_name: catalog_name.clone(),
                        if_not_exists: *if_not_exists,
                        schema: Some(df_schema.try_into()?),
                    },
                )),
            }),
            LogicalPlan::Analyze(a) => {
                let input = protobuf::LogicalPlanNode::try_from_logical_plan(
                    a.input.as_ref(),
                    extension_codec,
                )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Analyze(Box::new(
                        protobuf::AnalyzeNode {
                            input: Some(Box::new(input)),
                            verbose: a.verbose,
                        },
                    ))),
                })
            }
            LogicalPlan::Explain(a) => {
                let input = protobuf::LogicalPlanNode::try_from_logical_plan(
                    a.plan.as_ref(),
                    extension_codec,
                )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Explain(Box::new(
                        protobuf::ExplainNode {
                            input: Some(Box::new(input)),
                            verbose: a.verbose,
                        },
                    ))),
                })
            }
            LogicalPlan::Union(union) => {
                let inputs: Vec<LogicalPlanNode> = union
                    .inputs
                    .iter()
                    .map(|i| {
                        protobuf::LogicalPlanNode::try_from_logical_plan(
                            i,
                            extension_codec,
                        )
                    })
                    .collect::<Result<_, DataFusionError>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Union(
                        protobuf::UnionNode { inputs },
                    )),
                })
            }
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                let left = protobuf::LogicalPlanNode::try_from_logical_plan(
                    left.as_ref(),
                    extension_codec,
                )?;
                let right = protobuf::LogicalPlanNode::try_from_logical_plan(
                    right.as_ref(),
                    extension_codec,
                )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::CrossJoin(Box::new(
                        protobuf::CrossJoinNode {
                            left: Some(Box::new(left)),
                            right: Some(Box::new(right)),
                        },
                    ))),
                })
            }
            LogicalPlan::Extension(extension) => {
                let mut buf: Vec<u8> = vec![];
                extension_codec.try_encode(extension, &mut buf)?;

                let inputs: Vec<LogicalPlanNode> = extension
                    .node
                    .inputs()
                    .iter()
                    .map(|i| {
                        protobuf::LogicalPlanNode::try_from_logical_plan(
                            i,
                            extension_codec,
                        )
                    })
                    .collect::<Result<_, DataFusionError>>()?;

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Extension(
                        LogicalExtensionNode { node: buf, inputs },
                    )),
                })
            }
            LogicalPlan::Prepare(Prepare {
                name,
                data_types,
                input,
            }) => {
                let input = protobuf::LogicalPlanNode::try_from_logical_plan(
                    input,
                    extension_codec,
                )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Prepare(Box::new(
                        protobuf::PrepareNode {
                            name: name.clone(),
                            data_types: data_types
                                .iter()
                                .map(|t| t.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                            input: Some(Box::new(input)),
                        },
                    ))),
                })
            }
            LogicalPlan::CreateMemoryTable(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for CreateMemoryTable",
            )),
            LogicalPlan::DropTable(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for DropTable",
            )),
            LogicalPlan::DropView(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for DropView",
            )),
            LogicalPlan::SetVariable(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for DropView",
            )),
        }
    }
}

#[cfg(test)]
mod roundtrip_tests {
    use super::from_proto::parse_expr;
    use super::protobuf;
    use crate::bytes::{
        logical_plan_from_bytes, logical_plan_from_bytes_with_extension_codec,
        logical_plan_to_bytes, logical_plan_to_bytes_with_extension_codec,
    };
    use crate::logical_plan::LogicalExtensionCodec;
    use arrow::datatypes::{Schema, SchemaRef};
    use arrow::{
        array::ArrayRef,
        datatypes::{
            DataType, Field, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
            TimeUnit, UnionMode,
        },
    };
    use datafusion::datasource::datasource::TableProviderFactory;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::physical_plan::functions::make_scalar_function;
    use datafusion::prelude::{
        create_udf, CsvReadOptions, SessionConfig, SessionContext,
    };
    use datafusion::test_util::{TestTableFactory, TestTableProvider};
    use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};
    use datafusion_expr::expr::{
        self, Between, BinaryExpr, Case, Cast, GroupingSet, Like, Sort,
    };
    use datafusion_expr::logical_plan::{Extension, UserDefinedLogicalNode};
    use datafusion_expr::{
        col, lit, Accumulator, AggregateFunction,
        BuiltinScalarFunction::{Sqrt, Substr},
        Expr, LogicalPlan, Operator, Volatility,
    };
    use datafusion_expr::{
        create_udaf, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunction,
    };
    use prost::Message;
    use std::any::Any;
    use std::collections::HashMap;
    use std::fmt;
    use std::fmt::Debug;
    use std::fmt::Formatter;
    use std::sync::Arc;

    #[cfg(feature = "json")]
    fn roundtrip_json_test(proto: &protobuf::LogicalExprNode) {
        let string = serde_json::to_string(proto).unwrap();
        let back: protobuf::LogicalExprNode = serde_json::from_str(&string).unwrap();
        assert_eq!(proto, &back);
    }

    #[cfg(not(feature = "json"))]
    fn roundtrip_json_test(_proto: &protobuf::LogicalExprNode) {}

    // Given a DataFusion logical Expr, convert it to protobuf and back, using debug formatting to test
    // equality.
    fn roundtrip_expr_test<T, E>(initial_struct: T, ctx: SessionContext)
    where
        for<'a> &'a T: TryInto<protobuf::LogicalExprNode, Error = E> + Debug,
        E: Debug,
    {
        let proto: protobuf::LogicalExprNode = (&initial_struct).try_into().unwrap();
        let round_trip: Expr = parse_expr(&proto, &ctx).unwrap();

        assert_eq!(format!("{:?}", &initial_struct), format!("{round_trip:?}"));

        roundtrip_json_test(&proto);
    }

    fn new_box_field(name: &str, dt: DataType, nullable: bool) -> Box<Field> {
        Box::new(Field::new(name, dt, nullable))
    }

    #[tokio::test]
    async fn roundtrip_logical_plan() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_csv("t1", "testdata/test.csv", CsvReadOptions::default())
            .await?;
        let scan = ctx.table("t1").await?.into_optimized_plan()?;
        let topk_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(TopKPlanNode::new(3, scan, col("revenue"))),
        });
        let extension_codec = TopKExtensionCodec {};
        let bytes =
            logical_plan_to_bytes_with_extension_codec(&topk_plan, &extension_codec)?;
        let logical_round_trip =
            logical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &extension_codec)?;
        assert_eq!(format!("{topk_plan:?}"), format!("{logical_round_trip:?}"));
        Ok(())
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct TestTableProto {
        /// URL of the table root
        #[prost(string, tag = "1")]
        pub url: String,
    }

    #[derive(Debug)]
    pub struct TestTableProviderCodec {}

    impl LogicalExtensionCodec for TestTableProviderCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[LogicalPlan],
            _ctx: &SessionContext,
        ) -> Result<Extension, DataFusionError> {
            Err(DataFusionError::NotImplemented(
                "No extension codec provided".to_string(),
            ))
        }

        fn try_encode(
            &self,
            _node: &Extension,
            _buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            Err(DataFusionError::NotImplemented(
                "No extension codec provided".to_string(),
            ))
        }

        fn try_decode_table_provider(
            &self,
            buf: &[u8],
            schema: SchemaRef,
            _ctx: &SessionContext,
        ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
            let msg = TestTableProto::decode(buf).map_err(|_| {
                DataFusionError::Internal("Error decoding test table".to_string())
            })?;
            let provider = TestTableProvider {
                url: msg.url,
                schema,
            };
            Ok(Arc::new(provider))
        }

        fn try_encode_table_provider(
            &self,
            node: Arc<dyn TableProvider>,
            buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            let table = node
                .as_ref()
                .as_any()
                .downcast_ref::<TestTableProvider>()
                .expect("Can't encode non-test tables");
            let msg = TestTableProto {
                url: table.url.clone(),
            };
            msg.encode(buf).map_err(|_| {
                DataFusionError::Internal("Error encoding test table".to_string())
            })
        }
    }

    #[tokio::test]
    async fn roundtrip_custom_tables() -> Result<(), DataFusionError> {
        let mut table_factories: HashMap<String, Arc<dyn TableProviderFactory>> =
            HashMap::new();
        table_factories.insert("TESTTABLE".to_string(), Arc::new(TestTableFactory {}));
        let cfg = RuntimeConfig::new().with_table_factories(table_factories);
        let env = RuntimeEnv::new(cfg).unwrap();
        let ses = SessionConfig::new();
        let ctx = SessionContext::with_config_rt(ses, Arc::new(env));

        let sql = "CREATE EXTERNAL TABLE t STORED AS testtable LOCATION 's3://bucket/schema/table';";
        ctx.sql(sql).await.unwrap();

        let codec = TestTableProviderCodec {};
        let scan = ctx.table("t").await?.into_optimized_plan()?;
        let bytes = logical_plan_to_bytes_with_extension_codec(&scan, &codec)?;
        let logical_round_trip =
            logical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &codec)?;
        assert_eq!(format!("{scan:?}"), format!("{logical_round_trip:?}"));
        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_logical_plan_aggregation() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Decimal128(15, 2), true),
        ]);

        ctx.register_csv(
            "t1",
            "testdata/test.csv",
            CsvReadOptions::default().schema(&schema),
        )
        .await?;

        let query =
            "SELECT a, SUM(b + 1) as b_sum FROM t1 GROUP BY a ORDER BY b_sum DESC";
        let plan = ctx.sql(query).await?.into_optimized_plan()?;

        let bytes = logical_plan_to_bytes(&plan)?;
        let logical_round_trip = logical_plan_from_bytes(&bytes, &ctx)?;
        assert_eq!(format!("{plan:?}"), format!("{logical_round_trip:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_single_count_distinct() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Decimal128(15, 2), true),
        ]);

        ctx.register_csv(
            "t1",
            "testdata/test.csv",
            CsvReadOptions::default().schema(&schema),
        )
        .await?;

        let query = "SELECT a, COUNT(DISTINCT b) as b_cd FROM t1 GROUP BY a";
        let plan = ctx.sql(query).await?.into_optimized_plan()?;

        let bytes = logical_plan_to_bytes(&plan)?;
        let logical_round_trip = logical_plan_from_bytes(&bytes, &ctx)?;
        assert_eq!(format!("{plan:?}"), format!("{logical_round_trip:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_logical_plan_with_extension() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_csv("t1", "testdata/test.csv", CsvReadOptions::default())
            .await?;
        let plan = ctx.table("t1").await?.into_optimized_plan()?;
        let bytes = logical_plan_to_bytes(&plan)?;
        let logical_round_trip = logical_plan_from_bytes(&bytes, &ctx)?;
        assert_eq!(format!("{plan:?}"), format!("{logical_round_trip:?}"));
        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_logical_plan_with_view_scan() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_csv("t1", "testdata/test.csv", CsvReadOptions::default())
            .await?;
        ctx.sql("CREATE VIEW view_t1(a, b) AS SELECT a, b FROM t1")
            .await?;
        let plan = ctx
            .sql("SELECT * FROM view_t1")
            .await?
            .into_optimized_plan()?;
        let bytes = logical_plan_to_bytes(&plan)?;
        let logical_round_trip = logical_plan_from_bytes(&bytes, &ctx)?;
        assert_eq!(format!("{plan:?}"), format!("{logical_round_trip:?}"));
        Ok(())
    }

    pub mod proto {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TopKPlanProto {
            #[prost(uint64, tag = "1")]
            pub k: u64,

            #[prost(message, optional, tag = "2")]
            pub expr: ::core::option::Option<crate::protobuf::LogicalExprNode>,
        }

        #[derive(Clone, PartialEq, Eq, ::prost::Message)]
        pub struct TopKExecProto {
            #[prost(uint64, tag = "1")]
            pub k: u64,
        }
    }

    struct TopKPlanNode {
        k: usize,
        input: LogicalPlan,
        /// The sort expression (this example only supports a single sort
        /// expr)
        expr: Expr,
    }

    impl TopKPlanNode {
        pub fn new(k: usize, input: LogicalPlan, expr: Expr) -> Self {
            Self { k, input, expr }
        }
    }

    impl Debug for TopKPlanNode {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            self.fmt_for_explain(f)
        }
    }

    impl UserDefinedLogicalNode for TopKPlanNode {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.input]
        }

        /// Schema for TopK is the same as the input
        fn schema(&self) -> &DFSchemaRef {
            self.input.schema()
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![self.expr.clone()]
        }

        /// For example: `TopK: k=10`
        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "TopK: k={}", self.k)
        }

        fn from_template(
            &self,
            exprs: &[Expr],
            inputs: &[LogicalPlan],
        ) -> Arc<dyn UserDefinedLogicalNode> {
            assert_eq!(inputs.len(), 1, "input size inconsistent");
            assert_eq!(exprs.len(), 1, "expression size inconsistent");
            Arc::new(TopKPlanNode {
                k: self.k,
                input: inputs[0].clone(),
                expr: exprs[0].clone(),
            })
        }
    }

    #[derive(Debug)]
    pub struct TopKExtensionCodec {}

    impl LogicalExtensionCodec for TopKExtensionCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[LogicalPlan],
            ctx: &SessionContext,
        ) -> Result<Extension, DataFusionError> {
            if let Some((input, _)) = inputs.split_first() {
                let proto = proto::TopKPlanProto::decode(buf).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to decode logical plan: {e:?}"
                    ))
                })?;

                if let Some(expr) = proto.expr.as_ref() {
                    let node = TopKPlanNode::new(
                        proto.k as usize,
                        input.clone(),
                        parse_expr(expr, ctx)?,
                    );

                    Ok(Extension {
                        node: Arc::new(node),
                    })
                } else {
                    Err(DataFusionError::Internal(
                        "invalid plan, no expr".to_string(),
                    ))
                }
            } else {
                Err(DataFusionError::Internal(
                    "invalid plan, no input".to_string(),
                ))
            }
        }

        fn try_encode(
            &self,
            node: &Extension,
            buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            if let Some(exec) = node.node.as_any().downcast_ref::<TopKPlanNode>() {
                let proto = proto::TopKPlanProto {
                    k: exec.k as u64,
                    expr: Some((&exec.expr).try_into()?),
                };

                proto.encode(buf).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to encode logical plan: {e:?}"
                    ))
                })?;

                Ok(())
            } else {
                Err(DataFusionError::Internal(
                    "unsupported plan type".to_string(),
                ))
            }
        }

        fn try_decode_table_provider(
            &self,
            _buf: &[u8],
            _schema: SchemaRef,
            _ctx: &SessionContext,
        ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
            Err(DataFusionError::Internal(
                "unsupported plan type".to_string(),
            ))
        }

        fn try_encode_table_provider(
            &self,
            _node: Arc<dyn TableProvider>,
            _buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            Err(DataFusionError::Internal(
                "unsupported plan type".to_string(),
            ))
        }
    }

    #[test]
    fn scalar_values_error_serialization() {
        let should_fail_on_seralize: Vec<ScalarValue> = vec![
            // Should fail due to empty values
            ScalarValue::Struct(
                Some(vec![]),
                Box::new(vec![Field::new("item", DataType::Int16, true)]),
            ),
            // Should fail due to inconsistent types in the list
            ScalarValue::new_list(
                Some(vec![
                    ScalarValue::Int16(None),
                    ScalarValue::Float32(Some(32.0)),
                ]),
                DataType::List(new_box_field("item", DataType::Int16, true)),
            ),
            ScalarValue::new_list(
                Some(vec![
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(32.0)),
                ]),
                DataType::List(new_box_field("item", DataType::Int16, true)),
            ),
            ScalarValue::new_list(
                Some(vec![
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(32.0)),
                ]),
                DataType::Int16,
            ),
            ScalarValue::new_list(
                Some(vec![
                    ScalarValue::new_list(
                        None,
                        DataType::List(new_box_field("level2", DataType::Float32, true)),
                    ),
                    ScalarValue::new_list(
                        Some(vec![
                            ScalarValue::Float32(Some(-213.1)),
                            ScalarValue::Float32(None),
                            ScalarValue::Float32(Some(5.5)),
                            ScalarValue::Float32(Some(2.0)),
                            ScalarValue::Float32(Some(1.0)),
                        ]),
                        DataType::List(new_box_field("level2", DataType::Float32, true)),
                    ),
                    ScalarValue::new_list(
                        None,
                        DataType::List(new_box_field(
                            "lists are typed inconsistently",
                            DataType::Int16,
                            true,
                        )),
                    ),
                ]),
                DataType::List(new_box_field(
                    "level1",
                    DataType::List(new_box_field("level2", DataType::Float32, true)),
                    true,
                )),
            ),
        ];

        for test_case in should_fail_on_seralize.into_iter() {
            let proto: Result<super::protobuf::ScalarValue, super::to_proto::Error> =
                (&test_case).try_into();

            // Validation is also done on read, so if serialization passed
            // also try to convert back to ScalarValue
            if let Ok(proto) = proto {
                let res: Result<ScalarValue, _> = (&proto).try_into();
                assert!(
                    res.is_err(),
                    "The value {test_case:?} unexpectedly serialized without error:{res:?}"
                );
            }
        }
    }

    #[test]
    fn round_trip_scalar_values() {
        let should_pass: Vec<ScalarValue> = vec![
            ScalarValue::Boolean(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::new_list(None, DataType::Boolean),
            ScalarValue::Date32(None),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Float32(Some(1.0)),
            ScalarValue::Float32(Some(f32::MAX)),
            ScalarValue::Float32(Some(f32::MIN)),
            ScalarValue::Float32(Some(-2000.0)),
            ScalarValue::Float64(Some(1.0)),
            ScalarValue::Float64(Some(f64::MAX)),
            ScalarValue::Float64(Some(f64::MIN)),
            ScalarValue::Float64(Some(-2000.0)),
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int8(Some(i8::MAX)),
            ScalarValue::Int8(Some(0)),
            ScalarValue::Int8(Some(-15)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int16(Some(0)),
            ScalarValue::Int16(Some(-15)),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int32(Some(i32::MAX)),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int32(Some(-15)),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(-15)),
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt8(Some(0)),
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt16(Some(0)),
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt32(Some(0)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(Some(0)),
            ScalarValue::Utf8(Some(String::from("Test string   "))),
            ScalarValue::LargeUtf8(Some(String::from("Test Large utf8"))),
            ScalarValue::Date32(Some(0)),
            ScalarValue::Date32(Some(i32::MAX)),
            ScalarValue::Date32(None),
            ScalarValue::Date64(Some(0)),
            ScalarValue::Date64(Some(i64::MAX)),
            ScalarValue::Date64(None),
            ScalarValue::Time32Second(Some(0)),
            ScalarValue::Time32Second(Some(i32::MAX)),
            ScalarValue::Time32Second(None),
            ScalarValue::Time32Millisecond(Some(0)),
            ScalarValue::Time32Millisecond(Some(i32::MAX)),
            ScalarValue::Time32Millisecond(None),
            ScalarValue::Time64Microsecond(Some(0)),
            ScalarValue::Time64Microsecond(Some(i64::MAX)),
            ScalarValue::Time64Microsecond(None),
            ScalarValue::Time64Nanosecond(Some(0)),
            ScalarValue::Time64Nanosecond(Some(i64::MAX)),
            ScalarValue::Time64Nanosecond(None),
            ScalarValue::TimestampNanosecond(Some(0), None),
            ScalarValue::TimestampNanosecond(Some(i64::MAX), None),
            ScalarValue::TimestampNanosecond(Some(0), Some("UTC".to_string())),
            ScalarValue::TimestampNanosecond(None, None),
            ScalarValue::TimestampMicrosecond(Some(0), None),
            ScalarValue::TimestampMicrosecond(Some(i64::MAX), None),
            ScalarValue::TimestampMicrosecond(Some(0), Some("UTC".to_string())),
            ScalarValue::TimestampMicrosecond(None, None),
            ScalarValue::TimestampMillisecond(Some(0), None),
            ScalarValue::TimestampMillisecond(Some(i64::MAX), None),
            ScalarValue::TimestampMillisecond(Some(0), Some("UTC".to_string())),
            ScalarValue::TimestampMillisecond(None, None),
            ScalarValue::TimestampSecond(Some(0), None),
            ScalarValue::TimestampSecond(Some(i64::MAX), None),
            ScalarValue::TimestampSecond(Some(0), Some("UTC".to_string())),
            ScalarValue::TimestampSecond(None, None),
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(0, 0))),
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(1, 2))),
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(
                i32::MAX,
                i32::MAX,
            ))),
            ScalarValue::IntervalDayTime(None),
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(0, 0, 0),
            )),
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(1, 2, 3),
            )),
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(i32::MAX, i32::MAX, i64::MAX),
            )),
            ScalarValue::IntervalMonthDayNano(None),
            ScalarValue::new_list(
                Some(vec![
                    ScalarValue::Float32(Some(-213.1)),
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(5.5)),
                    ScalarValue::Float32(Some(2.0)),
                    ScalarValue::Float32(Some(1.0)),
                ]),
                DataType::Float32,
            ),
            ScalarValue::new_list(
                Some(vec![
                    ScalarValue::new_list(None, DataType::Float32),
                    ScalarValue::new_list(
                        Some(vec![
                            ScalarValue::Float32(Some(-213.1)),
                            ScalarValue::Float32(None),
                            ScalarValue::Float32(Some(5.5)),
                            ScalarValue::Float32(Some(2.0)),
                            ScalarValue::Float32(Some(1.0)),
                        ]),
                        DataType::Float32,
                    ),
                ]),
                DataType::List(new_box_field("item", DataType::Float32, true)),
            ),
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("foo".into()))),
            ),
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(None)),
            ),
            ScalarValue::Binary(Some(b"bar".to_vec())),
            ScalarValue::Binary(None),
            ScalarValue::LargeBinary(Some(b"bar".to_vec())),
            ScalarValue::LargeBinary(None),
            ScalarValue::Struct(
                Some(vec![
                    ScalarValue::Int32(Some(23)),
                    ScalarValue::Boolean(Some(false)),
                ]),
                Box::new(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Boolean, false),
                ]),
            ),
            ScalarValue::Struct(
                None,
                Box::new(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("a", DataType::Boolean, false),
                ]),
            ),
            ScalarValue::FixedSizeBinary(
                b"bar".to_vec().len() as i32,
                Some(b"bar".to_vec()),
            ),
            ScalarValue::FixedSizeBinary(0, None),
            ScalarValue::FixedSizeBinary(5, None),
        ];

        for test_case in should_pass.into_iter() {
            let proto: super::protobuf::ScalarValue = (&test_case)
                .try_into()
                .expect("failed conversion to protobuf");

            let roundtrip: ScalarValue = (&proto)
                .try_into()
                .expect("failed conversion from protobuf");

            assert_eq!(
                test_case, roundtrip,
                "ScalarValue was not the same after round trip!\n\n\
                        Input: {test_case:?}\n\nRoundtrip: {roundtrip:?}"
            );
        }
    }

    #[test]
    fn round_trip_scalar_types() {
        let should_pass: Vec<DataType> = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Utf8,
            DataType::LargeUtf8,
            // Recursive list tests
            DataType::List(new_box_field("level1", DataType::Boolean, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field("level2", DataType::Date32, true)),
                true,
            )),
        ];

        for test_case in should_pass.into_iter() {
            let field = Field::new("item", test_case, true);
            let proto: super::protobuf::Field = (&field).try_into().unwrap();
            let roundtrip: Field = (&proto).try_into().unwrap();
            assert_eq!(format!("{field:?}"), format!("{roundtrip:?}"));
        }
    }

    #[test]
    fn round_trip_datatype() {
        let test_cases: Vec<DataType> = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            // Add more timestamp tests
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Binary,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Decimal128(7, 12),
            // Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Binary, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field(
                    "Level2",
                    DataType::FixedSizeBinary(53),
                    false,
                )),
                true,
            )),
            // Fixed size lists
            DataType::FixedSizeList(new_box_field("Level1", DataType::Binary, true), 4),
            DataType::FixedSizeList(
                new_box_field(
                    "Level1",
                    DataType::List(new_box_field(
                        "Level2",
                        DataType::FixedSizeBinary(53),
                        false,
                    )),
                    true,
                ),
                41,
            ),
            // Struct Testing
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ],
                vec![7, 5, 3],
                UnionMode::Sparse,
            ),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                    Field::new(
                        "nested_struct",
                        DataType::Struct(vec![
                            Field::new("nullable", DataType::Boolean, false),
                            Field::new("name", DataType::Utf8, false),
                            Field::new("datatype", DataType::Binary, false),
                        ]),
                        true,
                    ),
                ],
                vec![5, 8, 1],
                UnionMode::Dense,
            ),
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::Struct(vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ])),
            ),
            DataType::Dictionary(
                Box::new(DataType::Decimal128(10, 50)),
                Box::new(DataType::FixedSizeList(
                    new_box_field("Level1", DataType::Binary, true),
                    4,
                )),
            ),
        ];

        for test_case in test_cases.into_iter() {
            let proto: super::protobuf::ArrowType = (&test_case).try_into().unwrap();
            let roundtrip: DataType = (&proto).try_into().unwrap();
            assert_eq!(format!("{test_case:?}"), format!("{roundtrip:?}"));
        }
    }

    #[test]
    fn roundtrip_null_scalar_values() {
        let test_types = vec![
            ScalarValue::Boolean(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::Date32(None),
            ScalarValue::TimestampMicrosecond(None, None),
            ScalarValue::TimestampNanosecond(None, None),
            ScalarValue::List(
                None,
                Box::new(Field::new("item", DataType::Boolean, false)),
            ),
        ];

        for test_case in test_types.into_iter() {
            let proto_scalar: super::protobuf::ScalarValue =
                (&test_case).try_into().unwrap();
            let returned_scalar: datafusion::scalar::ScalarValue =
                (&proto_scalar).try_into().unwrap();
            assert_eq!(format!("{:?}", &test_case), format!("{returned_scalar:?}"));
        }
    }

    #[test]
    fn roundtrip_not() {
        let test_expr = Expr::Not(Box::new(lit(1.0_f32)));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_is_null() {
        let test_expr = Expr::IsNull(Box::new(col("id")));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_is_not_null() {
        let test_expr = Expr::IsNotNull(Box::new(col("id")));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_between() {
        let test_expr = Expr::Between(Between::new(
            Box::new(lit(1.0_f32)),
            true,
            Box::new(lit(2.0_f32)),
            Box::new(lit(3.0_f32)),
        ));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_binary_op() {
        fn test(op: Operator) {
            let test_expr = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(lit(1.0_f32)),
                op,
                Box::new(lit(2.0_f32)),
            ));
            let ctx = SessionContext::new();
            roundtrip_expr_test(test_expr, ctx);
        }
        test(Operator::StringConcat);
        test(Operator::RegexNotIMatch);
        test(Operator::RegexNotMatch);
        test(Operator::RegexIMatch);
        test(Operator::RegexMatch);
        test(Operator::BitwiseShiftRight);
        test(Operator::BitwiseShiftLeft);
        test(Operator::BitwiseAnd);
        test(Operator::BitwiseOr);
        test(Operator::BitwiseXor);
        test(Operator::IsDistinctFrom);
        test(Operator::IsNotDistinctFrom);
        test(Operator::And);
        test(Operator::Or);
        test(Operator::Eq);
        test(Operator::NotEq);
        test(Operator::Lt);
        test(Operator::LtEq);
        test(Operator::Gt);
        test(Operator::GtEq);
    }

    #[test]
    fn roundtrip_case() {
        let test_expr = Expr::Case(Case::new(
            Some(Box::new(lit(1.0_f32))),
            vec![(Box::new(lit(2.0_f32)), Box::new(lit(3.0_f32)))],
            Some(Box::new(lit(4.0_f32))),
        ));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_case_with_null() {
        let test_expr = Expr::Case(Case::new(
            Some(Box::new(lit(1.0_f32))),
            vec![(Box::new(lit(2.0_f32)), Box::new(lit(3.0_f32)))],
            Some(Box::new(Expr::Literal(ScalarValue::Null))),
        ));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_null_literal() {
        let test_expr = Expr::Literal(ScalarValue::Null);

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_cast() {
        let test_expr = Expr::Cast(Cast::new(Box::new(lit(1.0_f32)), DataType::Boolean));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_sort_expr() {
        let test_expr = Expr::Sort(Sort::new(Box::new(lit(1.0_f32)), true, true));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_negative() {
        let test_expr = Expr::Negative(Box::new(lit(1.0_f32)));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_inlist() {
        let test_expr = Expr::InList {
            expr: Box::new(lit(1.0_f32)),
            list: vec![lit(2.0_f32)],
            negated: true,
        };

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_wildcard() {
        let test_expr = Expr::Wildcard;

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_sqrt() {
        let test_expr = Expr::ScalarFunction {
            fun: Sqrt,
            args: vec![col("col")],
        };
        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_like() {
        fn like(negated: bool, escape_char: Option<char>) {
            let test_expr = Expr::Like(Like::new(
                negated,
                Box::new(col("col")),
                Box::new(lit("[0-9]+")),
                escape_char,
            ));
            let ctx = SessionContext::new();
            roundtrip_expr_test(test_expr, ctx);
        }
        like(true, Some('X'));
        like(false, Some('\\'));
        like(true, None);
        like(false, None);
    }

    #[test]
    fn roundtrip_ilike() {
        fn ilike(negated: bool, escape_char: Option<char>) {
            let test_expr = Expr::ILike(Like::new(
                negated,
                Box::new(col("col")),
                Box::new(lit("[0-9]+")),
                escape_char,
            ));
            let ctx = SessionContext::new();
            roundtrip_expr_test(test_expr, ctx);
        }
        ilike(true, Some('X'));
        ilike(false, Some('\\'));
        ilike(true, None);
        ilike(false, None);
    }

    #[test]
    fn roundtrip_similar_to() {
        fn similar_to(negated: bool, escape_char: Option<char>) {
            let test_expr = Expr::SimilarTo(Like::new(
                negated,
                Box::new(col("col")),
                Box::new(lit("[0-9]+")),
                escape_char,
            ));
            let ctx = SessionContext::new();
            roundtrip_expr_test(test_expr, ctx);
        }
        similar_to(true, Some('X'));
        similar_to(false, Some('\\'));
        similar_to(true, None);
        similar_to(false, None);
    }

    #[test]
    fn roundtrip_count() {
        let test_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            AggregateFunction::Count,
            vec![col("bananas")],
            false,
            None,
        ));
        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_count_distinct() {
        let test_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            AggregateFunction::Count,
            vec![col("bananas")],
            true,
            None,
        ));
        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_approx_percentile_cont() {
        let test_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            AggregateFunction::ApproxPercentileCont,
            vec![col("bananas"), lit(0.42_f32)],
            false,
            None,
        ));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_aggregate_udf() {
        #[derive(Debug)]
        struct Dummy {}

        impl Accumulator for Dummy {
            fn state(&self) -> datafusion::error::Result<Vec<ScalarValue>> {
                Ok(vec![])
            }

            fn update_batch(
                &mut self,
                _values: &[ArrayRef],
            ) -> datafusion::error::Result<()> {
                Ok(())
            }

            fn merge_batch(
                &mut self,
                _states: &[ArrayRef],
            ) -> datafusion::error::Result<()> {
                Ok(())
            }

            fn evaluate(&self) -> datafusion::error::Result<ScalarValue> {
                Ok(ScalarValue::Float64(None))
            }

            fn size(&self) -> usize {
                std::mem::size_of_val(self)
            }
        }

        let dummy_agg = create_udaf(
            // the name; used to represent it in plan descriptions and in the registry, to use in SQL.
            "dummy_agg",
            // the input type; DataFusion guarantees that the first entry of `values` in `update` has this type.
            DataType::Float64,
            // the return type; DataFusion expects this to match the type returned by `evaluate`.
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            // This is the accumulator factory; DataFusion uses it to create new accumulators.
            Arc::new(|_| Ok(Box::new(Dummy {}))),
            // This is the description of the state. `state()` must match the types here.
            Arc::new(vec![DataType::Float64, DataType::UInt32]),
        );

        let test_expr = Expr::AggregateUDF {
            fun: Arc::new(dummy_agg.clone()),
            args: vec![lit(1.0_f64)],
            filter: Some(Box::new(lit(true))),
        };

        let ctx = SessionContext::new();
        ctx.register_udaf(dummy_agg);

        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_scalar_udf() {
        let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

        let scalar_fn = make_scalar_function(fn_impl);

        let udf = create_udf(
            "dummy",
            vec![DataType::Utf8],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            scalar_fn,
        );

        let test_expr = Expr::ScalarUDF {
            fun: Arc::new(udf.clone()),
            args: vec![lit("")],
        };

        let ctx = SessionContext::new();
        ctx.register_udf(udf);

        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_grouping_sets() {
        let test_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("a")],
            vec![col("b")],
            vec![col("a"), col("b")],
        ]));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_rollup() {
        let test_expr = Expr::GroupingSet(GroupingSet::Rollup(vec![col("a"), col("b")]));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_cube() {
        let test_expr = Expr::GroupingSet(GroupingSet::Cube(vec![col("a"), col("b")]));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_substr() {
        // substr(string, position)
        let test_expr = Expr::ScalarFunction {
            fun: Substr,
            args: vec![col("col"), lit(1_i64)],
        };

        // substr(string, position, count)
        let test_expr_with_count = Expr::ScalarFunction {
            fun: Substr,
            args: vec![col("col"), lit(1_i64), lit(1_i64)],
        };

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx.clone());
        roundtrip_expr_test(test_expr_with_count, ctx);
    }
    #[test]
    fn roundtrip_window() {
        let ctx = SessionContext::new();

        // 1. without window_frame
        let test_expr1 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::BuiltInWindowFunction(
                datafusion_expr::window_function::BuiltInWindowFunction::Rank,
            ),
            vec![],
            vec![col("col1")],
            vec![col("col2")],
            WindowFrame::new(true),
        ));

        // 2. with default window_frame
        let test_expr2 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::BuiltInWindowFunction(
                datafusion_expr::window_function::BuiltInWindowFunction::Rank,
            ),
            vec![],
            vec![col("col1")],
            vec![col("col2")],
            WindowFrame::new(true),
        ));

        // 3. with window_frame with row numbers
        let range_number_frame = WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2))),
            end_bound: WindowFrameBound::Following(ScalarValue::UInt64(Some(2))),
        };

        let test_expr3 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::BuiltInWindowFunction(
                datafusion_expr::window_function::BuiltInWindowFunction::Rank,
            ),
            vec![],
            vec![col("col1")],
            vec![col("col2")],
            range_number_frame,
        ));

        // 4. test with AggregateFunction
        let row_number_frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2))),
            end_bound: WindowFrameBound::Following(ScalarValue::UInt64(Some(2))),
        };

        let test_expr4 = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::AggregateFunction(AggregateFunction::Max),
            vec![col("col1")],
            vec![col("col1")],
            vec![col("col2")],
            row_number_frame,
        ));

        roundtrip_expr_test(test_expr1, ctx.clone());
        roundtrip_expr_test(test_expr2, ctx.clone());
        roundtrip_expr_test(test_expr3, ctx.clone());
        roundtrip_expr_test(test_expr4, ctx);
    }
}
