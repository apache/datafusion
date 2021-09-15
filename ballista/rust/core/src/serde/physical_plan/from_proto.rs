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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::error::BallistaError;
use crate::execution_plans::{
    ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec,
};
use crate::serde::protobuf::repartition_exec_node::PartitionMethod;
use crate::serde::protobuf::ShuffleReaderPartition;
use crate::serde::scheduler::PartitionLocation;
use crate::serde::{from_proto_binary_op, proto_error, protobuf};
use crate::{convert_box_required, convert_required, into_required};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::catalog::catalog::{
    CatalogList, CatalogProvider, MemoryCatalogList, MemoryCatalogProvider,
};
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::datasource::FilePartition;
use datafusion::execution::context::{
    ExecutionConfig, ExecutionContextState, ExecutionProps,
};
use datafusion::logical_plan::{
    window_frames::WindowFrame, DFSchema, Expr, JoinConstraint, JoinType,
};
use datafusion::physical_plan::aggregates::{create_aggregate_expr, AggregateFunction};
use datafusion::physical_plan::avro::{AvroExec, AvroReadOptions};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::hash_join::PartitionMode;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::parquet::ParquetPartition;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::window_functions::{
    BuiltInWindowFunction, WindowFunction,
};
use datafusion::physical_plan::windows::{create_window_expr, WindowAggExec};
use datafusion::physical_plan::{
    coalesce_batches::CoalesceBatchesExec,
    cross_join::CrossJoinExec,
    csv::CsvExec,
    empty::EmptyExec,
    expressions::{
        col, Avg, BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr,
        IsNullExpr, Literal, NegativeExpr, NotExpr, PhysicalSortExpr, TryCastExpr,
        DEFAULT_DATAFUSION_CAST_OPTIONS,
    },
    filter::FilterExec,
    functions::{self, BuiltinScalarFunction, ScalarFunctionExpr},
    hash_join::HashJoinExec,
    limit::{GlobalLimitExec, LocalLimitExec},
    parquet::ParquetExec,
    projection::ProjectionExec,
    repartition::RepartitionExec,
    sort::{SortExec, SortOptions},
    Partitioning,
};
use datafusion::physical_plan::{
    AggregateExpr, ExecutionPlan, PhysicalExpr, Statistics, WindowExpr,
};
use datafusion::prelude::CsvReadOptions;
use log::debug;
use protobuf::physical_expr_node::ExprType;
use protobuf::physical_plan_node::PhysicalPlanType;

impl TryInto<Arc<dyn ExecutionPlan>> for &protobuf::PhysicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Arc<dyn ExecutionPlan>, Self::Error> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(projection.input)?;
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .map(|(expr, name)| Ok((expr.try_into()?, name.to_string())))
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, Self::Error>>(
                    )?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;
                let predicate = filter
                    .expr
                    .as_ref()
                    .ok_or_else(|| {
                        BallistaError::General(
                            "filter (FilterExecNode) in PhysicalPlanNode is missing."
                                .to_owned(),
                        )
                    })?
                    .try_into()?;
                Ok(Arc::new(FilterExec::try_new(predicate, input)?))
            }
            PhysicalPlanType::CsvScan(scan) => {
                let schema = Arc::new(convert_required!(scan.schema)?);
                let options = CsvReadOptions::new()
                    .has_header(scan.has_header)
                    .file_extension(&scan.file_extension)
                    .delimiter(scan.delimiter.as_bytes()[0])
                    .schema(&schema);
                let projection = scan.projection.iter().map(|i| *i as usize).collect();
                Ok(Arc::new(CsvExec::try_new(
                    &scan.path,
                    options,
                    Some(projection),
                    scan.batch_size as usize,
                    None,
                )?))
            }
            PhysicalPlanType::ParquetScan(scan) => {
                let partitions = scan
                    .partitions
                    .iter()
                    .map(|p| p.try_into())
                    .collect::<Result<Vec<ParquetPartition>, _>>()?;
                let schema = Arc::new(convert_required!(scan.schema)?);
                let projection = scan.projection.iter().map(|i| *i as usize).collect();
                Ok(Arc::new(ParquetExec::new(
                    partitions,
                    schema,
                    Some(projection),
                    Statistics::default(),
                    ExecutionPlanMetricsSet::new(),
                    None,
                    scan.batch_size as usize,
                    None,
                )))
            }
            PhysicalPlanType::AvroScan(scan) => {
                let schema = Arc::new(convert_required!(scan.schema)?);
                let options = AvroReadOptions {
                    schema: Some(schema),
                    file_extension: &scan.file_extension,
                };
                let projection = scan.projection.iter().map(|i| *i as usize).collect();
                Ok(Arc::new(AvroExec::try_from_path(
                    &scan.path,
                    options,
                    Some(projection),
                    scan.batch_size as usize,
                    None,
                )?))
            }
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(coalesce_batches.input)?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::Merge(merge) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(merge.input)?;
                Ok(Arc::new(CoalescePartitionsExec::new(input)))
            }
            PhysicalPlanType::Repartition(repart) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(repart.input)?;
                match repart.partition_method {
                    Some(PartitionMethod::Hash(ref hash_part)) => {
                        let expr = hash_part
                            .hash_expr
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::Hash(
                                expr,
                                hash_part.partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    Some(PartitionMethod::RoundRobin(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::RoundRobinBatch(
                                partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    Some(PartitionMethod::Unknown(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::UnknownPartitioning(
                                partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    _ => Err(BallistaError::General(
                        "Invalid partitioning scheme".to_owned(),
                    )),
                }
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(GlobalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::Window(window_agg) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(window_agg.input)?;
                let input_schema = window_agg
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        BallistaError::General(
                            "input_schema in WindowAggrNode is missing.".to_owned(),
                        )
                    })?
                    .clone();
                let physical_schema: SchemaRef =
                    SchemaRef::new((&input_schema).try_into()?);

                let physical_window_expr: Vec<Arc<dyn WindowExpr>> = window_agg
                    .window_expr
                    .iter()
                    .zip(window_agg.window_expr_name.iter())
                    .map(|(expr, name)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty window physical expression")
                        })?;

                        match expr_type {
                            ExprType::WindowExpr(window_node) => Ok(create_window_expr(
                                &convert_required!(window_node.window_function)?,
                                name.to_owned(),
                                &[convert_box_required!(window_node.expr)?],
                                &[],
                                &[],
                                Some(WindowFrame::default()),
                                &physical_schema,
                            )?),
                            _ => Err(BallistaError::General(
                                "Invalid expression for WindowAggrExec".to_string(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(WindowAggExec::try_new(
                    physical_window_expr,
                    input,
                    Arc::new((&input_schema).try_into()?),
                )?))
            }
            PhysicalPlanType::HashAggregate(hash_agg) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(hash_agg.input)?;
                let mode = protobuf::AggregateMode::from_i32(hash_agg.mode).ok_or_else(|| {
                    proto_error(format!(
                        "Received a HashAggregateNode message with unknown AggregateMode {}",
                        hash_agg.mode
                    ))
                })?;
                let agg_mode: AggregateMode = match mode {
                    protobuf::AggregateMode::Partial => AggregateMode::Partial,
                    protobuf::AggregateMode::Final => AggregateMode::Final,
                    protobuf::AggregateMode::FinalPartitioned => {
                        AggregateMode::FinalPartitioned
                    }
                };
                let group = hash_agg
                    .group_expr
                    .iter()
                    .zip(hash_agg.group_expr_name.iter())
                    .map(|(expr, name)| {
                        expr.try_into().map(|expr| (expr, name.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let input_schema = hash_agg
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        BallistaError::General(
                            "input_schema in HashAggregateNode is missing.".to_owned(),
                        )
                    })?
                    .clone();
                let physical_schema: SchemaRef =
                    SchemaRef::new((&input_schema).try_into()?);

                let physical_aggr_expr: Vec<Arc<dyn AggregateExpr>> = hash_agg
                    .aggr_expr
                    .iter()
                    .zip(hash_agg.aggr_expr_name.iter())
                    .map(|(expr, name)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty aggregate physical expression")
                        })?;

                        match expr_type {
                            ExprType::AggregateExpr(agg_node) => {
                                let aggr_function =
                                    protobuf::AggregateFunction::from_i32(
                                        agg_node.aggr_function,
                                    )
                                    .ok_or_else(
                                        || {
                                            proto_error(format!(
                                                "Received an unknown aggregate function: {}",
                                                agg_node.aggr_function
                                            ))
                                        },
                                    )?;

                                Ok(create_aggregate_expr(
                                    &aggr_function.into(),
                                    false,
                                    &[convert_box_required!(agg_node.expr)?],
                                    &physical_schema,
                                    name.to_string(),
                                )?)
                            }
                            _ => Err(BallistaError::General(
                                "Invalid aggregate  expression for HashAggregateExec"
                                    .to_string(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(HashAggregateExec::try_new(
                    agg_mode,
                    group,
                    physical_aggr_expr,
                    input,
                    Arc::new((&input_schema).try_into()?),
                )?))
            }
            PhysicalPlanType::HashJoin(hashjoin) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(hashjoin.left)?;
                let right: Arc<dyn ExecutionPlan> =
                    convert_box_required!(hashjoin.right)?;
                let on: Vec<(Column, Column)> = hashjoin
                    .on
                    .iter()
                    .map(|col| {
                        let left = into_required!(col.left)?;
                        let right = into_required!(col.right)?;
                        Ok((left, right))
                    })
                    .collect::<Result<_, Self::Error>>()?;
                let join_type = protobuf::JoinType::from_i32(hashjoin.join_type)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received a HashJoinNode message with unknown JoinType {}",
                            hashjoin.join_type
                        ))
                    })?;

                let partition_mode =
                    protobuf::PartitionMode::from_i32(hashjoin.partition_mode)
                        .ok_or_else(|| {
                            proto_error(format!(
                        "Received a HashJoinNode message with unknown PartitionMode {}",
                        hashjoin.partition_mode
                    ))
                        })?;
                let partition_mode = match partition_mode {
                    protobuf::PartitionMode::CollectLeft => PartitionMode::CollectLeft,
                    protobuf::PartitionMode::Partitioned => PartitionMode::Partitioned,
                };
                Ok(Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    on,
                    &join_type.into(),
                    partition_mode,
                )?))
            }
            PhysicalPlanType::CrossJoin(crossjoin) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(crossjoin.left)?;
                let right: Arc<dyn ExecutionPlan> =
                    convert_box_required!(crossjoin.right)?;
                Ok(Arc::new(CrossJoinExec::try_new(left, right)?))
            }
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(shuffle_writer.input)?;

                let output_partitioning = parse_protobuf_hash_partitioning(
                    shuffle_writer.output_partitioning.as_ref(),
                )?;

                Ok(Arc::new(ShuffleWriterExec::try_new(
                    shuffle_writer.job_id.clone(),
                    shuffle_writer.stage_id as usize,
                    input,
                    "".to_string(), // this is intentional but hacky - the executor will fill this in
                    output_partitioning,
                )?))
            }
            PhysicalPlanType::ShuffleReader(shuffle_reader) => {
                let schema = Arc::new(convert_required!(shuffle_reader.schema)?);
                let partition_location: Vec<Vec<PartitionLocation>> = shuffle_reader
                    .partition
                    .iter()
                    .map(|p| {
                        p.location
                            .iter()
                            .map(|l| l.clone().try_into())
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, BallistaError>>()?;
                let shuffle_reader =
                    ShuffleReaderExec::try_new(partition_location, schema)?;
                Ok(Arc::new(shuffle_reader))
            }
            PhysicalPlanType::Empty(empty) => {
                let schema = Arc::new(convert_required!(empty.schema)?);
                Ok(Arc::new(EmptyExec::new(empty.produce_one_row, schema)))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(sort.input)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {:?}",
                                self
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: expr.try_into()?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(BallistaError::General(format!(
                                "physical_plan::from_proto() {:?}",
                                self
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(SortExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Unresolved(unresolved_shuffle) => {
                let schema = Arc::new(convert_required!(unresolved_shuffle.schema)?);
                Ok(Arc::new(UnresolvedShuffleExec {
                    stage_id: unresolved_shuffle.stage_id as usize,
                    schema,
                    input_partition_count: unresolved_shuffle.input_partition_count
                        as usize,
                    output_partition_count: unresolved_shuffle.output_partition_count
                        as usize,
                }))
            }
        }
    }
}

impl TryInto<ParquetPartition> for &protobuf::ParquetPartition {
    type Error = BallistaError;

    fn try_into(self) -> Result<ParquetPartition, Self::Error> {
        let files = self
            .files
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ParquetPartition::new(
            files,
            self.index as usize,
            ExecutionPlanMetricsSet::new(),
        ))
    }
}

impl From<&protobuf::PhysicalColumn> for Column {
    fn from(c: &protobuf::PhysicalColumn) -> Column {
        Column::new(&c.name, c.index as usize)
    }
}

impl From<&protobuf::ScalarFunction> for BuiltinScalarFunction {
    fn from(f: &protobuf::ScalarFunction) -> BuiltinScalarFunction {
        use protobuf::ScalarFunction;
        match f {
            ScalarFunction::Sqrt => BuiltinScalarFunction::Sqrt,
            ScalarFunction::Sin => BuiltinScalarFunction::Sin,
            ScalarFunction::Cos => BuiltinScalarFunction::Cos,
            ScalarFunction::Tan => BuiltinScalarFunction::Tan,
            ScalarFunction::Asin => BuiltinScalarFunction::Asin,
            ScalarFunction::Acos => BuiltinScalarFunction::Acos,
            ScalarFunction::Atan => BuiltinScalarFunction::Atan,
            ScalarFunction::Exp => BuiltinScalarFunction::Exp,
            ScalarFunction::Log => BuiltinScalarFunction::Log,
            ScalarFunction::Log2 => BuiltinScalarFunction::Log2,
            ScalarFunction::Log10 => BuiltinScalarFunction::Log10,
            ScalarFunction::Floor => BuiltinScalarFunction::Floor,
            ScalarFunction::Ceil => BuiltinScalarFunction::Ceil,
            ScalarFunction::Round => BuiltinScalarFunction::Round,
            ScalarFunction::Trunc => BuiltinScalarFunction::Trunc,
            ScalarFunction::Abs => BuiltinScalarFunction::Abs,
            ScalarFunction::Signum => BuiltinScalarFunction::Signum,
            ScalarFunction::Octetlength => BuiltinScalarFunction::OctetLength,
            ScalarFunction::Concat => BuiltinScalarFunction::Concat,
            ScalarFunction::Lower => BuiltinScalarFunction::Lower,
            ScalarFunction::Upper => BuiltinScalarFunction::Upper,
            ScalarFunction::Trim => BuiltinScalarFunction::Trim,
            ScalarFunction::Ltrim => BuiltinScalarFunction::Ltrim,
            ScalarFunction::Rtrim => BuiltinScalarFunction::Rtrim,
            ScalarFunction::Totimestamp => BuiltinScalarFunction::ToTimestamp,
            ScalarFunction::Array => BuiltinScalarFunction::Array,
            ScalarFunction::Nullif => BuiltinScalarFunction::NullIf,
            ScalarFunction::Datepart => BuiltinScalarFunction::DatePart,
            ScalarFunction::Datetrunc => BuiltinScalarFunction::DateTrunc,
            ScalarFunction::Md5 => BuiltinScalarFunction::MD5,
            ScalarFunction::Sha224 => BuiltinScalarFunction::SHA224,
            ScalarFunction::Sha256 => BuiltinScalarFunction::SHA256,
            ScalarFunction::Sha384 => BuiltinScalarFunction::SHA384,
            ScalarFunction::Sha512 => BuiltinScalarFunction::SHA512,
            ScalarFunction::Ln => BuiltinScalarFunction::Ln,
            ScalarFunction::Totimestampmillis => BuiltinScalarFunction::ToTimestampMillis,
        }
    }
}

impl TryFrom<&protobuf::PhysicalExprNode> for Arc<dyn PhysicalExpr> {
    type Error = BallistaError;

    fn try_from(expr: &protobuf::PhysicalExprNode) -> Result<Self, Self::Error> {
        let expr_type = expr
            .expr_type
            .as_ref()
            .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

        let pexpr: Arc<dyn PhysicalExpr> = match expr_type {
            ExprType::Column(c) => {
                let pcol: Column = c.into();
                Arc::new(pcol)
            }
            ExprType::Literal(scalar) => {
                Arc::new(Literal::new(convert_required!(scalar.value)?))
            }
            ExprType::BinaryExpr(binary_expr) => Arc::new(BinaryExpr::new(
                convert_box_required!(&binary_expr.l)?,
                from_proto_binary_op(&binary_expr.op)?,
                convert_box_required!(&binary_expr.r)?,
            )),
            ExprType::AggregateExpr(_) => {
                return Err(BallistaError::General(
                    "Cannot convert aggregate expr node to physical expression"
                        .to_owned(),
                ));
            }
            ExprType::WindowExpr(_) => {
                return Err(BallistaError::General(
                    "Cannot convert window expr node to physical expression".to_owned(),
                ));
            }
            ExprType::Sort(_) => {
                return Err(BallistaError::General(
                    "Cannot convert sort expr node to physical expression".to_owned(),
                ));
            }
            ExprType::IsNullExpr(e) => {
                Arc::new(IsNullExpr::new(convert_box_required!(e.expr)?))
            }
            ExprType::IsNotNullExpr(e) => {
                Arc::new(IsNotNullExpr::new(convert_box_required!(e.expr)?))
            }
            ExprType::NotExpr(e) => {
                Arc::new(NotExpr::new(convert_box_required!(e.expr)?))
            }
            ExprType::Negative(e) => {
                Arc::new(NegativeExpr::new(convert_box_required!(e.expr)?))
            }
            ExprType::InList(e) => Arc::new(InListExpr::new(
                convert_box_required!(e.expr)?,
                e.list
                    .iter()
                    .map(|x| x.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                e.negated,
            )),
            ExprType::Case(e) => Arc::new(CaseExpr::try_new(
                e.expr.as_ref().map(|e| e.as_ref().try_into()).transpose()?,
                e.when_then_expr
                    .iter()
                    .map(|e| {
                        Ok((
                            convert_required!(e.when_expr)?,
                            convert_required!(e.then_expr)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, BallistaError>>()?
                    .as_slice(),
                e.else_expr
                    .as_ref()
                    .map(|e| e.as_ref().try_into())
                    .transpose()?,
            )?),
            ExprType::Cast(e) => Arc::new(CastExpr::new(
                convert_box_required!(e.expr)?,
                convert_required!(e.arrow_type)?,
                DEFAULT_DATAFUSION_CAST_OPTIONS,
            )),
            ExprType::TryCast(e) => Arc::new(TryCastExpr::new(
                convert_box_required!(e.expr)?,
                convert_required!(e.arrow_type)?,
            )),
            ExprType::ScalarFunction(e) => {
                let scalar_function = protobuf::ScalarFunction::from_i32(e.fun)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received an unknown scalar function: {}",
                            e.fun,
                        ))
                    })?;

                let args = e
                    .args
                    .iter()
                    .map(|x| x.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                let catalog_list =
                    Arc::new(MemoryCatalogList::new()) as Arc<dyn CatalogList>;

                let ctx_state = ExecutionContextState {
                    catalog_list,
                    scalar_functions: Default::default(),
                    var_provider: Default::default(),
                    aggregate_functions: Default::default(),
                    config: ExecutionConfig::new(),
                    execution_props: ExecutionProps::new(),
                    object_store_registry: Arc::new(ObjectStoreRegistry::new()),
                };

                let fun_expr = functions::create_physical_fun(
                    &(&scalar_function).into(),
                    &ctx_state,
                )?;

                Arc::new(ScalarFunctionExpr::new(
                    &e.name,
                    fun_expr,
                    args,
                    &convert_required!(e.return_type)?,
                ))
            }
        };

        Ok(pexpr)
    }
}

impl TryFrom<&protobuf::physical_window_expr_node::WindowFunction> for WindowFunction {
    type Error = BallistaError;

    fn try_from(
        expr: &protobuf::physical_window_expr_node::WindowFunction,
    ) -> Result<Self, Self::Error> {
        match expr {
            protobuf::physical_window_expr_node::WindowFunction::AggrFunction(n) => {
                let f = protobuf::AggregateFunction::from_i32(*n).ok_or_else(|| {
                    proto_error(format!(
                        "Received an unknown window aggregate function: {}",
                        n
                    ))
                })?;

                Ok(WindowFunction::AggregateFunction(f.into()))
            }
            protobuf::physical_window_expr_node::WindowFunction::BuiltInFunction(n) => {
                let f =
                    protobuf::BuiltInWindowFunction::from_i32(*n).ok_or_else(|| {
                        proto_error(format!(
                            "Received an unknown window builtin function: {}",
                            n
                        ))
                    })?;

                Ok(WindowFunction::BuiltInWindowFunction(f.into()))
            }
        }
    }
}

pub fn parse_protobuf_hash_partitioning(
    partitioning: Option<&protobuf::PhysicalHashRepartition>,
) -> Result<Option<Partitioning>, BallistaError> {
    match partitioning {
        Some(hash_part) => {
            let expr = hash_part
                .hash_expr
                .iter()
                .map(|e| e.try_into())
                .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

            Ok(Some(Partitioning::Hash(
                expr,
                hash_part.partition_count.try_into().unwrap(),
            )))
        }
        None => Ok(None),
    }
}
