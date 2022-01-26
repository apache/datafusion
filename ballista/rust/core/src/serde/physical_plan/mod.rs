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

use crate::error::BallistaError;
use crate::execution_plans::{
    ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec,
};
use crate::serde::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use crate::serde::protobuf::physical_expr_node::ExprType;
use crate::serde::protobuf::physical_plan_node::PhysicalPlanType;
use crate::serde::protobuf::repartition_exec_node::PartitionMethod;
use crate::serde::protobuf::PhysicalPlanNode;
use crate::serde::protobuf::ShuffleReaderPartition;
use crate::serde::scheduler::PartitionLocation;
use crate::serde::{proto_error, protobuf, str_to_byte, AsExecutionPlan};
use crate::{convert_box_required, convert_required, into_physical_plan, into_required};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::object_store::local::LocalFileSystem;
use datafusion::datasource::PartitionedFile;
use datafusion::logical_plan::window_frames::WindowFrame;
use datafusion::physical_plan::aggregates::create_aggregate_expr;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::cross_join::CrossJoinExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::file_format::{
    AvroExec, CsvExec, FileScanConfig, ParquetExec,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::hash_join::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{create_window_expr, WindowAggExec};
use datafusion::physical_plan::{
    AggregateExpr, ExecutionPlan, Partitioning, PhysicalExpr, WindowExpr,
};
use datafusion::prelude::ExecutionContext;
use prost::bytes::BufMut;
use prost::Message;
use std::convert::TryInto;
use std::sync::Arc;

pub mod from_proto;
pub mod to_proto;

impl AsExecutionPlan for PhysicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self, BallistaError>
    where
        Self: Sized,
    {
        PhysicalPlanNode::decode(buf).map_err(|e| {
            BallistaError::Internal(format!("failed to decode physical plan: {:?}", e))
        })
    }

    fn try_encode<B>(&self, buf: &mut B) -> Result<(), BallistaError>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf).map_err(|e| {
            BallistaError::Internal(format!("failed to encode physical plan: {:?}", e))
        })
    }

    fn try_into_physical_plan(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(projection.input, &ctx)?;
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .map(|(expr, name)| Ok((expr.try_into()?, name.to_string())))
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, BallistaError>>(
                    )?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(filter.input, &ctx)?;
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
            PhysicalPlanType::CsvScan(scan) => Ok(Arc::new(CsvExec::new(
                scan.base_conf.as_ref().unwrap().try_into()?,
                scan.has_header,
                str_to_byte(&scan.delimiter)?,
            ))),
            PhysicalPlanType::ParquetScan(scan) => {
                Ok(Arc::new(ParquetExec::new(
                    scan.base_conf.as_ref().unwrap().try_into()?,
                    // TODO predicate should be de-serialized
                    None,
                )))
            }
            PhysicalPlanType::AvroScan(scan) => Ok(Arc::new(AvroExec::new(
                scan.base_conf.as_ref().unwrap().try_into()?,
            ))),
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(coalesce_batches.input, &ctx)?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::Merge(merge) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(merge.input, &ctx)?;
                Ok(Arc::new(CoalescePartitionsExec::new(input)))
            }
            PhysicalPlanType::Repartition(repart) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(repart.input, &ctx)?;
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
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(limit.input, &ctx)?;
                Ok(Arc::new(GlobalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(limit.input, &ctx)?;
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
                    into_physical_plan!(hash_agg.input, &ctx)?;
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
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(hashjoin.left, &ctx)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(hashjoin.right, &ctx)?;
                let on: Vec<(Column, Column)> = hashjoin
                    .on
                    .iter()
                    .map(|col| {
                        let left = into_required!(col.left)?;
                        let right = into_required!(col.right)?;
                        Ok((left, right))
                    })
                    .collect::<Result<_, BallistaError>>()?;
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
                    &hashjoin.null_equals_null,
                )?))
            }
            PhysicalPlanType::CrossJoin(crossjoin) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(crossjoin.left, &ctx)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(crossjoin.right, &ctx)?;
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
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(sort.input, &ctx)?;
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

    fn try_from_physical_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Self, BallistaError>
    where
        Self: Sized,
    {
        plan.try_into()
    }
}

fn decode_scan_config(
    proto: &protobuf::FileScanExecConf,
    ctx: &ExecutionContext,
) -> Result<FileScanConfig, BallistaError> {
    let schema = Arc::new(convert_required!(proto.schema)?);
    let projection = proto
        .projection
        .iter()
        .map(|i| *i as usize)
        .collect::<Vec<_>>();
    let projection = if projection.is_empty() {
        None
    } else {
        Some(projection)
    };
    let statistics = convert_required!(proto.statistics)?;

    let file_groups: Vec<Vec<PartitionedFile>> = proto
        .file_groups
        .iter()
        .map(|f| f.try_into())
        .collect::<Result<Vec<_>, _>>()?;

    let object_store = if let Some(file) = file_groups.get(0).and_then(|h| h.get(0)) {
        ctx.object_store(file.file_meta.path())?.0
    } else {
        Arc::new(LocalFileSystem {})
    };

    Ok(FileScanConfig {
        object_store,
        file_schema: schema,
        file_groups,
        statistics,
        projection,
        limit: proto.limit.as_ref().map(|sl| sl.limit as usize),
        table_partition_cols: vec![],
    })
}

#[macro_export]
macro_rules! into_physical_plan {
    ($PB:expr, $CTX:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into_physical_plan(&$CTX)
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[cfg(test)]
mod roundtrip_tests {
    use std::{convert::TryInto, sync::Arc};

    use crate::serde::AsExecutionPlan;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::ExecutionContext;
    use datafusion::{
        arrow::{
            compute::kernels::sort::SortOptions,
            datatypes::{DataType, Field, Schema},
        },
        logical_plan::{JoinType, Operator},
        physical_plan::{
            empty::EmptyExec,
            expressions::{binary, col, lit, InListExpr, NotExpr},
            expressions::{Avg, Column, PhysicalSortExpr},
            filter::FilterExec,
            hash_aggregate::{AggregateMode, HashAggregateExec},
            hash_join::{HashJoinExec, PartitionMode},
            limit::{GlobalLimitExec, LocalLimitExec},
            AggregateExpr, ColumnarValue, Distribution, ExecutionPlan, Partitioning,
            PhysicalExpr,
        },
        scalar::ScalarValue,
    };

    use super::super::super::error::Result;
    use super::super::protobuf;
    use crate::execution_plans::ShuffleWriterExec;

    fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let ctx = ExecutionContext::new();
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone())
                .expect("to proto"); //exec_plan.clone().try_into()?;
        let result_exec_plan: Arc<dyn ExecutionPlan> =
            proto.try_into_physical_plan(&ctx).expect("from proto");
        assert_eq!(
            format!("{:?}", exec_plan),
            format!("{:?}", result_exec_plan)
        );
        Ok(())
    }

    #[test]
    fn roundtrip_empty() -> Result<()> {
        roundtrip_test(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))))
    }

    #[test]
    fn roundtrip_local_limit() -> Result<()> {
        roundtrip_test(Arc::new(LocalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_global_limit() -> Result<()> {
        roundtrip_test(Arc::new(GlobalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_hash_join() -> Result<()> {
        let field_a = Field::new("col", DataType::Int64, false);
        let schema_left = Schema::new(vec![field_a.clone()]);
        let schema_right = Schema::new(vec![field_a]);
        let on = vec![(
            Column::new("col", schema_left.index_of("col")?),
            Column::new("col", schema_right.index_of("col")?),
        )];

        let schema_left = Arc::new(schema_left);
        let schema_right = Arc::new(schema_right);
        for join_type in &[
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Anti,
            JoinType::Semi,
        ] {
            for partition_mode in
                &[PartitionMode::Partitioned, PartitionMode::CollectLeft]
            {
                roundtrip_test(Arc::new(HashJoinExec::try_new(
                    Arc::new(EmptyExec::new(false, schema_left.clone())),
                    Arc::new(EmptyExec::new(false, schema_right.clone())),
                    on.clone(),
                    join_type,
                    *partition_mode,
                    &false,
                )?))?;
            }
        }
        Ok(())
    }

    #[test]
    fn rountrip_hash_aggregate() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &schema)?, "unused".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        roundtrip_test(Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
            groups.clone(),
            aggregates.clone(),
            Arc::new(EmptyExec::new(false, schema.clone())),
            schema,
        )?))
    }

    #[test]
    fn roundtrip_filter_with_not_and_in_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let field_c = Field::new("c", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c]));
        let not = Arc::new(NotExpr::new(col("a", &schema)?));
        let in_list = Arc::new(InListExpr::new(
            col("b", &schema)?,
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            false,
        ));
        let and = binary(not, Operator::And, in_list, &schema)?;
        roundtrip_test(Arc::new(FilterExec::try_new(
            and,
            Arc::new(EmptyExec::new(false, schema.clone())),
        )?))
    }

    #[test]
    fn roundtrip_sort() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
        ];
        roundtrip_test(Arc::new(SortExec::try_new(
            sort_exprs,
            Arc::new(EmptyExec::new(false, schema)),
        )?))
    }

    #[test]
    fn roundtrip_shuffle_writer() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        roundtrip_test(Arc::new(ShuffleWriterExec::try_new(
            "job123".to_string(),
            123,
            Arc::new(EmptyExec::new(false, schema)),
            "".to_string(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 4)),
        )?))
    }
}
