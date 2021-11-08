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
// under the License.language governing permissions and
// limitations under the License.

//! Serde code to convert Arrow schemas and DataFusion logical plans to Ballista protocol
//! buffer format, allowing DataFusion physical plans to be serialized and transmitted between
//! processes.

use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
    sync::Arc,
};

use datafusion::physical_plan::hash_join::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sort::SortExec;
use datafusion::physical_plan::{cross_join::CrossJoinExec, ColumnStatistics};
use datafusion::physical_plan::{
    expressions::{
        CaseExpr, InListExpr, IsNotNullExpr, IsNullExpr, NegativeExpr, NotExpr,
    },
    Statistics,
};
use datafusion::physical_plan::{
    expressions::{CastExpr, TryCastExpr},
    file_format::ParquetExec,
};
use datafusion::physical_plan::{file_format::AvroExec, filter::FilterExec};
use datafusion::physical_plan::{
    file_format::PhysicalPlanConfig, hash_aggregate::AggregateMode,
};
use datafusion::{
    datasource::PartitionedFile, physical_plan::coalesce_batches::CoalesceBatchesExec,
};
use datafusion::{logical_plan::JoinType, physical_plan::file_format::CsvExec};
use datafusion::{
    physical_plan::expressions::{Count, Literal},
    scalar::ScalarValue,
};

use datafusion::physical_plan::{
    empty::EmptyExec,
    expressions::{Avg, BinaryExpr, Column, Max, Min, Sum},
    Partitioning,
};
use datafusion::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr};

use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use protobuf::physical_plan_node::PhysicalPlanType;

use crate::serde::protobuf::repartition_exec_node::PartitionMethod;
use crate::serde::scheduler::PartitionLocation;
use crate::serde::{protobuf, BallistaError};
use crate::{
    execution_plans::{ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec},
    serde::byte_to_string,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::functions::{BuiltinScalarFunction, ScalarFunctionExpr};
use datafusion::physical_plan::repartition::RepartitionExec;

impl TryInto<protobuf::PhysicalPlanNode> for Arc<dyn ExecutionPlan> {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PhysicalPlanNode, Self::Error> {
        let plan = self.as_any();

        if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| expr.0.clone().try_into())
                .collect::<Result<Vec<_>, Self::Error>>()?;
            let expr_name = exec.expr().iter().map(|expr| expr.1.clone()).collect();
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Projection(Box::new(
                    protobuf::ProjectionExecNode {
                        input: Some(Box::new(input)),
                        expr,
                        expr_name,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<FilterExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Filter(Box::new(
                    protobuf::FilterExecNode {
                        input: Some(Box::new(input)),
                        expr: Some(exec.predicate().clone().try_into()?),
                    },
                ))),
            })
        } else if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
            let input: protobuf::PhysicalPlanNode =
                limit.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::GlobalLimit(Box::new(
                    protobuf::GlobalLimitExecNode {
                        input: Some(Box::new(input)),
                        limit: limit.limit() as u32,
                    },
                ))),
            })
        } else if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
            let input: protobuf::PhysicalPlanNode =
                limit.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::LocalLimit(Box::new(
                    protobuf::LocalLimitExecNode {
                        input: Some(Box::new(input)),
                        limit: limit.limit() as u32,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<HashJoinExec>() {
            let left: protobuf::PhysicalPlanNode = exec.left().to_owned().try_into()?;
            let right: protobuf::PhysicalPlanNode = exec.right().to_owned().try_into()?;
            let on: Vec<protobuf::JoinOn> = exec
                .on()
                .iter()
                .map(|tuple| protobuf::JoinOn {
                    left: Some(protobuf::PhysicalColumn {
                        name: tuple.0.name().to_string(),
                        index: tuple.0.index() as u32,
                    }),
                    right: Some(protobuf::PhysicalColumn {
                        name: tuple.1.name().to_string(),
                        index: tuple.1.index() as u32,
                    }),
                })
                .collect();
            let join_type: protobuf::JoinType = exec.join_type().to_owned().into();

            let partition_mode = match exec.partition_mode() {
                PartitionMode::CollectLeft => protobuf::PartitionMode::CollectLeft,
                PartitionMode::Partitioned => protobuf::PartitionMode::Partitioned,
            };

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::HashJoin(Box::new(
                    protobuf::HashJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        on,
                        join_type: join_type.into(),
                        partition_mode: partition_mode.into(),
                        null_equals_null: *exec.null_equals_null(),
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<CrossJoinExec>() {
            let left: protobuf::PhysicalPlanNode = exec.left().to_owned().try_into()?;
            let right: protobuf::PhysicalPlanNode = exec.right().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CrossJoin(Box::new(
                    protobuf::CrossJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<HashAggregateExec>() {
            let groups = exec
                .group_expr()
                .iter()
                .map(|expr| expr.0.to_owned().try_into())
                .collect::<Result<Vec<_>, BallistaError>>()?;
            let group_names = exec
                .group_expr()
                .iter()
                .map(|expr| expr.1.to_owned())
                .collect();
            let agg = exec
                .aggr_expr()
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>, BallistaError>>()?;
            let agg_names = exec
                .aggr_expr()
                .iter()
                .map(|expr| match expr.field() {
                    Ok(field) => Ok(field.name().clone()),
                    Err(e) => Err(BallistaError::DataFusionError(e)),
                })
                .collect::<Result<_, Self::Error>>()?;

            let agg_mode = match exec.mode() {
                AggregateMode::Partial => protobuf::AggregateMode::Partial,
                AggregateMode::Final => protobuf::AggregateMode::Final,
                AggregateMode::FinalPartitioned => {
                    protobuf::AggregateMode::FinalPartitioned
                }
            };
            let input_schema = exec.input_schema();
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::HashAggregate(Box::new(
                    protobuf::HashAggregateExecNode {
                        group_expr: groups,
                        group_expr_name: group_names,
                        aggr_expr: agg,
                        aggr_expr_name: agg_names,
                        mode: agg_mode as i32,
                        input: Some(Box::new(input)),
                        input_schema: Some(input_schema.as_ref().into()),
                    },
                ))),
            })
        } else if let Some(empty) = plan.downcast_ref::<EmptyExec>() {
            let schema = empty.schema().as_ref().into();
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Empty(
                    protobuf::EmptyExecNode {
                        produce_one_row: empty.produce_one_row(),
                        schema: Some(schema),
                    },
                )),
            })
        } else if let Some(coalesce_batches) = plan.downcast_ref::<CoalesceBatchesExec>()
        {
            let input: protobuf::PhysicalPlanNode =
                coalesce_batches.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CoalesceBatches(Box::new(
                    protobuf::CoalesceBatchesExecNode {
                        input: Some(Box::new(input)),
                        target_batch_size: coalesce_batches.target_batch_size() as u32,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<CsvExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CsvScan(
                    protobuf::CsvScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                        has_header: exec.has_header(),
                        delimiter: byte_to_string(exec.delimiter())?,
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<ParquetExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ParquetScan(
                    protobuf::ParquetScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                        // TODO serialize predicates
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<AvroExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::AvroScan(
                    protobuf::AvroScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<ShuffleReaderExec>() {
            let mut partition = vec![];
            for location in &exec.partition {
                partition.push(protobuf::ShuffleReaderPartition {
                    location: location
                        .iter()
                        .map(|l| l.clone().try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                });
            }
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleReader(
                    protobuf::ShuffleReaderExecNode {
                        partition,
                        schema: Some(exec.schema().as_ref().into()),
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<CoalescePartitionsExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Merge(Box::new(
                    protobuf::CoalescePartitionsExecNode {
                        input: Some(Box::new(input)),
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<RepartitionExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;

            let pb_partition_method = match exec.partitioning() {
                Partitioning::Hash(exprs, partition_count) => {
                    PartitionMethod::Hash(protobuf::PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr| expr.clone().try_into())
                            .collect::<Result<Vec<_>, BallistaError>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                Partitioning::RoundRobinBatch(partition_count) => {
                    PartitionMethod::RoundRobin(*partition_count as u64)
                }
                Partitioning::UnknownPartitioning(partition_count) => {
                    PartitionMethod::Unknown(*partition_count as u64)
                }
            };

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Repartition(Box::new(
                    protobuf::RepartitionExecNode {
                        input: Some(Box::new(input)),
                        partition_method: Some(pb_partition_method),
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<SortExec>() {
            let input: protobuf::PhysicalPlanNode = exec.input().to_owned().try_into()?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| {
                    let sort_expr = Box::new(protobuf::PhysicalSortExprNode {
                        expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                        asc: !expr.options.descending,
                        nulls_first: expr.options.nulls_first,
                    });
                    Ok(protobuf::PhysicalExprNode {
                        expr_type: Some(protobuf::physical_expr_node::ExprType::Sort(
                            sort_expr,
                        )),
                    })
                })
                .collect::<Result<Vec<_>, Self::Error>>()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Sort(Box::new(
                    protobuf::SortExecNode {
                        input: Some(Box::new(input)),
                        expr,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<ShuffleWriterExec>() {
            let input: protobuf::PhysicalPlanNode =
                exec.children()[0].to_owned().try_into()?;
            // note that we use shuffle_output_partitioning() rather than output_partitioning()
            // to get the true output partitioning
            let output_partitioning = match exec.shuffle_output_partitioning() {
                Some(Partitioning::Hash(exprs, partition_count)) => {
                    Some(protobuf::PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr| expr.clone().try_into())
                            .collect::<Result<Vec<_>, BallistaError>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                None => None,
                other => {
                    return Err(BallistaError::General(format!(
                        "physical_plan::to_proto() invalid partitioning for ShuffleWriterExec: {:?}",
                        other
                    )))
                }
            };
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleWriter(Box::new(
                    protobuf::ShuffleWriterExecNode {
                        job_id: exec.job_id().to_string(),
                        stage_id: exec.stage_id() as u32,
                        input: Some(Box::new(input)),
                        output_partitioning,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<UnresolvedShuffleExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Unresolved(
                    protobuf::UnresolvedShuffleExecNode {
                        stage_id: exec.stage_id as u32,
                        schema: Some(exec.schema().as_ref().into()),
                        input_partition_count: exec.input_partition_count as u32,
                        output_partition_count: exec.output_partition_count as u32,
                    },
                )),
            })
        } else {
            Err(BallistaError::General(format!(
                "physical plan to_proto unsupported plan {:?}",
                self
            )))
        }
    }
}

impl TryInto<protobuf::PhysicalExprNode> for Arc<dyn AggregateExpr> {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PhysicalExprNode, Self::Error> {
        let aggr_function = if self.as_any().downcast_ref::<Avg>().is_some() {
            Ok(protobuf::AggregateFunction::Avg.into())
        } else if self.as_any().downcast_ref::<Sum>().is_some() {
            Ok(protobuf::AggregateFunction::Sum.into())
        } else if self.as_any().downcast_ref::<Count>().is_some() {
            Ok(protobuf::AggregateFunction::Count.into())
        } else if self.as_any().downcast_ref::<Min>().is_some() {
            Ok(protobuf::AggregateFunction::Min.into())
        } else if self.as_any().downcast_ref::<Max>().is_some() {
            Ok(protobuf::AggregateFunction::Max.into())
        } else {
            Err(BallistaError::NotImplemented(format!(
                "Aggregate function not supported: {:?}",
                self
            )))
        }?;
        let expressions: Vec<protobuf::PhysicalExprNode> = self
            .expressions()
            .iter()
            .map(|e| e.clone().try_into())
            .collect::<Result<Vec<_>, BallistaError>>()?;
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
                Box::new(protobuf::PhysicalAggregateExprNode {
                    aggr_function,
                    expr: Some(Box::new(expressions[0].clone())),
                }),
            )),
        })
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for protobuf::PhysicalExprNode {
    type Error = BallistaError;

    fn try_from(value: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
        let expr = value.as_any();

        if let Some(expr) = expr.downcast_ref::<Column>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::Column(
                    protobuf::PhysicalColumn {
                        name: expr.name().to_string(),
                        index: expr.index() as u32,
                    },
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
            let binary_expr = Box::new(protobuf::PhysicalBinaryExprNode {
                l: Some(Box::new(expr.left().to_owned().try_into()?)),
                r: Some(Box::new(expr.right().to_owned().try_into()?)),
                op: format!("{:?}", expr.op()),
            });

            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::BinaryExpr(
                    binary_expr,
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<CaseExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::Case(
                        Box::new(
                            protobuf::PhysicalCaseNode {
                                expr: expr
                                    .expr()
                                    .as_ref()
                                    .map(|exp| exp.clone().try_into().map(Box::new))
                                    .transpose()?,
                                when_then_expr: expr
                                    .when_then_expr()
                                    .iter()
                                    .map(|(when_expr, then_expr)| {
                                        try_parse_when_then_expr(when_expr, then_expr)
                                    })
                                    .collect::<Result<
                                        Vec<protobuf::PhysicalWhenThen>,
                                        Self::Error,
                                    >>()?,
                                else_expr: expr
                                    .else_expr()
                                    .map(|a| a.clone().try_into().map(Box::new))
                                    .transpose()?,
                            },
                        ),
                    ),
                ),
            })
        } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::NotExpr(
                    Box::new(protobuf::PhysicalNot {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::IsNullExpr(
                    Box::new(protobuf::PhysicalIsNull {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::IsNotNullExpr(
                    Box::new(protobuf::PhysicalIsNotNull {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::InList(
                        Box::new(
                            protobuf::PhysicalInListNode {
                                expr: Some(Box::new(expr.expr().to_owned().try_into()?)),
                                list: expr
                                    .list()
                                    .iter()
                                    .map(|a| a.clone().try_into())
                                    .collect::<Result<
                                    Vec<protobuf::PhysicalExprNode>,
                                    Self::Error,
                                >>()?,
                                negated: expr.negated(),
                            },
                        ),
                    ),
                ),
            })
        } else if let Some(expr) = expr.downcast_ref::<NegativeExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::Negative(
                    Box::new(protobuf::PhysicalNegativeNode {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(lit) = expr.downcast_ref::<Literal>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::Literal(
                    lit.value().try_into()?,
                )),
            })
        } else if let Some(cast) = expr.downcast_ref::<CastExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::Cast(Box::new(
                    protobuf::PhysicalCastNode {
                        expr: Some(Box::new(cast.expr().clone().try_into()?)),
                        arrow_type: Some(cast.cast_type().into()),
                    },
                ))),
            })
        } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::TryCast(
                    Box::new(protobuf::PhysicalTryCastNode {
                        expr: Some(Box::new(cast.expr().clone().try_into()?)),
                        arrow_type: Some(cast.cast_type().into()),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
            let fun: BuiltinScalarFunction =
                BuiltinScalarFunction::from_str(expr.name())?;
            let fun: protobuf::ScalarFunction = (&fun).try_into()?;
            let args: Vec<protobuf::PhysicalExprNode> = expr
                .args()
                .iter()
                .map(|e| e.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarFunction(
                    protobuf::PhysicalScalarFunctionNode {
                        name: expr.name().to_string(),
                        fun: fun.into(),
                        args,
                        return_type: Some(expr.return_type().into()),
                    },
                )),
            })
        } else {
            Err(BallistaError::General(format!(
                "physical_plan::to_proto() unsupported expression {:?}",
                value
            )))
        }
    }
}

fn try_parse_when_then_expr(
    when_expr: &Arc<dyn PhysicalExpr>,
    then_expr: &Arc<dyn PhysicalExpr>,
) -> Result<protobuf::PhysicalWhenThen, BallistaError> {
    Ok(protobuf::PhysicalWhenThen {
        when_expr: Some(when_expr.clone().try_into()?),
        then_expr: Some(then_expr.clone().try_into()?),
    })
}

impl TryFrom<&PartitionedFile> for protobuf::PartitionedFile {
    type Error = BallistaError;

    fn try_from(pf: &PartitionedFile) -> Result<Self, Self::Error> {
        Ok(protobuf::PartitionedFile {
            path: pf.file_meta.path().to_owned(),
            size: pf.file_meta.size(),
            last_modified_ns: pf
                .file_meta
                .last_modified
                .map(|ts| ts.timestamp_nanos() as u64)
                .unwrap_or(0),
            partition_values: pf
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<&[PartitionedFile]> for protobuf::FileGroup {
    type Error = BallistaError;

    fn try_from(gr: &[PartitionedFile]) -> Result<Self, Self::Error> {
        Ok(protobuf::FileGroup {
            files: gr
                .iter()
                .map(|f| f.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<&ColumnStatistics> for protobuf::ColumnStats {
    fn from(cs: &ColumnStatistics) -> protobuf::ColumnStats {
        protobuf::ColumnStats {
            min_value: cs.min_value.as_ref().map(|m| m.try_into().unwrap()),
            max_value: cs.max_value.as_ref().map(|m| m.try_into().unwrap()),
            null_count: cs.null_count.map(|n| n as u32).unwrap_or(0),
            distinct_count: cs.distinct_count.map(|n| n as u32).unwrap_or(0),
        }
    }
}

impl From<&Statistics> for protobuf::Statistics {
    fn from(s: &Statistics) -> protobuf::Statistics {
        let none_value = -1_i64;
        let column_stats = match &s.column_statistics {
            None => vec![],
            Some(column_stats) => column_stats.iter().map(|s| s.into()).collect(),
        };
        protobuf::Statistics {
            num_rows: s.num_rows.map(|n| n as i64).unwrap_or(none_value),
            total_byte_size: s.total_byte_size.map(|n| n as i64).unwrap_or(none_value),
            column_stats,
            is_exact: s.is_exact,
        }
    }
}

impl TryFrom<&PhysicalPlanConfig> for protobuf::FileScanExecConf {
    type Error = BallistaError;
    fn try_from(
        conf: &PhysicalPlanConfig,
    ) -> Result<protobuf::FileScanExecConf, Self::Error> {
        let file_groups = conf
            .file_groups
            .iter()
            .map(|p| p.as_slice().try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(protobuf::FileScanExecConf {
            file_groups,
            statistics: Some((&conf.statistics).into()),
            limit: conf.limit.map(|l| protobuf::ScanLimit { limit: l as u32 }),
            projection: conf
                .projection
                .as_ref()
                .unwrap_or(&vec![])
                .iter()
                .map(|n| *n as u32)
                .collect(),
            schema: Some(conf.file_schema.as_ref().into()),
            batch_size: conf.batch_size as u32,
            table_partition_cols: conf.table_partition_cols.to_vec(),
        })
    }
}
