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

use datafusion::physical_plan::expressions::{CastExpr, TryCastExpr};
use datafusion::physical_plan::ColumnStatistics;
use datafusion::physical_plan::{
    expressions::{
        CaseExpr, InListExpr, IsNotNullExpr, IsNullExpr, NegativeExpr, NotExpr,
    },
    Statistics,
};

use datafusion::datasource::PartitionedFile;
use datafusion::physical_plan::file_format::FileScanConfig;

use datafusion::physical_plan::expressions::{Count, Literal};

use datafusion::physical_plan::expressions::{Avg, BinaryExpr, Column, Max, Min, Sum};
use datafusion::physical_plan::{AggregateExpr, PhysicalExpr};

use crate::serde::{protobuf, BallistaError};

use datafusion::physical_plan::functions::{BuiltinScalarFunction, ScalarFunctionExpr};

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

impl TryFrom<&FileScanConfig> for protobuf::FileScanExecConf {
    type Error = BallistaError;
    fn try_from(
        conf: &FileScanConfig,
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
            table_partition_cols: conf.table_partition_cols.to_vec(),
        })
    }
}
