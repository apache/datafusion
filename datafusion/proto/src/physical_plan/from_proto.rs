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

use crate::protobuf;
use arrow::datatypes::DataType;
use chrono::TimeZone;
use chrono::Utc;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::listing::{FileRange, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::window_function::WindowFunction;
use datafusion::physical_expr::expressions::DateTimeIntervalExpr;
use datafusion::physical_expr::{PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_plan::expressions::LikeExpr;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::{
    expressions::{
        BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr,
        Literal, NegativeExpr, NotExpr, TryCastExpr, DEFAULT_DATAFUSION_CAST_OPTIONS,
    },
    functions, Partitioning,
};
use datafusion::physical_plan::{ColumnStatistics, PhysicalExpr, Statistics};
use datafusion_common::DataFusionError;
use object_store::path::Path;
use object_store::ObjectMeta;
use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::sync::Arc;

use crate::common::proto_error;
use crate::convert_required;
use crate::logical_plan;
use crate::protobuf::physical_expr_node::ExprType;
use datafusion::physical_plan::joins::utils::JoinSide;
use datafusion::physical_plan::sorts::sort::SortOptions;

impl From<&protobuf::PhysicalColumn> for Column {
    fn from(c: &protobuf::PhysicalColumn) -> Column {
        Column::new(&c.name, c.index as usize)
    }
}

pub(crate) fn parse_physical_expr(
    proto: &protobuf::PhysicalExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

    let pexpr: Arc<dyn PhysicalExpr> = match expr_type {
        ExprType::Column(c) => {
            let pcol: Column = c.into();
            Arc::new(pcol)
        }
        ExprType::Literal(scalar) => Arc::new(Literal::new(scalar.try_into()?)),
        ExprType::BinaryExpr(binary_expr) => Arc::new(BinaryExpr::new(
            parse_required_physical_box_expr(
                &binary_expr.l,
                registry,
                "left",
                input_schema,
            )?,
            logical_plan::from_proto::from_proto_binary_op(&binary_expr.op)?,
            parse_required_physical_box_expr(
                &binary_expr.r,
                registry,
                "right",
                input_schema,
            )?,
        )),
        ExprType::DateTimeIntervalExpr(expr) => Arc::new(DateTimeIntervalExpr::try_new(
            parse_required_physical_box_expr(&expr.l, registry, "left", input_schema)?,
            logical_plan::from_proto::from_proto_binary_op(&expr.op)?,
            parse_required_physical_box_expr(&expr.r, registry, "right", input_schema)?,
            input_schema,
        )?),
        ExprType::AggregateExpr(_) => {
            return Err(DataFusionError::NotImplemented(
                "Cannot convert aggregate expr node to physical expression".to_owned(),
            ));
        }
        ExprType::WindowExpr(_) => {
            return Err(DataFusionError::NotImplemented(
                "Cannot convert window expr node to physical expression".to_owned(),
            ));
        }
        ExprType::Sort(_) => {
            return Err(DataFusionError::NotImplemented(
                "Cannot convert sort expr node to physical expression".to_owned(),
            ));
        }
        ExprType::IsNullExpr(e) => Arc::new(IsNullExpr::new(
            parse_required_physical_box_expr(&e.expr, registry, "expr", input_schema)?,
        )),
        ExprType::IsNotNullExpr(e) => Arc::new(IsNotNullExpr::new(
            parse_required_physical_box_expr(&e.expr, registry, "expr", input_schema)?,
        )),
        ExprType::NotExpr(e) => Arc::new(NotExpr::new(parse_required_physical_box_expr(
            &e.expr,
            registry,
            "expr",
            input_schema,
        )?)),
        ExprType::Negative(e) => Arc::new(NegativeExpr::new(
            parse_required_physical_box_expr(&e.expr, registry, "expr", input_schema)?,
        )),
        ExprType::InList(e) => Arc::new(InListExpr::new(
            parse_required_physical_box_expr(&e.expr, registry, "expr", input_schema)?,
            e.list
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?,
            e.negated,
            input_schema,
        )),
        ExprType::Case(e) => Arc::new(CaseExpr::try_new(
            e.expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema))
                .transpose()?,
            e.when_then_expr
                .iter()
                .map(|e| {
                    Ok((
                        parse_required_physical_expr(
                            e.when_expr.as_ref(),
                            registry,
                            "when_expr",
                            input_schema,
                        )?,
                        parse_required_physical_expr(
                            e.then_expr.as_ref(),
                            registry,
                            "then_expr",
                            input_schema,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            e.else_expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema))
                .transpose()?,
        )?),
        ExprType::Cast(e) => Arc::new(CastExpr::new(
            parse_required_physical_box_expr(&e.expr, registry, "expr", input_schema)?,
            convert_required!(e.arrow_type)?,
            DEFAULT_DATAFUSION_CAST_OPTIONS,
        )),
        ExprType::TryCast(e) => Arc::new(TryCastExpr::new(
            parse_required_physical_box_expr(&e.expr, registry, "expr", input_schema)?,
            convert_required!(e.arrow_type)?,
        )),
        ExprType::ScalarFunction(e) => {
            let scalar_function =
                protobuf::ScalarFunction::from_i32(e.fun).ok_or_else(|| {
                    proto_error(
                        format!("Received an unknown scalar function: {}", e.fun,),
                    )
                })?;

            let args = e
                .args
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?;

            // TODO Do not create new the ExecutionProps
            let execution_props = ExecutionProps::new();

            let fun_expr = functions::create_physical_fun(
                &(&scalar_function).into(),
                &execution_props,
            )?;

            Arc::new(ScalarFunctionExpr::new(
                &e.name,
                fun_expr,
                args,
                &convert_required!(e.return_type)?,
            ))
        }
        ExprType::ScalarUdf(e) => {
            let scalar_fun = registry.udf(e.name.as_str())?.deref().clone().fun;

            let args = e
                .args
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?;

            Arc::new(ScalarFunctionExpr::new(
                e.name.as_str(),
                scalar_fun,
                args,
                &convert_required!(e.return_type)?,
            ))
        }
        ExprType::LikeExpr(like_expr) => Arc::new(LikeExpr::new(
            like_expr.negated,
            like_expr.case_insensitive,
            parse_required_physical_box_expr(
                &like_expr.expr,
                registry,
                "expr",
                input_schema,
            )?,
            parse_required_physical_box_expr(
                &like_expr.pattern,
                registry,
                "pattern",
                input_schema,
            )?,
        )),
    };

    Ok(pexpr)
}

fn parse_required_physical_box_expr(
    expr: &Option<Box<protobuf::PhysicalExprNode>>,
    registry: &dyn FunctionRegistry,
    field: &str,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    expr.as_ref()
        .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema))
        .transpose()?
        .ok_or_else(|| {
            DataFusionError::Internal(format!("Missing required field {field:?}"))
        })
}

fn parse_required_physical_expr(
    expr: Option<&protobuf::PhysicalExprNode>,
    registry: &dyn FunctionRegistry,
    field: &str,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    expr.as_ref()
        .map(|e| parse_physical_expr(e, registry, input_schema))
        .transpose()?
        .ok_or_else(|| {
            DataFusionError::Internal(format!("Missing required field {field:?}"))
        })
}

impl TryFrom<&protobuf::physical_window_expr_node::WindowFunction> for WindowFunction {
    type Error = DataFusionError;

    fn try_from(
        expr: &protobuf::physical_window_expr_node::WindowFunction,
    ) -> Result<Self, Self::Error> {
        match expr {
            protobuf::physical_window_expr_node::WindowFunction::AggrFunction(n) => {
                let f = protobuf::AggregateFunction::from_i32(*n).ok_or_else(|| {
                    proto_error(format!(
                        "Received an unknown window aggregate function: {n}"
                    ))
                })?;

                Ok(WindowFunction::AggregateFunction(f.into()))
            }
            protobuf::physical_window_expr_node::WindowFunction::BuiltInFunction(n) => {
                let f =
                    protobuf::BuiltInWindowFunction::from_i32(*n).ok_or_else(|| {
                        proto_error(format!(
                            "Received an unknown window builtin function: {n}"
                        ))
                    })?;

                Ok(WindowFunction::BuiltInWindowFunction(f.into()))
            }
        }
    }
}

pub fn parse_protobuf_hash_partitioning(
    partitioning: Option<&protobuf::PhysicalHashRepartition>,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Option<Partitioning>, DataFusionError> {
    match partitioning {
        Some(hash_part) => {
            let expr = hash_part
                .hash_expr
                .iter()
                .map(|e| parse_physical_expr(e, registry, input_schema))
                .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

            Ok(Some(Partitioning::Hash(
                expr,
                hash_part.partition_count.try_into().unwrap(),
            )))
        }
        None => Ok(None),
    }
}

pub fn parse_protobuf_file_scan_config(
    proto: &protobuf::FileScanExecConf,
    registry: &dyn FunctionRegistry,
) -> Result<FileScanConfig, DataFusionError> {
    let schema: Arc<Schema> = Arc::new(convert_required!(proto.schema)?);
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

    let object_store_url = match proto.object_store_url.is_empty() {
        false => ObjectStoreUrl::parse(&proto.object_store_url)?,
        true => ObjectStoreUrl::local_filesystem(),
    };

    // extract types of partition columns
    let table_partition_cols = proto
        .table_partition_cols
        .iter()
        .map(|col| {
            Ok((
                col.to_owned(),
                schema.field_with_name(col)?.data_type().clone(),
            ))
        })
        .collect::<Result<Vec<(String, DataType)>, DataFusionError>>()?;

    let output_ordering = proto
        .output_ordering
        .iter()
        .map(|o| {
            let expr = o
                .expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, &schema))
                .unwrap()?;
            Ok(PhysicalSortExpr {
                expr,
                options: SortOptions {
                    descending: !o.asc,
                    nulls_first: o.nulls_first,
                },
            })
        })
        .collect::<Result<Vec<PhysicalSortExpr>, DataFusionError>>()?;
    let output_ordering = if output_ordering.is_empty() {
        None
    } else {
        Some(output_ordering)
    };

    Ok(FileScanConfig {
        object_store_url,
        file_schema: schema,
        file_groups,
        statistics,
        projection,
        limit: proto.limit.as_ref().map(|sl| sl.limit as usize),
        table_partition_cols,
        output_ordering,
        infinite_source: false,
    })
}

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = DataFusionError;

    fn try_from(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        Ok(PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::from(val.path.as_str()),
                last_modified: Utc.timestamp_nanos(val.last_modified_ns as i64),
                size: val.size as usize,
            },
            partition_values: val
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: val.range.as_ref().map(|v| v.try_into()).transpose()?,
            extensions: None,
        })
    }
}

impl TryFrom<&protobuf::FileRange> for FileRange {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::FileRange) -> Result<Self, Self::Error> {
        Ok(FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&protobuf::FileGroup> for Vec<PartitionedFile> {
    type Error = DataFusionError;

    fn try_from(val: &protobuf::FileGroup) -> Result<Self, Self::Error> {
        val.files
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>, _>>()
    }
}

impl From<&protobuf::ColumnStats> for ColumnStatistics {
    fn from(cs: &protobuf::ColumnStats) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Some(cs.null_count as usize),
            max_value: cs.max_value.as_ref().map(|m| m.try_into().unwrap()),
            min_value: cs.min_value.as_ref().map(|m| m.try_into().unwrap()),
            distinct_count: Some(cs.distinct_count as usize),
        }
    }
}

impl From<protobuf::JoinSide> for JoinSide {
    fn from(t: protobuf::JoinSide) -> Self {
        match t {
            protobuf::JoinSide::LeftSide => JoinSide::Left,
            protobuf::JoinSide::RightSide => JoinSide::Right,
        }
    }
}

impl TryFrom<&protobuf::Statistics> for Statistics {
    type Error = DataFusionError;

    fn try_from(s: &protobuf::Statistics) -> Result<Self, Self::Error> {
        // Keep it sync with Statistics::to_proto
        let none_value = -1_i64;
        let column_statistics =
            s.column_stats.iter().map(|s| s.into()).collect::<Vec<_>>();
        Ok(Statistics {
            num_rows: if s.num_rows == none_value {
                None
            } else {
                Some(s.num_rows as usize)
            },
            total_byte_size: if s.total_byte_size == none_value {
                None
            } else {
                Some(s.total_byte_size as usize)
            },
            // No column statistic (None) is encoded with empty array
            column_statistics: if column_statistics.is_empty() {
                None
            } else {
                Some(column_statistics)
            },
            is_exact: s.is_exact,
        })
    }
}
