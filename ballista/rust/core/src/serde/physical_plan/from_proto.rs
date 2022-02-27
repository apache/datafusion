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

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::error::BallistaError;

use crate::serde::{from_proto_binary_op, proto_error, protobuf};
use crate::{convert_box_required, convert_required};
use chrono::{TimeZone, Utc};

use datafusion::catalog::catalog::{CatalogList, MemoryCatalogList};
use datafusion::datasource::object_store::local::LocalFileSystem;
use datafusion::datasource::object_store::{FileMeta, ObjectStoreRegistry, SizedFile};
use datafusion::datasource::PartitionedFile;
use datafusion::execution::context::{
    ExecutionConfig, ExecutionContextState, ExecutionProps,
};
use datafusion::execution::runtime_env::RuntimeEnv;

use datafusion::physical_plan::file_format::FileScanConfig;

use datafusion::physical_plan::window_functions::WindowFunction;

use datafusion::physical_plan::{
    expressions::{
        BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr,
        Literal, NegativeExpr, NotExpr, TryCastExpr, DEFAULT_DATAFUSION_CAST_OPTIONS,
    },
    functions::{self, BuiltinScalarFunction, ScalarFunctionExpr},
    Partitioning,
};
use datafusion::physical_plan::{ColumnStatistics, PhysicalExpr, Statistics};

use protobuf::physical_expr_node::ExprType;

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
            ScalarFunction::Digest => BuiltinScalarFunction::Digest,
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
                    aggregate_functions: Default::default(),
                    config: ExecutionConfig::new(),
                    execution_props: ExecutionProps::new(),
                    object_store_registry: Arc::new(ObjectStoreRegistry::new()),
                    runtime_env: Arc::new(RuntimeEnv::default()),
                };

                let fun_expr = functions::create_physical_fun(
                    &(&scalar_function).into(),
                    &ctx_state.execution_props,
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

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = BallistaError;

    fn try_from(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        Ok(PartitionedFile {
            file_meta: FileMeta {
                sized_file: SizedFile {
                    path: val.path.clone(),
                    size: val.size,
                },
                last_modified: if val.last_modified_ns == 0 {
                    None
                } else {
                    Some(Utc.timestamp_nanos(val.last_modified_ns as i64))
                },
            },
            partition_values: val
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<&protobuf::FileGroup> for Vec<PartitionedFile> {
    type Error = BallistaError;

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

impl TryInto<Statistics> for &protobuf::Statistics {
    type Error = BallistaError;

    fn try_into(self) -> Result<Statistics, Self::Error> {
        let column_statistics = self
            .column_stats
            .iter()
            .map(|s| s.into())
            .collect::<Vec<_>>();
        Ok(Statistics {
            num_rows: Some(self.num_rows as usize),
            total_byte_size: Some(self.total_byte_size as usize),
            // No column statistic (None) is encoded with empty array
            column_statistics: if column_statistics.is_empty() {
                None
            } else {
                Some(column_statistics)
            },
            is_exact: self.is_exact,
        })
    }
}

impl TryInto<FileScanConfig> for &protobuf::FileScanExecConf {
    type Error = BallistaError;

    fn try_into(self) -> Result<FileScanConfig, Self::Error> {
        let schema = Arc::new(convert_required!(self.schema)?);
        let projection = self
            .projection
            .iter()
            .map(|i| *i as usize)
            .collect::<Vec<_>>();
        let projection = if projection.is_empty() {
            None
        } else {
            Some(projection)
        };
        let statistics = convert_required!(self.statistics)?;

        Ok(FileScanConfig {
            object_store: Arc::new(LocalFileSystem {}),
            file_schema: schema,
            file_groups: self
                .file_groups
                .iter()
                .map(|f| f.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            statistics,
            projection,
            limit: self.limit.as_ref().map(|sl| sl.limit as usize),
            table_partition_cols: vec![],
        })
    }
}
