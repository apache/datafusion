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

use crate::error::BallistaError;
use crate::serde::{from_proto_binary_op, proto_error, protobuf};
use crate::{convert_box_required, convert_required};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::parquet::{ParquetTable, ParquetTableDescriptor};
use datafusion::datasource::{PartitionedFile, TableDescriptor};
use datafusion::logical_plan::window_frames::{
    WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use datafusion::logical_plan::{
    abs, acos, asin, atan, ceil, cos, exp, floor, ln, log10, log2, round, signum, sin,
    sqrt, tan, trunc, Column, DFField, DFSchema, Expr, JoinConstraint, JoinType,
    LogicalPlan, LogicalPlanBuilder, Operator,
};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::avro::AvroReadOptions;
use datafusion::physical_plan::csv::CsvReadOptions;
use datafusion::physical_plan::window_functions::BuiltInWindowFunction;
use datafusion::scalar::ScalarValue;
use protobuf::logical_plan_node::LogicalPlanType;
use protobuf::{logical_expr_node::ExprType, scalar_type};
use std::{
    convert::{From, TryInto},
    sync::Arc,
    unimplemented,
};

impl TryInto<LogicalPlan> for &protobuf::LogicalPlanNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<LogicalPlan, Self::Error> {
        let plan = self.logical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "logical_plan::from_proto() Unsupported logical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            LogicalPlanType::Projection(projection) => {
                let input: LogicalPlan = convert_box_required!(projection.input)?;
                let x: Vec<Expr> = projection
                    .expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .project(x)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Selection(selection) => {
                let input: LogicalPlan = convert_box_required!(selection.input)?;
                LogicalPlanBuilder::from(input)
                    .filter(
                        selection
                            .expr
                            .as_ref()
                            .expect("expression required")
                            .try_into()?,
                    )?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Window(window) => {
                let input: LogicalPlan = convert_box_required!(window.input)?;
                let window_expr = window
                    .window_expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .window(window_expr)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Aggregate(aggregate) => {
                let input: LogicalPlan = convert_box_required!(aggregate.input)?;
                let group_expr = aggregate
                    .group_expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let aggr_expr = aggregate
                    .aggr_expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .aggregate(group_expr, aggr_expr)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::CsvScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;
                let options = CsvReadOptions::new()
                    .schema(&schema)
                    .delimiter(scan.delimiter.as_bytes()[0])
                    .file_extension(&scan.file_extension)
                    .has_header(scan.has_header);

                let mut projection = None;
                if let Some(columns) = &scan.projection {
                    let column_indices = columns
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                LogicalPlanBuilder::scan_csv_with_name(
                    &scan.path,
                    options,
                    projection,
                    &scan.table_name,
                )?
                .build()
                .map_err(|e| e.into())
            }
            LogicalPlanType::ParquetScan(scan) => {
                let descriptor: TableDescriptor = convert_required!(scan.table_desc)?;
                let projection = match scan.projection.as_ref() {
                    None => None,
                    Some(columns) => {
                        let schema = descriptor.schema.clone();
                        let r: Result<Vec<usize>, _> = columns
                            .columns
                            .iter()
                            .map(|col_name| {
                                schema.fields().iter().position(|field| field.name() == col_name).ok_or_else(|| {
                                    let column_names: Vec<&String> = schema.fields().iter().map(|f| f.name()).collect();
                                    proto_error(format!(
                                        "Parquet projection contains column name that is not present in schema. Column name: {}. Schema columns: {:?}",
                                        col_name, column_names
                                    ))
                                })
                            })
                            .collect();
                        Some(r?)
                    }
                };

                let parquet_table = ParquetTable::try_new_with_desc(
                    Arc::new(ParquetTableDescriptor { descriptor }),
                    24,
                    true,
                )?;
                LogicalPlanBuilder::scan(
                    &scan.table_name,
                    Arc::new(parquet_table),
                    projection,
                )? //TODO remove hard-coded max_partitions
                .build()
                .map_err(|e| e.into())
            }
            LogicalPlanType::AvroScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;
                let options = AvroReadOptions {
                    schema: Some(Arc::new(schema.clone())),
                    file_extension: &scan.file_extension,
                };

                let mut projection = None;
                if let Some(columns) = &scan.projection {
                    let column_indices = columns
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                LogicalPlanBuilder::scan_avro_with_name(
                    &scan.path,
                    options,
                    projection,
                    &scan.table_name,
                )?
                .build()
                .map_err(|e| e.into())
            }
            LogicalPlanType::Sort(sort) => {
                let input: LogicalPlan = convert_box_required!(sort.input)?;
                let sort_expr: Vec<Expr> = sort
                    .expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .sort(sort_expr)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Repartition(repartition) => {
                use datafusion::logical_plan::Partitioning;
                let input: LogicalPlan = convert_box_required!(repartition.input)?;
                use protobuf::repartition_node::PartitionMethod;
                let pb_partition_method = repartition.partition_method.clone().ok_or_else(|| {
                    BallistaError::General(String::from(
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
                            .map(|pb_expr| pb_expr.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                        partition_count as usize,
                    ),
                    PartitionMethod::RoundRobin(batch_size) => {
                        Partitioning::RoundRobinBatch(batch_size as usize)
                    }
                };

                LogicalPlanBuilder::from(input)
                    .repartition(partitioning_scheme)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::EmptyRelation(empty_relation) => {
                LogicalPlanBuilder::empty(empty_relation.produce_one_row)
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::CreateExternalTable(create_extern_table) => {
                let pb_schema = (create_extern_table.schema.clone()).ok_or_else(|| {
                    BallistaError::General(String::from(
                        "Protobuf deserialization error, CreateExternalTableNode was missing required field schema.",
                    ))
                })?;

                let pb_file_type: protobuf::FileType =
                    create_extern_table.file_type.try_into()?;

                Ok(LogicalPlan::CreateExternalTable {
                    schema: pb_schema.try_into()?,
                    name: create_extern_table.name.clone(),
                    location: create_extern_table.location.clone(),
                    file_type: pb_file_type.into(),
                    has_header: create_extern_table.has_header,
                })
            }
            LogicalPlanType::Analyze(analyze) => {
                let input: LogicalPlan = convert_box_required!(analyze.input)?;
                LogicalPlanBuilder::from(input)
                    .explain(analyze.verbose, true)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Explain(explain) => {
                let input: LogicalPlan = convert_box_required!(explain.input)?;
                LogicalPlanBuilder::from(input)
                    .explain(explain.verbose, false)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Limit(limit) => {
                let input: LogicalPlan = convert_box_required!(limit.input)?;
                LogicalPlanBuilder::from(input)
                    .limit(limit.limit as usize)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Join(join) => {
                let left_keys: Vec<Column> =
                    join.left_join_column.iter().map(|i| i.into()).collect();
                let right_keys: Vec<Column> =
                    join.right_join_column.iter().map(|i| i.into()).collect();
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

                let builder = LogicalPlanBuilder::from(convert_box_required!(join.left)?);
                let builder = match join_constraint.into() {
                    JoinConstraint::On => builder.join(
                        &convert_box_required!(join.right)?,
                        join_type.into(),
                        (left_keys, right_keys),
                    )?,
                    JoinConstraint::Using => builder.join_using(
                        &convert_box_required!(join.right)?,
                        join_type.into(),
                        left_keys,
                    )?,
                };

                builder.build().map_err(|e| e.into())
            }
            LogicalPlanType::CrossJoin(crossjoin) => {
                let left = convert_box_required!(crossjoin.left)?;
                let right = convert_box_required!(crossjoin.right)?;

                LogicalPlanBuilder::from(left)
                    .cross_join(&right)?
                    .build()
                    .map_err(|e| e.into())
            }
        }
    }
}

impl TryInto<TableDescriptor> for &protobuf::TableDescriptor {
    type Error = BallistaError;

    fn try_into(self) -> Result<TableDescriptor, Self::Error> {
        let partition_files = self
            .partition_files
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<PartitionedFile>, _>>()?;
        let schema = convert_required!(self.schema)?;
        Ok(TableDescriptor {
            path: self.path.to_owned(),
            partition_files,
            schema: Arc::new(schema),
        })
    }
}

impl TryInto<PartitionedFile> for &protobuf::PartitionedFile {
    type Error = BallistaError;

    fn try_into(self) -> Result<PartitionedFile, Self::Error> {
        let statistics = convert_required!(self.statistics)?;
        Ok(PartitionedFile {
            path: self.path.clone(),
            statistics,
        })
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
        let column_statistics = self.column_stats.iter().map(|s| s.into()).collect();
        Ok(Statistics {
            num_rows: Some(self.num_rows as usize),
            total_byte_size: Some(self.total_byte_size as usize),
            column_statistics: Some(column_statistics),
            is_exact: self.is_exact,
        })
    }
}

impl From<&protobuf::Column> for Column {
    fn from(c: &protobuf::Column) -> Column {
        let c = c.clone();
        Column {
            relation: c.relation.map(|r| r.relation),
            name: c.name,
        }
    }
}

impl TryInto<DFSchema> for &protobuf::DfSchema {
    type Error = BallistaError;

    fn try_into(self) -> Result<DFSchema, BallistaError> {
        let fields = self
            .columns
            .iter()
            .map(|c| c.try_into())
            .collect::<Result<Vec<DFField>, _>>()?;
        Ok(DFSchema::new(fields)?)
    }
}

impl TryInto<datafusion::logical_plan::DFSchemaRef> for protobuf::DfSchema {
    type Error = BallistaError;

    fn try_into(self) -> Result<datafusion::logical_plan::DFSchemaRef, Self::Error> {
        let dfschema: DFSchema = (&self).try_into()?;
        Ok(Arc::new(dfschema))
    }
}

impl TryInto<DFField> for &protobuf::DfField {
    type Error = BallistaError;

    fn try_into(self) -> Result<DFField, Self::Error> {
        let field: Field = convert_required!(self.field)?;

        Ok(match &self.qualifier {
            Some(q) => DFField::from_qualified(&q.relation, field),
            None => DFField::from(field),
        })
    }
}

impl TryInto<DataType> for &protobuf::scalar_type::Datatype {
    type Error = BallistaError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        use protobuf::scalar_type::Datatype;
        Ok(match self {
            Datatype::Scalar(scalar_type) => {
                let pb_scalar_enum = protobuf::PrimitiveScalarType::from_i32(*scalar_type).ok_or_else(|| {
                    proto_error(format!(
                        "Protobuf deserialization error, scalar_type::Datatype missing was provided invalid enum variant: {}",
                        *scalar_type
                    ))
                })?;
                pb_scalar_enum.into()
            }
            Datatype::List(protobuf::ScalarListType {
                deepest_type,
                field_names,
            }) => {
                if field_names.is_empty() {
                    return Err(proto_error(
                        "Protobuf deserialization error: found no field names in ScalarListType message which requires at least one",
                    ));
                }
                let pb_scalar_type = protobuf::PrimitiveScalarType::from_i32(
                    *deepest_type,
                )
                .ok_or_else(|| {
                    proto_error(format!(
                        "Protobuf deserialization error: invalid i32 for scalar enum: {}",
                        *deepest_type
                    ))
                })?;
                //Because length is checked above it is safe to unwrap .last()
                let mut scalar_type = DataType::List(Box::new(Field::new(
                    field_names.last().unwrap().as_str(),
                    pb_scalar_type.into(),
                    true,
                )));
                //Iterate over field names in reverse order except for the last item in the vector
                for name in field_names.iter().rev().skip(1) {
                    let new_datatype = DataType::List(Box::new(Field::new(
                        name.as_str(),
                        scalar_type,
                        true,
                    )));
                    scalar_type = new_datatype;
                }
                scalar_type
            }
        })
    }
}

//Does not typecheck lists
fn typechecked_scalar_value_conversion(
    tested_type: &protobuf::scalar_value::Value,
    required_type: protobuf::PrimitiveScalarType,
) -> Result<datafusion::scalar::ScalarValue, BallistaError> {
    use protobuf::scalar_value::Value;
    use protobuf::PrimitiveScalarType;
    Ok(match (tested_type, &required_type) {
        (Value::BoolValue(v), PrimitiveScalarType::Bool) => {
            ScalarValue::Boolean(Some(*v))
        }
        (Value::Int8Value(v), PrimitiveScalarType::Int8) => {
            ScalarValue::Int8(Some(*v as i8))
        }
        (Value::Int16Value(v), PrimitiveScalarType::Int16) => {
            ScalarValue::Int16(Some(*v as i16))
        }
        (Value::Int32Value(v), PrimitiveScalarType::Int32) => {
            ScalarValue::Int32(Some(*v))
        }
        (Value::Int64Value(v), PrimitiveScalarType::Int64) => {
            ScalarValue::Int64(Some(*v))
        }
        (Value::Uint8Value(v), PrimitiveScalarType::Uint8) => {
            ScalarValue::UInt8(Some(*v as u8))
        }
        (Value::Uint16Value(v), PrimitiveScalarType::Uint16) => {
            ScalarValue::UInt16(Some(*v as u16))
        }
        (Value::Uint32Value(v), PrimitiveScalarType::Uint32) => {
            ScalarValue::UInt32(Some(*v))
        }
        (Value::Uint64Value(v), PrimitiveScalarType::Uint64) => {
            ScalarValue::UInt64(Some(*v))
        }
        (Value::Float32Value(v), PrimitiveScalarType::Float32) => {
            ScalarValue::Float32(Some(*v))
        }
        (Value::Float64Value(v), PrimitiveScalarType::Float64) => {
            ScalarValue::Float64(Some(*v))
        }
        (Value::Date32Value(v), PrimitiveScalarType::Date32) => {
            ScalarValue::Date32(Some(*v))
        }
        (Value::TimeMicrosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimestampMicrosecond(Some(*v))
        }
        (Value::TimeNanosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimestampNanosecond(Some(*v))
        }
        (Value::Utf8Value(v), PrimitiveScalarType::Utf8) => {
            ScalarValue::Utf8(Some(v.to_owned()))
        }
        (Value::LargeUtf8Value(v), PrimitiveScalarType::LargeUtf8) => {
            ScalarValue::LargeUtf8(Some(v.to_owned()))
        }

        (Value::NullValue(i32_enum), required_scalar_type) => {
            if *i32_enum == *required_scalar_type as i32 {
                let pb_scalar_type = PrimitiveScalarType::from_i32(*i32_enum).ok_or_else(|| {
                    BallistaError::General(format!(
                        "Invalid i32_enum={} when converting with PrimitiveScalarType::from_i32()",
                        *i32_enum
                    ))
                })?;
                let scalar_value: ScalarValue = match pb_scalar_type {
                    PrimitiveScalarType::Bool => ScalarValue::Boolean(None),
                    PrimitiveScalarType::Uint8 => ScalarValue::UInt8(None),
                    PrimitiveScalarType::Int8 => ScalarValue::Int8(None),
                    PrimitiveScalarType::Uint16 => ScalarValue::UInt16(None),
                    PrimitiveScalarType::Int16 => ScalarValue::Int16(None),
                    PrimitiveScalarType::Uint32 => ScalarValue::UInt32(None),
                    PrimitiveScalarType::Int32 => ScalarValue::Int32(None),
                    PrimitiveScalarType::Uint64 => ScalarValue::UInt64(None),
                    PrimitiveScalarType::Int64 => ScalarValue::Int64(None),
                    PrimitiveScalarType::Float32 => ScalarValue::Float32(None),
                    PrimitiveScalarType::Float64 => ScalarValue::Float64(None),
                    PrimitiveScalarType::Utf8 => ScalarValue::Utf8(None),
                    PrimitiveScalarType::LargeUtf8 => ScalarValue::LargeUtf8(None),
                    PrimitiveScalarType::Date32 => ScalarValue::Date32(None),
                    PrimitiveScalarType::TimeMicrosecond => {
                        ScalarValue::TimestampMicrosecond(None)
                    }
                    PrimitiveScalarType::TimeNanosecond => {
                        ScalarValue::TimestampNanosecond(None)
                    }
                    PrimitiveScalarType::Null => {
                        return Err(proto_error(
                            "Untyped scalar null is not a valid scalar value",
                        ))
                    }
                };
                scalar_value
            } else {
                return Err(proto_error("Could not convert to the proper type"));
            }
        }
        _ => return Err(proto_error("Could not convert to the proper type")),
    })
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::scalar_value::Value {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use datafusion::scalar::ScalarValue;
        use protobuf::PrimitiveScalarType;
        let scalar = match self {
            protobuf::scalar_value::Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            protobuf::scalar_value::Value::Utf8Value(v) => {
                ScalarValue::Utf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::LargeUtf8Value(v) => {
                ScalarValue::LargeUtf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::Int8Value(v) => {
                ScalarValue::Int8(Some(*v as i8))
            }
            protobuf::scalar_value::Value::Int16Value(v) => {
                ScalarValue::Int16(Some(*v as i16))
            }
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => {
                ScalarValue::UInt8(Some(*v as u8))
            }
            protobuf::scalar_value::Value::Uint16Value(v) => {
                ScalarValue::UInt16(Some(*v as u16))
            }
            protobuf::scalar_value::Value::Uint32Value(v) => {
                ScalarValue::UInt32(Some(*v))
            }
            protobuf::scalar_value::Value::Uint64Value(v) => {
                ScalarValue::UInt64(Some(*v))
            }
            protobuf::scalar_value::Value::Float32Value(v) => {
                ScalarValue::Float32(Some(*v))
            }
            protobuf::scalar_value::Value::Float64Value(v) => {
                ScalarValue::Float64(Some(*v))
            }
            protobuf::scalar_value::Value::Date32Value(v) => {
                ScalarValue::Date32(Some(*v))
            }
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => {
                ScalarValue::TimestampMicrosecond(Some(*v))
            }
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => {
                ScalarValue::TimestampNanosecond(Some(*v))
            }
            protobuf::scalar_value::Value::ListValue(v) => v.try_into()?,
            protobuf::scalar_value::Value::NullListValue(v) => {
                ScalarValue::List(None, Box::new(v.try_into()?))
            }
            protobuf::scalar_value::Value::NullValue(null_enum) => {
                PrimitiveScalarType::from_i32(*null_enum)
                    .ok_or_else(|| proto_error("Invalid scalar type"))?
                    .try_into()?
            }
        };
        Ok(scalar)
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::ScalarListValue {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use protobuf::scalar_type::Datatype;
        use protobuf::PrimitiveScalarType;
        let protobuf::ScalarListValue { datatype, values } = self;
        let pb_scalar_type = datatype
            .as_ref()
            .ok_or_else(|| proto_error("Protobuf deserialization error: ScalarListValue messsage missing required field 'datatype'"))?;
        let scalar_type = pb_scalar_type
            .datatype
            .as_ref()
            .ok_or_else(|| proto_error("Protobuf deserialization error: ScalarListValue.Datatype messsage missing required field 'datatype'"))?;
        let scalar_values = match scalar_type {
            Datatype::Scalar(scalar_type_i32) => {
                let leaf_scalar_type =
                    protobuf::PrimitiveScalarType::from_i32(*scalar_type_i32)
                        .ok_or_else(|| {
                            proto_error("Error converting i32 to basic scalar type")
                        })?;
                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = values
                    .iter()
                    .map(|protobuf::ScalarValue { value: opt_value }| {
                        let value = opt_value.as_ref().ok_or_else(|| {
                            proto_error(
                                "Protobuf deserialization error: missing required field 'value'",
                            )
                        })?;
                        typechecked_scalar_value_conversion(value, leaf_scalar_type)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                datafusion::scalar::ScalarValue::List(
                    Some(Box::new(typechecked_values)),
                    Box::new(leaf_scalar_type.into()),
                )
            }
            Datatype::List(list_type) => {
                let protobuf::ScalarListType {
                    deepest_type,
                    field_names,
                } = &list_type;
                let leaf_type =
                    PrimitiveScalarType::from_i32(*deepest_type).ok_or_else(|| {
                        proto_error("Error converting i32 to basic scalar type")
                    })?;
                let depth = field_names.len();

                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = if depth
                    == 0
                {
                    return Err(proto_error(
                        "Protobuf deserialization error, ScalarListType had no field names, requires at least one",
                    ));
                } else if depth == 1 {
                    values
                        .iter()
                        .map(|protobuf::ScalarValue { value: opt_value }| {
                            let value = opt_value
                                .as_ref()
                                .ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                            typechecked_scalar_value_conversion(value, leaf_type)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    values
                        .iter()
                        .map(|protobuf::ScalarValue { value: opt_value }| {
                            let value = opt_value
                                .as_ref()
                                .ok_or_else(|| proto_error("Protobuf deserialization error: missing required field 'value'"))?;
                            value.try_into()
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
                datafusion::scalar::ScalarValue::List(
                    match typechecked_values.len() {
                        0 => None,
                        _ => Some(Box::new(typechecked_values)),
                    },
                    Box::new(list_type.try_into()?),
                )
            }
        };
        Ok(scalar_values)
    }
}

impl TryInto<DataType> for &protobuf::ScalarListType {
    type Error = BallistaError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        use protobuf::PrimitiveScalarType;
        let protobuf::ScalarListType {
            deepest_type,
            field_names,
        } = self;

        let depth = field_names.len();
        if depth == 0 {
            return Err(proto_error(
                "Protobuf deserialization error: Found a ScalarListType message with no field names, at least one is required",
            ));
        }

        let mut curr_type = DataType::List(Box::new(Field::new(
            //Since checked vector is not empty above this is safe to unwrap
            field_names.last().unwrap(),
            PrimitiveScalarType::from_i32(*deepest_type)
                .ok_or_else(|| {
                    proto_error("Could not convert to datafusion scalar type")
                })?
                .into(),
            true,
        )));
        //Iterates over field names in reverse order except for the last item in the vector
        for name in field_names.iter().rev().skip(1) {
            let temp_curr_type =
                DataType::List(Box::new(Field::new(name, curr_type, true)));
            curr_type = temp_curr_type;
        }
        Ok(curr_type)
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for protobuf::PrimitiveScalarType {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        use datafusion::scalar::ScalarValue;
        Ok(match self {
            protobuf::PrimitiveScalarType::Null => {
                return Err(proto_error("Untyped null is an invalid scalar value"))
            }
            protobuf::PrimitiveScalarType::Bool => ScalarValue::Boolean(None),
            protobuf::PrimitiveScalarType::Uint8 => ScalarValue::UInt8(None),
            protobuf::PrimitiveScalarType::Int8 => ScalarValue::Int8(None),
            protobuf::PrimitiveScalarType::Uint16 => ScalarValue::UInt16(None),
            protobuf::PrimitiveScalarType::Int16 => ScalarValue::Int16(None),
            protobuf::PrimitiveScalarType::Uint32 => ScalarValue::UInt32(None),
            protobuf::PrimitiveScalarType::Int32 => ScalarValue::Int32(None),
            protobuf::PrimitiveScalarType::Uint64 => ScalarValue::UInt64(None),
            protobuf::PrimitiveScalarType::Int64 => ScalarValue::Int64(None),
            protobuf::PrimitiveScalarType::Float32 => ScalarValue::Float32(None),
            protobuf::PrimitiveScalarType::Float64 => ScalarValue::Float64(None),
            protobuf::PrimitiveScalarType::Utf8 => ScalarValue::Utf8(None),
            protobuf::PrimitiveScalarType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            protobuf::PrimitiveScalarType::Date32 => ScalarValue::Date32(None),
            protobuf::PrimitiveScalarType::TimeMicrosecond => {
                ScalarValue::TimestampMicrosecond(None)
            }
            protobuf::PrimitiveScalarType::TimeNanosecond => {
                ScalarValue::TimestampNanosecond(None)
            }
        })
    }
}

impl TryInto<datafusion::scalar::ScalarValue> for &protobuf::ScalarValue {
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::scalar::ScalarValue, Self::Error> {
        let value = self.value.as_ref().ok_or_else(|| {
            proto_error("Protobuf deserialization error: missing required field 'value'")
        })?;
        Ok(match value {
            protobuf::scalar_value::Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            protobuf::scalar_value::Value::Utf8Value(v) => {
                ScalarValue::Utf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::LargeUtf8Value(v) => {
                ScalarValue::LargeUtf8(Some(v.to_owned()))
            }
            protobuf::scalar_value::Value::Int8Value(v) => {
                ScalarValue::Int8(Some(*v as i8))
            }
            protobuf::scalar_value::Value::Int16Value(v) => {
                ScalarValue::Int16(Some(*v as i16))
            }
            protobuf::scalar_value::Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            protobuf::scalar_value::Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            protobuf::scalar_value::Value::Uint8Value(v) => {
                ScalarValue::UInt8(Some(*v as u8))
            }
            protobuf::scalar_value::Value::Uint16Value(v) => {
                ScalarValue::UInt16(Some(*v as u16))
            }
            protobuf::scalar_value::Value::Uint32Value(v) => {
                ScalarValue::UInt32(Some(*v))
            }
            protobuf::scalar_value::Value::Uint64Value(v) => {
                ScalarValue::UInt64(Some(*v))
            }
            protobuf::scalar_value::Value::Float32Value(v) => {
                ScalarValue::Float32(Some(*v))
            }
            protobuf::scalar_value::Value::Float64Value(v) => {
                ScalarValue::Float64(Some(*v))
            }
            protobuf::scalar_value::Value::Date32Value(v) => {
                ScalarValue::Date32(Some(*v))
            }
            protobuf::scalar_value::Value::TimeMicrosecondValue(v) => {
                ScalarValue::TimestampMicrosecond(Some(*v))
            }
            protobuf::scalar_value::Value::TimeNanosecondValue(v) => {
                ScalarValue::TimestampNanosecond(Some(*v))
            }
            protobuf::scalar_value::Value::ListValue(scalar_list) => {
                let protobuf::ScalarListValue {
                    values,
                    datatype: opt_scalar_type,
                } = &scalar_list;
                let pb_scalar_type = opt_scalar_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization err: ScalaListValue missing required field 'datatype'"))?;
                let typechecked_values: Vec<ScalarValue> = values
                    .iter()
                    .map(|val| val.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let scalar_type: DataType = pb_scalar_type.try_into()?;
                let scalar_type = Box::new(scalar_type);
                ScalarValue::List(Some(Box::new(typechecked_values)), scalar_type)
            }
            protobuf::scalar_value::Value::NullListValue(v) => {
                let pb_datatype = v
                    .datatype
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: NullListValue message missing required field 'datatyp'"))?;
                let pb_datatype = Box::new(pb_datatype.try_into()?);
                ScalarValue::List(None, pb_datatype)
            }
            protobuf::scalar_value::Value::NullValue(v) => {
                let null_type_enum = protobuf::PrimitiveScalarType::from_i32(*v)
                    .ok_or_else(|| proto_error("Protobuf deserialization error found invalid enum variant for DatafusionScalar"))?;
                null_type_enum.try_into()?
            }
        })
    }
}

impl TryInto<Expr> for &protobuf::LogicalExprNode {
    type Error = BallistaError;

    fn try_into(self) -> Result<Expr, Self::Error> {
        use datafusion::physical_plan::window_functions;
        use protobuf::logical_expr_node::ExprType;
        use protobuf::window_expr_node;
        use protobuf::WindowExprNode;

        let expr_type = self
            .expr_type
            .as_ref()
            .ok_or_else(|| proto_error("Unexpected empty logical expression"))?;
        match expr_type {
            ExprType::BinaryExpr(binary_expr) => Ok(Expr::BinaryExpr {
                left: Box::new(parse_required_expr(&binary_expr.l)?),
                op: from_proto_binary_op(&binary_expr.op)?,
                right: Box::new(parse_required_expr(&binary_expr.r)?),
            }),
            ExprType::Column(column) => Ok(Expr::Column(column.into())),
            ExprType::Literal(literal) => {
                use datafusion::scalar::ScalarValue;
                let scalar_value: datafusion::scalar::ScalarValue = literal.try_into()?;
                Ok(Expr::Literal(scalar_value))
            }
            ExprType::WindowExpr(expr) => {
                let window_function = expr
                    .window_function
                    .as_ref()
                    .ok_or_else(|| proto_error("Received empty window function"))?;
                let partition_by = expr
                    .partition_by
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let order_by = expr
                    .order_by
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let window_frame = expr
                    .window_frame
                    .as_ref()
                    .map::<Result<WindowFrame, _>, _>(|e| match e {
                        window_expr_node::WindowFrame::Frame(frame) => {
                            let window_frame: WindowFrame = frame.clone().try_into()?;
                            if WindowFrameUnits::Range == window_frame.units
                                && order_by.len() != 1
                            {
                                Err(proto_error("With window frame of type RANGE, the order by expression must be of length 1"))
                            } else {
                                Ok(window_frame)
                            }
                        }
                    })
                    .transpose()?;

                match window_function {
                    window_expr_node::WindowFunction::AggrFunction(i) => {
                        let aggr_function = protobuf::AggregateFunction::from_i32(*i)
                            .ok_or_else(|| {
                                proto_error(format!(
                                    "Received an unknown aggregate window function: {}",
                                    i
                                ))
                            })?;

                        Ok(Expr::WindowFunction {
                            fun: window_functions::WindowFunction::AggregateFunction(
                                AggregateFunction::from(aggr_function),
                            ),
                            args: vec![parse_required_expr(&expr.expr)?],
                            partition_by,
                            order_by,
                            window_frame,
                        })
                    }
                    window_expr_node::WindowFunction::BuiltInFunction(i) => {
                        let built_in_function =
                            protobuf::BuiltInWindowFunction::from_i32(*i).ok_or_else(
                                || {
                                    proto_error(format!(
                                        "Received an unknown built-in window function: {}",
                                        i
                                    ))
                                },
                            )?;

                        Ok(Expr::WindowFunction {
                            fun: window_functions::WindowFunction::BuiltInWindowFunction(
                                BuiltInWindowFunction::from(built_in_function),
                            ),
                            args: vec![parse_required_expr(&expr.expr)?],
                            partition_by,
                            order_by,
                            window_frame,
                        })
                    }
                }
            }
            ExprType::AggregateExpr(expr) => {
                let aggr_function =
                    protobuf::AggregateFunction::from_i32(expr.aggr_function)
                        .ok_or_else(|| {
                            proto_error(format!(
                                "Received an unknown aggregate function: {}",
                                expr.aggr_function
                            ))
                        })?;
                let fun = AggregateFunction::from(aggr_function);

                Ok(Expr::AggregateFunction {
                    fun,
                    args: vec![parse_required_expr(&expr.expr)?],
                    distinct: false, //TODO
                })
            }
            ExprType::Alias(alias) => Ok(Expr::Alias(
                Box::new(parse_required_expr(&alias.expr)?),
                alias.alias.clone(),
            )),
            ExprType::IsNullExpr(is_null) => {
                Ok(Expr::IsNull(Box::new(parse_required_expr(&is_null.expr)?)))
            }
            ExprType::IsNotNullExpr(is_not_null) => Ok(Expr::IsNotNull(Box::new(
                parse_required_expr(&is_not_null.expr)?,
            ))),
            ExprType::NotExpr(not) => {
                Ok(Expr::Not(Box::new(parse_required_expr(&not.expr)?)))
            }
            ExprType::Between(between) => Ok(Expr::Between {
                expr: Box::new(parse_required_expr(&between.expr)?),
                negated: between.negated,
                low: Box::new(parse_required_expr(&between.low)?),
                high: Box::new(parse_required_expr(&between.high)?),
            }),
            ExprType::Case(case) => {
                let when_then_expr = case
                    .when_then_expr
                    .iter()
                    .map(|e| {
                        Ok((
                            Box::new(match &e.when_expr {
                                Some(e) => e.try_into(),
                                None => Err(proto_error("Missing required expression")),
                            }?),
                            Box::new(match &e.then_expr {
                                Some(e) => e.try_into(),
                                None => Err(proto_error("Missing required expression")),
                            }?),
                        ))
                    })
                    .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, BallistaError>>()?;
                Ok(Expr::Case {
                    expr: parse_optional_expr(&case.expr)?.map(Box::new),
                    when_then_expr,
                    else_expr: parse_optional_expr(&case.else_expr)?.map(Box::new),
                })
            }
            ExprType::Cast(cast) => {
                let expr = Box::new(parse_required_expr(&cast.expr)?);
                let arrow_type: &protobuf::ArrowType = cast
                    .arrow_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: CastNode message missing required field 'arrow_type'"))?;
                let data_type = arrow_type.try_into()?;
                Ok(Expr::Cast { expr, data_type })
            }
            ExprType::TryCast(cast) => {
                let expr = Box::new(parse_required_expr(&cast.expr)?);
                let arrow_type: &protobuf::ArrowType = cast
                    .arrow_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: CastNode message missing required field 'arrow_type'"))?;
                let data_type = arrow_type.try_into()?;
                Ok(Expr::TryCast { expr, data_type })
            }
            ExprType::Sort(sort) => Ok(Expr::Sort {
                expr: Box::new(parse_required_expr(&sort.expr)?),
                asc: sort.asc,
                nulls_first: sort.nulls_first,
            }),
            ExprType::Negative(negative) => Ok(Expr::Negative(Box::new(
                parse_required_expr(&negative.expr)?,
            ))),
            ExprType::InList(in_list) => Ok(Expr::InList {
                expr: Box::new(parse_required_expr(&in_list.expr)?),
                list: in_list
                    .list
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                negated: in_list.negated,
            }),
            ExprType::Wildcard(_) => Ok(Expr::Wildcard),
            ExprType::ScalarFunction(expr) => {
                let scalar_function = protobuf::ScalarFunction::from_i32(expr.fun)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received an unknown scalar function: {}",
                            expr.fun
                        ))
                    })?;
                let args = &expr.args;

                match scalar_function {
                    protobuf::ScalarFunction::Sqrt => Ok(sqrt((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Sin => Ok(sin((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Cos => Ok(cos((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Tan => Ok(tan((&args[0]).try_into()?)),
                    // protobuf::ScalarFunction::Asin => Ok(asin(&args[0]).try_into()?)),
                    // protobuf::ScalarFunction::Acos => Ok(acos(&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Atan => Ok(atan((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Exp => Ok(exp((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Log2 => Ok(log2((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Ln => Ok(ln((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Log10 => Ok(log10((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Floor => Ok(floor((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Ceil => Ok(ceil((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Round => Ok(round((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Trunc => Ok(trunc((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Abs => Ok(abs((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Signum => {
                        Ok(signum((&args[0]).try_into()?))
                    }
                    protobuf::ScalarFunction::Octetlength => {
                        Ok(length((&args[0]).try_into()?))
                    }
                    // // protobuf::ScalarFunction::Concat => Ok(concat((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Lower => Ok(lower((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Upper => Ok(upper((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Trim => Ok(trim((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Ltrim => Ok(ltrim((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Rtrim => Ok(rtrim((&args[0]).try_into()?)),
                    // protobuf::ScalarFunction::Totimestamp => Ok(to_timestamp((&args[0]).try_into()?)),
                    // protobuf::ScalarFunction::Array => Ok(array((&args[0]).try_into()?)),
                    // // protobuf::ScalarFunction::Nullif => Ok(nulli((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Datepart => {
                        Ok(date_part((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    protobuf::ScalarFunction::Datetrunc => {
                        Ok(date_trunc((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    // protobuf::ScalarFunction::Md5 => Ok(md5((&args[0]).try_into()?)),
                    protobuf::ScalarFunction::Sha224 => {
                        Ok(sha224((&args[0]).try_into()?))
                    }
                    protobuf::ScalarFunction::Sha256 => {
                        Ok(sha256((&args[0]).try_into()?))
                    }
                    protobuf::ScalarFunction::Sha384 => {
                        Ok(sha384((&args[0]).try_into()?))
                    }
                    protobuf::ScalarFunction::Sha512 => {
                        Ok(sha512((&args[0]).try_into()?))
                    }
                    _ => Err(proto_error(
                        "Protobuf deserialization error: Unsupported scalar function",
                    )),
                }
            }
        }
    }
}

impl TryInto<DataType> for &protobuf::ScalarType {
    type Error = BallistaError;
    fn try_into(self) -> Result<DataType, Self::Error> {
        let pb_scalartype = self.datatype.as_ref().ok_or_else(|| {
            proto_error("ScalarType message missing required field 'datatype'")
        })?;
        pb_scalartype.try_into()
    }
}

impl TryInto<Schema> for &protobuf::Schema {
    type Error = BallistaError;

    fn try_into(self) -> Result<Schema, BallistaError> {
        let fields = self
            .columns
            .iter()
            .map(|c| {
                let pb_arrow_type_res = c
                    .arrow_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Field message was missing required field 'arrow_type'"));
                let pb_arrow_type: &protobuf::ArrowType = match pb_arrow_type_res {
                    Ok(res) => res,
                    Err(e) => return Err(e),
                };
                Ok(Field::new(&c.name, pb_arrow_type.try_into()?, c.nullable))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schema::new(fields))
    }
}

impl TryInto<Field> for &protobuf::Field {
    type Error = BallistaError;
    fn try_into(self) -> Result<Field, Self::Error> {
        let pb_datatype = self.arrow_type.as_ref().ok_or_else(|| {
            proto_error(
                "Protobuf deserialization error: Field message missing required field 'arrow_type'",
            )
        })?;

        Ok(Field::new(
            self.name.as_str(),
            pb_datatype.as_ref().try_into()?,
            self.nullable,
        ))
    }
}

use crate::serde::protobuf::ColumnStats;
use datafusion::physical_plan::{aggregates, windows, ColumnStatistics, Statistics};
use datafusion::prelude::{
    array, date_part, date_trunc, length, lower, ltrim, md5, rtrim, sha224, sha256,
    sha384, sha512, trim, upper,
};
use std::convert::TryFrom;

impl TryFrom<i32> for protobuf::FileType {
    type Error = BallistaError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        use protobuf::FileType;
        match value {
            _x if _x == FileType::NdJson as i32 => Ok(FileType::NdJson),
            _x if _x == FileType::Parquet as i32 => Ok(FileType::Parquet),
            _x if _x == FileType::Csv as i32 => Ok(FileType::Csv),
            _x if _x == FileType::Avro as i32 => Ok(FileType::Avro),
            invalid => Err(BallistaError::General(format!(
                "Attempted to convert invalid i32 to protobuf::Filetype: {}",
                invalid
            ))),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<datafusion::sql::parser::FileType> for protobuf::FileType {
    fn into(self) -> datafusion::sql::parser::FileType {
        use datafusion::sql::parser::FileType;
        match self {
            protobuf::FileType::NdJson => FileType::NdJson,
            protobuf::FileType::Parquet => FileType::Parquet,
            protobuf::FileType::Csv => FileType::CSV,
            protobuf::FileType::Avro => FileType::Avro,
        }
    }
}

fn parse_required_expr(
    p: &Option<Box<protobuf::LogicalExprNode>>,
) -> Result<Expr, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().try_into(),
        None => Err(proto_error("Missing required expression")),
    }
}

fn parse_optional_expr(
    p: &Option<Box<protobuf::LogicalExprNode>>,
) -> Result<Option<Expr>, BallistaError> {
    match p {
        Some(expr) => expr.as_ref().try_into().map(Some),
        None => Ok(None),
    }
}

impl From<protobuf::WindowFrameUnits> for WindowFrameUnits {
    fn from(units: protobuf::WindowFrameUnits) -> Self {
        match units {
            protobuf::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
            protobuf::WindowFrameUnits::Range => WindowFrameUnits::Range,
            protobuf::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
        }
    }
}

impl TryFrom<protobuf::WindowFrameBound> for WindowFrameBound {
    type Error = BallistaError;

    fn try_from(bound: protobuf::WindowFrameBound) -> Result<Self, Self::Error> {
        let bound_type = protobuf::WindowFrameBoundType::from_i32(bound.window_frame_bound_type).ok_or_else(|| {
            proto_error(format!(
                "Received a WindowFrameBound message with unknown WindowFrameBoundType {}",
                bound.window_frame_bound_type
            ))
        })?;
        match bound_type {
            protobuf::WindowFrameBoundType::CurrentRow => {
                Ok(WindowFrameBound::CurrentRow)
            }
            protobuf::WindowFrameBoundType::Preceding => {
                // FIXME implement bound value parsing
                // https://github.com/apache/arrow-datafusion/issues/361
                Ok(WindowFrameBound::Preceding(Some(1)))
            }
            protobuf::WindowFrameBoundType::Following => {
                // FIXME implement bound value parsing
                // https://github.com/apache/arrow-datafusion/issues/361
                Ok(WindowFrameBound::Following(Some(1)))
            }
        }
    }
}

impl TryFrom<protobuf::WindowFrame> for WindowFrame {
    type Error = BallistaError;

    fn try_from(window: protobuf::WindowFrame) -> Result<Self, Self::Error> {
        let units = protobuf::WindowFrameUnits::from_i32(window.window_frame_units)
            .ok_or_else(|| {
                proto_error(format!(
                    "Received a WindowFrame message with unknown WindowFrameUnits {}",
                    window.window_frame_units
                ))
            })?
            .into();
        let start_bound = window
            .start_bound
            .ok_or_else(|| {
                proto_error(
                    "Received a WindowFrame message with no start_bound".to_owned(),
                )
            })?
            .try_into()?;
        let end_bound = window
            .end_bound
            .map(|end_bound| match end_bound {
                protobuf::window_frame::EndBound::Bound(end_bound) => {
                    end_bound.try_into()
                }
            })
            .transpose()?
            .unwrap_or(WindowFrameBound::CurrentRow);
        Ok(WindowFrame {
            units,
            start_bound,
            end_bound,
        })
    }
}
