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

//! This crate contains code generated from the Ballista Protocol Buffer Definition as well
//! as convenience code for interacting with the generated code.

use prost::bytes::BufMut;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{convert::TryInto, io::Cursor};

use datafusion::arrow::datatypes::{IntervalUnit, UnionMode};
use datafusion::logical_plan::{JoinConstraint, JoinType, LogicalPlan, Operator};
use datafusion::physical_plan::aggregates::AggregateFunction;
use datafusion::physical_plan::window_functions::BuiltInWindowFunction;

use crate::{error::BallistaError, serde::scheduler::Action as BallistaAction};

use datafusion::logical_plan::plan::Extension;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::ExecutionContext;
use prost::Message;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/ballista.protobuf.rs"));
}

pub mod logical_plan;
pub mod physical_plan;
pub mod scheduler;

pub fn decode_protobuf(bytes: &[u8]) -> Result<BallistaAction, BallistaError> {
    let mut buf = Cursor::new(bytes);

    protobuf::Action::decode(&mut buf)
        .map_err(|e| BallistaError::Internal(format!("{:?}", e)))
        .and_then(|node| node.try_into())
}

pub(crate) fn proto_error<S: Into<String>>(message: S) -> BallistaError {
    BallistaError::General(message.into())
}

pub trait AsLogicalPlan: Debug + Send + Sync + Clone {
    fn try_decode(buf: &[u8]) -> Result<Self, BallistaError>
    where
        Self: Sized;

    fn try_encode<B>(&self, buf: &mut B) -> Result<(), BallistaError>
    where
        B: BufMut,
        Self: Sized;

    fn try_into_logical_plan(
        &self,
        ctx: &ExecutionContext,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<LogicalPlan, BallistaError>;

    fn try_from_logical_plan(
        plan: &LogicalPlan,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<Self, BallistaError>
    where
        Self: Sized;
}

pub trait LogicalExtensionCodec: Debug + Send + Sync {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
    ) -> Result<Extension, BallistaError>;

    fn try_encode(
        &self,
        node: &Extension,
        buf: &mut Vec<u8>,
    ) -> Result<(), BallistaError>;
}

#[derive(Debug, Clone)]
pub struct DefaultLogicalExtensionCodec {}

impl LogicalExtensionCodec for DefaultLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
    ) -> Result<Extension, BallistaError> {
        Err(BallistaError::NotImplemented(
            "LogicalExtensionCodec is not provided".to_string(),
        ))
    }

    fn try_encode(
        &self,
        _node: &Extension,
        _buf: &mut Vec<u8>,
    ) -> Result<(), BallistaError> {
        Err(BallistaError::NotImplemented(
            "LogicalExtensionCodec is not provided".to_string(),
        ))
    }
}

pub trait AsExecutionPlan: Debug + Send + Sync + Clone {
    fn try_decode(buf: &[u8]) -> Result<Self, BallistaError>
    where
        Self: Sized;

    fn try_encode<B>(&self, buf: &mut B) -> Result<(), BallistaError>
    where
        B: BufMut,
        Self: Sized;

    fn try_into_physical_plan(
        &self,
        ctx: &ExecutionContext,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError>;

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self, BallistaError>
    where
        Self: Sized;
}

pub trait PhysicalExtensionCodec: Debug + Send + Sync {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError>;

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), BallistaError>;
}

#[derive(Debug, Clone)]
pub struct DefaultPhysicalExtensionCodec {}

impl PhysicalExtensionCodec for DefaultPhysicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
        Err(BallistaError::NotImplemented(
            "PhysicalExtensionCodec is not provided".to_string(),
        ))
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<(), BallistaError> {
        Err(BallistaError::NotImplemented(
            "PhysicalExtensionCodec is not provided".to_string(),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct BallistaCodec<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> {
    logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
    physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    logical_plan_repr: PhantomData<T>,
    physical_plan_repr: PhantomData<U>,
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> Default
    for BallistaCodec<T, U>
{
    fn default() -> Self {
        Self {
            logical_extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            physical_extension_codec: Arc::new(DefaultPhysicalExtensionCodec {}),
            logical_plan_repr: PhantomData,
            physical_plan_repr: PhantomData,
        }
    }
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> BallistaCodec<T, U> {
    pub fn new(
        logical_extension_codec: Arc<dyn LogicalExtensionCodec>,
        physical_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        Self {
            logical_extension_codec,
            physical_extension_codec,
            logical_plan_repr: PhantomData,
            physical_plan_repr: PhantomData,
        }
    }

    pub fn logical_extension_codec(&self) -> &dyn LogicalExtensionCodec {
        self.logical_extension_codec.as_ref()
    }

    pub fn physical_extension_codec(&self) -> &dyn PhysicalExtensionCodec {
        self.physical_extension_codec.as_ref()
    }
}

#[macro_export]
macro_rules! convert_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[macro_export]
macro_rules! into_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            Ok(field.into())
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[macro_export]
macro_rules! convert_box_required {
    ($PB:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into()
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

pub(crate) fn from_proto_binary_op(op: &str) -> Result<Operator, BallistaError> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Modulo" => Ok(Operator::Modulo),
        "Like" => Ok(Operator::Like),
        "NotLike" => Ok(Operator::NotLike),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{:?}'",
            other
        ))),
    }
}

impl From<protobuf::AggregateFunction> for AggregateFunction {
    fn from(agg_fun: protobuf::AggregateFunction) -> AggregateFunction {
        match agg_fun {
            protobuf::AggregateFunction::Min => AggregateFunction::Min,
            protobuf::AggregateFunction::Max => AggregateFunction::Max,
            protobuf::AggregateFunction::Sum => AggregateFunction::Sum,
            protobuf::AggregateFunction::Avg => AggregateFunction::Avg,
            protobuf::AggregateFunction::Count => AggregateFunction::Count,
            protobuf::AggregateFunction::ApproxDistinct => {
                AggregateFunction::ApproxDistinct
            }
            protobuf::AggregateFunction::ArrayAgg => AggregateFunction::ArrayAgg,
            protobuf::AggregateFunction::Variance => AggregateFunction::Variance,
            protobuf::AggregateFunction::VariancePop => AggregateFunction::VariancePop,
            protobuf::AggregateFunction::Covariance => AggregateFunction::Covariance,
            protobuf::AggregateFunction::CovariancePop => {
                AggregateFunction::CovariancePop
            }
            protobuf::AggregateFunction::Stddev => AggregateFunction::Stddev,
            protobuf::AggregateFunction::StddevPop => AggregateFunction::StddevPop,
            protobuf::AggregateFunction::Correlation => AggregateFunction::Correlation,
            protobuf::AggregateFunction::ApproxPercentileCont => {
                AggregateFunction::ApproxPercentileCont
            }
            protobuf::AggregateFunction::ApproxMedian => AggregateFunction::ApproxMedian,
        }
    }
}

impl From<protobuf::BuiltInWindowFunction> for BuiltInWindowFunction {
    fn from(built_in_function: protobuf::BuiltInWindowFunction) -> Self {
        match built_in_function {
            protobuf::BuiltInWindowFunction::RowNumber => {
                BuiltInWindowFunction::RowNumber
            }
            protobuf::BuiltInWindowFunction::Rank => BuiltInWindowFunction::Rank,
            protobuf::BuiltInWindowFunction::PercentRank => {
                BuiltInWindowFunction::PercentRank
            }
            protobuf::BuiltInWindowFunction::DenseRank => {
                BuiltInWindowFunction::DenseRank
            }
            protobuf::BuiltInWindowFunction::Lag => BuiltInWindowFunction::Lag,
            protobuf::BuiltInWindowFunction::Lead => BuiltInWindowFunction::Lead,
            protobuf::BuiltInWindowFunction::FirstValue => {
                BuiltInWindowFunction::FirstValue
            }
            protobuf::BuiltInWindowFunction::CumeDist => BuiltInWindowFunction::CumeDist,
            protobuf::BuiltInWindowFunction::Ntile => BuiltInWindowFunction::Ntile,
            protobuf::BuiltInWindowFunction::NthValue => BuiltInWindowFunction::NthValue,
            protobuf::BuiltInWindowFunction::LastValue => {
                BuiltInWindowFunction::LastValue
            }
        }
    }
}

impl TryInto<datafusion::arrow::datatypes::DataType>
    for &protobuf::arrow_type::ArrowTypeEnum
{
    type Error = BallistaError;
    fn try_into(self) -> Result<datafusion::arrow::datatypes::DataType, Self::Error> {
        use datafusion::arrow::datatypes::DataType;
        use protobuf::arrow_type;
        Ok(match self {
            arrow_type::ArrowTypeEnum::None(_) => DataType::Null,
            arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => {
                DataType::FixedSizeBinary(*size)
            }
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            arrow_type::ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.to_owned()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(protobuf::TimeUnit::from_i32_to_arrow(*time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => DataType::Interval(
                protobuf::IntervalUnit::from_i32_to_arrow(*interval_unit)?,
            ),
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal {
                whole,
                fractional,
            }) => DataType::Decimal(*whole as usize, *fractional as usize),
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::List(Box::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                DataType::LargeList(Box::new(list_type.try_into()?))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type: &protobuf::Field = list
                    .as_ref()
                    .field_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: List message missing required field 'field_type'"))?
                    .as_ref();
                let list_size = list.list_size;
                DataType::FixedSizeList(Box::new(list_type.try_into()?), list_size)
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                strct
                    .sub_field_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Union(union) => {
                let union_mode = protobuf::UnionMode::from_i32(union.union_mode)
                    .ok_or_else(|| {
                        proto_error(
                            "Protobuf deserialization error: Unknown union mode type",
                        )
                    })?;
                let union_mode = match union_mode {
                    protobuf::UnionMode::Dense => UnionMode::Dense,
                    protobuf::UnionMode::Sparse => UnionMode::Sparse,
                };
                let union_types = union
                    .union_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                DataType::Union(union_types, union_mode)
            }
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let pb_key_datatype = dict
                    .as_ref()
                    .key
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let pb_value_datatype = dict
                    .as_ref()
                    .value
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Dictionary message missing required field 'key'"))?;
                let key_datatype: DataType = pb_key_datatype.as_ref().try_into()?;
                let value_datatype: DataType = pb_value_datatype.as_ref().try_into()?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
            }
        })
    }
}

#[allow(clippy::from_over_into)]
impl Into<datafusion::arrow::datatypes::DataType> for protobuf::PrimitiveScalarType {
    fn into(self) -> datafusion::arrow::datatypes::DataType {
        use datafusion::arrow::datatypes::{DataType, TimeUnit};
        match self {
            protobuf::PrimitiveScalarType::Bool => DataType::Boolean,
            protobuf::PrimitiveScalarType::Uint8 => DataType::UInt8,
            protobuf::PrimitiveScalarType::Int8 => DataType::Int8,
            protobuf::PrimitiveScalarType::Uint16 => DataType::UInt16,
            protobuf::PrimitiveScalarType::Int16 => DataType::Int16,
            protobuf::PrimitiveScalarType::Uint32 => DataType::UInt32,
            protobuf::PrimitiveScalarType::Int32 => DataType::Int32,
            protobuf::PrimitiveScalarType::Uint64 => DataType::UInt64,
            protobuf::PrimitiveScalarType::Int64 => DataType::Int64,
            protobuf::PrimitiveScalarType::Float32 => DataType::Float32,
            protobuf::PrimitiveScalarType::Float64 => DataType::Float64,
            protobuf::PrimitiveScalarType::Utf8 => DataType::Utf8,
            protobuf::PrimitiveScalarType::LargeUtf8 => DataType::LargeUtf8,
            protobuf::PrimitiveScalarType::Date32 => DataType::Date32,
            protobuf::PrimitiveScalarType::TimeMicrosecond => {
                DataType::Time64(TimeUnit::Microsecond)
            }
            protobuf::PrimitiveScalarType::TimeNanosecond => {
                DataType::Time64(TimeUnit::Nanosecond)
            }
            protobuf::PrimitiveScalarType::Null => DataType::Null,
            protobuf::PrimitiveScalarType::Decimal128 => DataType::Decimal(0, 0),
            protobuf::PrimitiveScalarType::Date64 => DataType::Date64,
            protobuf::PrimitiveScalarType::TimeSecond => {
                DataType::Timestamp(TimeUnit::Second, None)
            }
            protobuf::PrimitiveScalarType::TimeMillisecond => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            protobuf::PrimitiveScalarType::IntervalYearmonth => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            protobuf::PrimitiveScalarType::IntervalDaytime => {
                DataType::Interval(IntervalUnit::DayTime)
            }
        }
    }
}

impl From<protobuf::JoinType> for JoinType {
    fn from(t: protobuf::JoinType) -> Self {
        match t {
            protobuf::JoinType::Inner => JoinType::Inner,
            protobuf::JoinType::Left => JoinType::Left,
            protobuf::JoinType::Right => JoinType::Right,
            protobuf::JoinType::Full => JoinType::Full,
            protobuf::JoinType::Semi => JoinType::Semi,
            protobuf::JoinType::Anti => JoinType::Anti,
        }
    }
}

impl From<JoinType> for protobuf::JoinType {
    fn from(t: JoinType) -> Self {
        match t {
            JoinType::Inner => protobuf::JoinType::Inner,
            JoinType::Left => protobuf::JoinType::Left,
            JoinType::Right => protobuf::JoinType::Right,
            JoinType::Full => protobuf::JoinType::Full,
            JoinType::Semi => protobuf::JoinType::Semi,
            JoinType::Anti => protobuf::JoinType::Anti,
        }
    }
}

impl From<protobuf::JoinConstraint> for JoinConstraint {
    fn from(t: protobuf::JoinConstraint) -> Self {
        match t {
            protobuf::JoinConstraint::On => JoinConstraint::On,
            protobuf::JoinConstraint::Using => JoinConstraint::Using,
        }
    }
}

impl From<JoinConstraint> for protobuf::JoinConstraint {
    fn from(t: JoinConstraint) -> Self {
        match t {
            JoinConstraint::On => protobuf::JoinConstraint::On,
            JoinConstraint::Using => protobuf::JoinConstraint::Using,
        }
    }
}

fn byte_to_string(b: u8) -> Result<String, BallistaError> {
    let b = &[b];
    let b = std::str::from_utf8(b)
        .map_err(|_| BallistaError::General("Invalid CSV delimiter".to_owned()))?;
    Ok(b.to_owned())
}

fn str_to_byte(s: &str) -> Result<u8, BallistaError> {
    if s.len() != 1 {
        return Err(BallistaError::General("Invalid CSV delimiter".to_owned()));
    }
    Ok(s.as_bytes()[0])
}

fn vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into().unwrap_or_else(|v: Vec<T>| {
        panic!("Expected a Vec of length {} but it was {}", N, v.len())
    })
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::datasource::object_store::local::LocalFileSystem;
    use datafusion::error::DataFusionError;
    use datafusion::execution::context::{ExecutionContextState, QueryPlanner};
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::logical_plan::plan::Extension;
    use datafusion::logical_plan::{
        col, DFSchemaRef, Expr, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
    };
    use datafusion::physical_plan::expressions::PhysicalSortExpr;
    use datafusion::physical_plan::planner::{DefaultPhysicalPlanner, ExtensionPlanner};
    use datafusion::physical_plan::{
        DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalPlanner,
        SendableRecordBatchStream, Statistics,
    };
    use datafusion::prelude::{CsvReadOptions, ExecutionConfig, ExecutionContext};
    use prost::Message;
    use std::any::Any;

    use std::convert::TryInto;
    use std::fmt;
    use std::fmt::{Debug, Formatter};
    use std::sync::Arc;

    pub mod proto {
        use crate::serde::protobuf;

        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TopKPlanProto {
            #[prost(uint64, tag = "1")]
            pub k: u64,

            #[prost(message, optional, tag = "2")]
            pub expr: ::core::option::Option<protobuf::LogicalExprNode>,
        }

        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TopKExecProto {
            #[prost(uint64, tag = "1")]
            pub k: u64,
        }
    }

    use crate::error::BallistaError;
    use crate::serde::protobuf::{LogicalPlanNode, PhysicalPlanNode};
    use crate::serde::{
        AsExecutionPlan, AsLogicalPlan, LogicalExtensionCodec, PhysicalExtensionCodec,
    };
    use proto::{TopKExecProto, TopKPlanProto};

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
        ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
            assert_eq!(inputs.len(), 1, "input size inconsistent");
            assert_eq!(exprs.len(), 1, "expression size inconsistent");
            Arc::new(TopKPlanNode {
                k: self.k,
                input: inputs[0].clone(),
                expr: exprs[0].clone(),
            })
        }
    }

    struct TopKExec {
        input: Arc<dyn ExecutionPlan>,
        /// The maxium number of values
        k: usize,
    }

    impl TopKExec {
        pub fn new(k: usize, input: Arc<dyn ExecutionPlan>) -> Self {
            Self { input, k }
        }
    }

    impl Debug for TopKExec {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "TopKExec")
        }
    }

    #[async_trait]
    impl ExecutionPlan for TopKExec {
        /// Return a reference to Any that can be used for downcasting
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.input.schema()
        }

        fn output_partitioning(&self) -> Partitioning {
            Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            None
        }

        fn required_child_distribution(&self) -> Distribution {
            Distribution::SinglePartition
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![self.input.clone()]
        }

        fn with_new_children(
            &self,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            match children.len() {
                1 => Ok(Arc::new(TopKExec {
                    input: children[0].clone(),
                    k: self.k,
                })),
                _ => Err(DataFusionError::Internal(
                    "TopKExec wrong number of children".to_string(),
                )),
            }
        }

        /// Execute one partition and return an iterator over RecordBatch
        async fn execute(
            &self,
            _partition: usize,
            _runtime: Arc<RuntimeEnv>,
        ) -> datafusion::error::Result<SendableRecordBatchStream> {
            Err(DataFusionError::NotImplemented(
                "not implemented".to_string(),
            ))
        }

        fn fmt_as(
            &self,
            t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            match t {
                DisplayFormatType::Default => {
                    write!(f, "TopKExec: k={}", self.k)
                }
            }
        }

        fn statistics(&self) -> Statistics {
            // to improve the optimizability of this plan
            // better statistics inference could be provided
            Statistics::default()
        }
    }

    struct TopKPlanner {}

    impl ExtensionPlanner for TopKPlanner {
        /// Create a physical plan for an extension node
        fn plan_extension(
            &self,
            _planner: &dyn PhysicalPlanner,
            node: &dyn UserDefinedLogicalNode,
            logical_inputs: &[&LogicalPlan],
            physical_inputs: &[Arc<dyn ExecutionPlan>],
            _ctx_state: &ExecutionContextState,
        ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(
                if let Some(topk_node) = node.as_any().downcast_ref::<TopKPlanNode>() {
                    assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                    assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
                    // figure out input name
                    Some(Arc::new(TopKExec {
                        input: physical_inputs[0].clone(),
                        k: topk_node.k,
                    }))
                } else {
                    None
                },
            )
        }
    }

    struct TopKQueryPlanner {}

    #[async_trait]
    impl QueryPlanner for TopKQueryPlanner {
        /// Given a `LogicalPlan` created from above, create an
        /// `ExecutionPlan` suitable for execution
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            ctx_state: &ExecutionContextState,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            // Teach the default physical planner how to plan TopK nodes.
            let physical_planner =
                DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                    TopKPlanner {},
                )]);
            // Delegate most work of physical planning to the default physical planner
            physical_planner
                .create_physical_plan(logical_plan, ctx_state)
                .await
        }
    }

    #[derive(Debug)]
    pub struct TopKExtensionCodec {}

    impl LogicalExtensionCodec for TopKExtensionCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[LogicalPlan],
        ) -> Result<Extension, BallistaError> {
            if let Some((input, _)) = inputs.split_first() {
                let proto = TopKPlanProto::decode(buf).map_err(|e| {
                    BallistaError::Internal(format!(
                        "failed to decode logical plan: {:?}",
                        e
                    ))
                })?;

                if let Some(expr) = proto.expr.as_ref() {
                    let node = TopKPlanNode::new(
                        proto.k as usize,
                        input.clone(),
                        expr.try_into()?,
                    );

                    Ok(Extension {
                        node: Arc::new(node),
                    })
                } else {
                    Err(BallistaError::from("invalid plan, no expr".to_string()))
                }
            } else {
                Err(BallistaError::from("invalid plan, no input".to_string()))
            }
        }

        fn try_encode(
            &self,
            node: &Extension,
            buf: &mut Vec<u8>,
        ) -> Result<(), BallistaError> {
            if let Some(exec) = node.node.as_any().downcast_ref::<TopKPlanNode>() {
                let proto = TopKPlanProto {
                    k: exec.k as u64,
                    expr: Some((&exec.expr).try_into()?),
                };

                proto.encode(buf).map_err(|e| {
                    BallistaError::Internal(format!(
                        "failed to encode logical plan: {:?}",
                        e
                    ))
                })?;

                Ok(())
            } else {
                Err(BallistaError::from("unsupported plan type".to_string()))
            }
        }
    }

    impl PhysicalExtensionCodec for TopKExtensionCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
        ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
            if let Some((input, _)) = inputs.split_first() {
                let proto = TopKExecProto::decode(buf).map_err(|e| {
                    BallistaError::Internal(format!(
                        "failed to decode execution plan: {:?}",
                        e
                    ))
                })?;
                Ok(Arc::new(TopKExec::new(proto.k as usize, input.clone())))
            } else {
                Err(BallistaError::from("invalid plan, no input".to_string()))
            }
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> Result<(), BallistaError> {
            if let Some(exec) = node.as_any().downcast_ref::<TopKExec>() {
                let proto = TopKExecProto { k: exec.k as u64 };

                proto.encode(buf).map_err(|e| {
                    BallistaError::Internal(format!(
                        "failed to encode execution plan: {:?}",
                        e
                    ))
                })?;

                Ok(())
            } else {
                Err(BallistaError::from("unsupported plan type".to_string()))
            }
        }
    }

    #[tokio::test]
    async fn test_extension_plan() -> crate::error::Result<()> {
        let store = Arc::new(LocalFileSystem {});
        let config =
            ExecutionConfig::new().with_query_planner(Arc::new(TopKQueryPlanner {}));

        let ctx = ExecutionContext::with_config(config);

        let scan = LogicalPlanBuilder::scan_csv(
            store,
            "../../../datafusion/tests/customer.csv",
            CsvReadOptions::default(),
            None,
            1,
        )
        .await?
        .build()?;

        let topk_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(TopKPlanNode::new(3, scan, col("revenue"))),
        });

        let topk_exec = ctx.create_physical_plan(&topk_plan).await?;

        let extension_codec = TopKExtensionCodec {};

        let proto = LogicalPlanNode::try_from_logical_plan(&topk_plan, &extension_codec)?;
        let logical_round_trip = proto.try_into_logical_plan(&ctx, &extension_codec)?;

        assert_eq!(
            format!("{:?}", topk_plan),
            format!("{:?}", logical_round_trip)
        );

        let proto = PhysicalPlanNode::try_from_physical_plan(
            topk_exec.clone(),
            &extension_codec,
        )?;
        let physical_round_trip = proto.try_into_physical_plan(&ctx, &extension_codec)?;

        assert_eq!(
            format!("{:?}", topk_exec),
            format!("{:?}", physical_round_trip)
        );

        Ok(())
    }
}
