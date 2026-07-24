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

//! Protobuf conversions shared by the join operators' `try_to_proto` /
//! `try_from_proto` implementations.
//!
//! The enum conversions are by-name exhaustive matches on purpose: the proto
//! enums and the `datafusion_common` enums are numbered differently, so a
//! numeric cast would silently corrupt them.

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::{
    JoinSide, JoinType, NullEquality, Result, internal_datafusion_err,
};
use datafusion_proto_models::protobuf;

use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::proto::{ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx};

pub(crate) fn join_type_to_proto(join_type: JoinType) -> protobuf::JoinType {
    match join_type {
        JoinType::Inner => protobuf::JoinType::Inner,
        JoinType::Left => protobuf::JoinType::Left,
        JoinType::Right => protobuf::JoinType::Right,
        JoinType::Full => protobuf::JoinType::Full,
        JoinType::LeftSemi => protobuf::JoinType::Leftsemi,
        JoinType::RightSemi => protobuf::JoinType::Rightsemi,
        JoinType::LeftAnti => protobuf::JoinType::Leftanti,
        JoinType::RightAnti => protobuf::JoinType::Rightanti,
        JoinType::LeftMark => protobuf::JoinType::Leftmark,
        JoinType::RightMark => protobuf::JoinType::Rightmark,
    }
}

pub(crate) fn join_type_from_proto(value: i32, plan_name: &str) -> Result<JoinType> {
    let join_type = protobuf::JoinType::try_from(value)
        .map_err(|_| internal_datafusion_err!("{plan_name}: unknown JoinType {value}"))?;
    Ok(match join_type {
        protobuf::JoinType::Inner => JoinType::Inner,
        protobuf::JoinType::Left => JoinType::Left,
        protobuf::JoinType::Right => JoinType::Right,
        protobuf::JoinType::Full => JoinType::Full,
        protobuf::JoinType::Leftsemi => JoinType::LeftSemi,
        protobuf::JoinType::Rightsemi => JoinType::RightSemi,
        protobuf::JoinType::Leftanti => JoinType::LeftAnti,
        protobuf::JoinType::Rightanti => JoinType::RightAnti,
        protobuf::JoinType::Leftmark => JoinType::LeftMark,
        protobuf::JoinType::Rightmark => JoinType::RightMark,
    })
}

pub(crate) fn join_side_to_proto(side: JoinSide) -> protobuf::JoinSide {
    match side {
        JoinSide::Left => protobuf::JoinSide::LeftSide,
        JoinSide::Right => protobuf::JoinSide::RightSide,
        JoinSide::None => protobuf::JoinSide::None,
    }
}

pub(crate) fn join_side_from_proto(value: i32, plan_name: &str) -> Result<JoinSide> {
    let side = protobuf::JoinSide::try_from(value)
        .map_err(|_| internal_datafusion_err!("{plan_name}: unknown JoinSide {value}"))?;
    Ok(match side {
        protobuf::JoinSide::LeftSide => JoinSide::Left,
        protobuf::JoinSide::RightSide => JoinSide::Right,
        protobuf::JoinSide::None => JoinSide::None,
    })
}

pub(crate) fn null_equality_to_proto(
    null_equality: NullEquality,
) -> protobuf::NullEquality {
    match null_equality {
        NullEquality::NullEqualsNothing => protobuf::NullEquality::NullEqualsNothing,
        NullEquality::NullEqualsNull => protobuf::NullEquality::NullEqualsNull,
    }
}

pub(crate) fn null_equality_from_proto(
    value: i32,
    plan_name: &str,
) -> Result<NullEquality> {
    let null_equality = protobuf::NullEquality::try_from(value).map_err(|_| {
        internal_datafusion_err!("{plan_name}: unknown NullEquality {value}")
    })?;
    Ok(match null_equality {
        protobuf::NullEquality::NullEqualsNothing => NullEquality::NullEqualsNothing,
        protobuf::NullEquality::NullEqualsNull => NullEquality::NullEqualsNull,
    })
}

pub(crate) fn join_filter_to_proto(
    filter: &JoinFilter,
    ctx: &ExecutionPlanEncodeCtx<'_>,
) -> Result<protobuf::JoinFilter> {
    let expression = ctx.encode_expr(filter.expression())?;
    let column_indices = filter
        .column_indices()
        .iter()
        .map(|column_index| protobuf::ColumnIndex {
            index: column_index.index as u32,
            side: join_side_to_proto(column_index.side).into(),
        })
        .collect();
    Ok(protobuf::JoinFilter {
        expression: Some(expression),
        column_indices,
        schema: Some(filter.schema().as_ref().try_into()?),
    })
}

pub(crate) fn join_filter_from_proto(
    filter: &protobuf::JoinFilter,
    ctx: &ExecutionPlanDecodeCtx<'_>,
    plan_name: &str,
) -> Result<JoinFilter> {
    let schema: Schema = filter
        .schema
        .as_ref()
        .ok_or_else(|| {
            internal_datafusion_err!("{plan_name}: JoinFilter missing schema")
        })?
        .try_into()?;
    let expression = ctx.decode_required_expr(
        filter.expression.as_ref(),
        &schema,
        plan_name,
        "filter.expression",
    )?;
    let column_indices = filter
        .column_indices
        .iter()
        .map(|column_index| {
            Ok(ColumnIndex {
                index: column_index.index as usize,
                side: join_side_from_proto(column_index.side, plan_name)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(JoinFilter::new(
        expression,
        column_indices,
        Arc::new(schema),
    ))
}
