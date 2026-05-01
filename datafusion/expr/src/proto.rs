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

//! Conversions between `datafusion-proto-common` types and `datafusion-expr`
//! types. Enabled by the `proto` feature.

use datafusion_common::{DataFusionError, ScalarValue, plan_datafusion_err};
use datafusion_proto_common::protobuf;

use crate::dml::InsertOp;
use crate::expr::NullTreatment;
use crate::{WindowFrame, WindowFrameBound, WindowFrameUnits, WriteOp};

impl From<protobuf::WindowFrameUnits> for WindowFrameUnits {
    fn from(units: protobuf::WindowFrameUnits) -> Self {
        match units {
            protobuf::WindowFrameUnits::Rows => Self::Rows,
            protobuf::WindowFrameUnits::Range => Self::Range,
            protobuf::WindowFrameUnits::Groups => Self::Groups,
        }
    }
}

impl From<WindowFrameUnits> for protobuf::WindowFrameUnits {
    fn from(units: WindowFrameUnits) -> Self {
        match units {
            WindowFrameUnits::Rows => Self::Rows,
            WindowFrameUnits::Range => Self::Range,
            WindowFrameUnits::Groups => Self::Groups,
        }
    }
}

impl TryFrom<protobuf::WindowFrame> for WindowFrame {
    type Error = DataFusionError;

    fn try_from(window: protobuf::WindowFrame) -> Result<Self, Self::Error> {
        let units = WindowFrameUnits::from(
            protobuf::WindowFrameUnits::try_from(window.window_frame_units).map_err(
                |_| {
                    plan_datafusion_err!(
                        "Unknown i32 value for WindowFrameUnits enum: {}",
                        window.window_frame_units
                    )
                },
            )?,
        );
        let start_bound =
            WindowFrameBound::try_from(window.start_bound.ok_or_else(|| {
                plan_datafusion_err!("Missing required field start_bound")
            })?)?;
        let end_bound = window
            .end_bound
            .map(|end_bound| match end_bound {
                protobuf::window_frame::EndBound::Bound(end_bound) => {
                    WindowFrameBound::try_from(end_bound)
                }
            })
            .transpose()?
            .unwrap_or(WindowFrameBound::CurrentRow);
        Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
    }
}

impl TryFrom<&WindowFrame> for protobuf::WindowFrame {
    type Error = DataFusionError;

    fn try_from(window: &WindowFrame) -> Result<Self, Self::Error> {
        Ok(protobuf::WindowFrame {
            window_frame_units: protobuf::WindowFrameUnits::from(window.units).into(),
            start_bound: Some((&window.start_bound).try_into()?),
            end_bound: Some(protobuf::window_frame::EndBound::Bound(
                (&window.end_bound).try_into()?,
            )),
        })
    }
}

impl TryFrom<protobuf::WindowFrameBound> for WindowFrameBound {
    type Error = DataFusionError;

    fn try_from(bound: protobuf::WindowFrameBound) -> Result<Self, Self::Error> {
        let bound_type =
            protobuf::WindowFrameBoundType::try_from(bound.window_frame_bound_type)
                .map_err(|_| {
                    plan_datafusion_err!(
                        "Unknown i32 value for WindowFrameBoundType enum: {}",
                        bound.window_frame_bound_type
                    )
                })?;
        match bound_type {
            protobuf::WindowFrameBoundType::CurrentRow => Ok(Self::CurrentRow),
            protobuf::WindowFrameBoundType::Preceding => match bound.bound_value {
                Some(x) => Ok(Self::Preceding(ScalarValue::try_from(&x)?)),
                None => Ok(Self::Preceding(ScalarValue::UInt64(None))),
            },
            protobuf::WindowFrameBoundType::Following => match bound.bound_value {
                Some(x) => Ok(Self::Following(ScalarValue::try_from(&x)?)),
                None => Ok(Self::Following(ScalarValue::UInt64(None))),
            },
        }
    }
}

impl TryFrom<&WindowFrameBound> for protobuf::WindowFrameBound {
    type Error = DataFusionError;

    fn try_from(bound: &WindowFrameBound) -> Result<Self, Self::Error> {
        Ok(match bound {
            WindowFrameBound::CurrentRow => protobuf::WindowFrameBound {
                window_frame_bound_type: protobuf::WindowFrameBoundType::CurrentRow
                    .into(),
                bound_value: None,
            },
            WindowFrameBound::Preceding(v) => protobuf::WindowFrameBound {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Preceding.into(),
                bound_value: Some(v.try_into()?),
            },
            WindowFrameBound::Following(v) => protobuf::WindowFrameBound {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Following.into(),
                bound_value: Some(v.try_into()?),
            },
        })
    }
}

impl From<protobuf::dml_node::Type> for WriteOp {
    fn from(t: protobuf::dml_node::Type) -> Self {
        match t {
            protobuf::dml_node::Type::Update => WriteOp::Update,
            protobuf::dml_node::Type::Delete => WriteOp::Delete,
            protobuf::dml_node::Type::InsertAppend => WriteOp::Insert(InsertOp::Append),
            protobuf::dml_node::Type::InsertOverwrite => {
                WriteOp::Insert(InsertOp::Overwrite)
            }
            protobuf::dml_node::Type::InsertReplace => WriteOp::Insert(InsertOp::Replace),
            protobuf::dml_node::Type::Ctas => WriteOp::Ctas,
            protobuf::dml_node::Type::Truncate => WriteOp::Truncate,
        }
    }
}

impl From<&WriteOp> for protobuf::dml_node::Type {
    fn from(op: &WriteOp) -> Self {
        match op {
            WriteOp::Update => Self::Update,
            WriteOp::Delete => Self::Delete,
            WriteOp::Insert(InsertOp::Append) => Self::InsertAppend,
            WriteOp::Insert(InsertOp::Overwrite) => Self::InsertOverwrite,
            WriteOp::Insert(InsertOp::Replace) => Self::InsertReplace,
            WriteOp::Ctas => Self::Ctas,
            WriteOp::Truncate => Self::Truncate,
        }
    }
}

impl From<protobuf::NullTreatment> for NullTreatment {
    fn from(t: protobuf::NullTreatment) -> Self {
        match t {
            protobuf::NullTreatment::RespectNulls => NullTreatment::RespectNulls,
            protobuf::NullTreatment::IgnoreNulls => NullTreatment::IgnoreNulls,
        }
    }
}

impl From<NullTreatment> for protobuf::NullTreatment {
    fn from(t: NullTreatment) -> Self {
        match t {
            NullTreatment::RespectNulls => Self::RespectNulls,
            NullTreatment::IgnoreNulls => Self::IgnoreNulls,
        }
    }
}
