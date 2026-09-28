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

//! Protobuf conversions for the expression types owned by this crate:
//! [`WindowFrame`], [`WindowFrameBound`], [`WindowFrameUnits`],
//! [`MergeIntoClauseKind`](crate::dml::MergeIntoClauseKind) and
//! [`NullTreatment`](crate::expr::NullTreatment).
//!
//! These are plain [`From`] / [`TryFrom`] impls rather than something taking a
//! codec: every field is either an enum tag or a [`ScalarValue`], so the
//! conversion needs nothing but the value itself. The orphan rule allows them
//! here because one side of each conversion is a type this crate owns.
//!
//! [`ScalarValue`]: datafusion_common::ScalarValue

use datafusion_common::ScalarValue;
use datafusion_proto_common::{FromProtoError, ToProtoError};
use datafusion_proto_models::protobuf;

use crate::dml::MergeIntoClauseKind;
use crate::expr::NullTreatment;
use crate::{WindowFrame, WindowFrameBound, WindowFrameUnits};

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

impl TryFrom<protobuf::WindowFrameBound> for WindowFrameBound {
    type Error = FromProtoError;

    fn try_from(bound: protobuf::WindowFrameBound) -> Result<Self, Self::Error> {
        let bound_type =
            protobuf::WindowFrameBoundType::try_from(bound.window_frame_bound_type)
                .map_err(|_| {
                    FromProtoError::unknown(
                        "WindowFrameBoundType",
                        bound.window_frame_bound_type,
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
    type Error = ToProtoError;

    fn try_from(bound: &WindowFrameBound) -> Result<Self, Self::Error> {
        Ok(match bound {
            WindowFrameBound::CurrentRow => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::CurrentRow
                    .into(),
                bound_value: None,
            },
            WindowFrameBound::Preceding(v) => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Preceding.into(),
                bound_value: Some(v.try_into()?),
            },
            WindowFrameBound::Following(v) => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Following.into(),
                bound_value: Some(v.try_into()?),
            },
        })
    }
}

impl TryFrom<protobuf::WindowFrame> for WindowFrame {
    type Error = FromProtoError;

    fn try_from(window: protobuf::WindowFrame) -> Result<Self, Self::Error> {
        let units = WindowFrameUnits::from(
            protobuf::WindowFrameUnits::try_from(window.window_frame_units).map_err(
                |_| {
                    FromProtoError::unknown("WindowFrameUnits", window.window_frame_units)
                },
            )?,
        );
        let start_bound = WindowFrameBound::try_from(
            window
                .start_bound
                .ok_or_else(|| FromProtoError::required("start_bound"))?,
        )?;
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
    type Error = ToProtoError;

    fn try_from(window: &WindowFrame) -> Result<Self, Self::Error> {
        Ok(Self {
            window_frame_units: protobuf::WindowFrameUnits::from(window.units).into(),
            start_bound: Some((&window.start_bound).try_into()?),
            end_bound: Some(protobuf::window_frame::EndBound::Bound(
                (&window.end_bound).try_into()?,
            )),
        })
    }
}

impl From<protobuf::merge_into_clause_node::Kind> for MergeIntoClauseKind {
    fn from(kind: protobuf::merge_into_clause_node::Kind) -> Self {
        match kind {
            protobuf::merge_into_clause_node::Kind::Matched => Self::Matched,
            protobuf::merge_into_clause_node::Kind::NotMatched => Self::NotMatched,
            protobuf::merge_into_clause_node::Kind::NotMatchedByTarget => {
                Self::NotMatchedByTarget
            }
            protobuf::merge_into_clause_node::Kind::NotMatchedBySource => {
                Self::NotMatchedBySource
            }
        }
    }
}

impl From<MergeIntoClauseKind> for protobuf::merge_into_clause_node::Kind {
    fn from(kind: MergeIntoClauseKind) -> Self {
        match kind {
            MergeIntoClauseKind::Matched => Self::Matched,
            MergeIntoClauseKind::NotMatched => Self::NotMatched,
            MergeIntoClauseKind::NotMatchedByTarget => Self::NotMatchedByTarget,
            MergeIntoClauseKind::NotMatchedBySource => Self::NotMatchedBySource,
        }
    }
}

impl From<protobuf::NullTreatment> for NullTreatment {
    fn from(t: protobuf::NullTreatment) -> Self {
        match t {
            protobuf::NullTreatment::RespectNulls => Self::RespectNulls,
            protobuf::NullTreatment::IgnoreNulls => Self::IgnoreNulls,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_frame_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2))),
            WindowFrameBound::Following(ScalarValue::UInt64(Some(3))),
        );

        let encoded = protobuf::WindowFrame::try_from(&frame)?;
        let decoded = WindowFrame::try_from(encoded)?;

        assert_eq!(decoded.units, frame.units);
        assert_eq!(decoded.start_bound, frame.start_bound);
        assert_eq!(decoded.end_bound, frame.end_bound);
        Ok(())
    }

    #[test]
    fn window_frame_from_proto_rejects_missing_start_bound() {
        let proto = protobuf::WindowFrame {
            window_frame_units: protobuf::WindowFrameUnits::Rows.into(),
            start_bound: None,
            end_bound: None,
        };

        let err = WindowFrame::try_from(proto).unwrap_err();
        assert!(
            err.to_string().contains("start_bound"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn missing_end_bound_decodes_as_current_row() -> Result<(), Box<dyn std::error::Error>>
    {
        let proto = protobuf::WindowFrame {
            window_frame_units: protobuf::WindowFrameUnits::Rows.into(),
            start_bound: Some(protobuf::WindowFrameBound {
                window_frame_bound_type: protobuf::WindowFrameBoundType::CurrentRow
                    .into(),
                bound_value: None,
            }),
            end_bound: None,
        };

        let decoded = WindowFrame::try_from(proto)?;
        assert_eq!(decoded.end_bound, WindowFrameBound::CurrentRow);
        Ok(())
    }
}
