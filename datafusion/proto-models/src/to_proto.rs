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

//! Conversions from `datafusion-common` types to the protobuf messages in this
//! crate.
//!
//! See [`crate::from_proto`] for why the impls live here rather than next to
//! the DataFusion types.

use datafusion_common::DataFusionError;
use datafusion_common::display::{PlanType, StringifiedPlan};
use datafusion_common::{
    JoinConstraint, JoinType, NullEquality, TableReference, UnnestOptions,
};

use crate::generated::datafusion_common::EmptyMessage;
use crate::protobuf::{
    self, AnalyzedLogicalPlanType, OptimizedLogicalPlanType, OptimizedPhysicalPlanType,
    RecursionUnnestOption,
    plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, FinalPhysicalPlanWithSchema, FinalPhysicalPlanWithStats,
        InitialLogicalPlan, InitialPhysicalPlan, InitialPhysicalPlanWithSchema,
        InitialPhysicalPlanWithStats, OptimizedLogicalPlan, OptimizedPhysicalPlan,
        PhysicalPlanError,
    },
};

impl From<&UnnestOptions> for protobuf::UnnestOptions {
    fn from(opts: &UnnestOptions) -> Self {
        use datafusion_common::NullHandling;
        use protobuf::unnest_options::NullHandling as ProtoNullHandling;
        let null_handling = match opts.null_handling {
            NullHandling::Preserve => ProtoNullHandling::Preserve,
            NullHandling::Drop => ProtoNullHandling::Drop,
            NullHandling::PreserveAndExpandEmpty => {
                ProtoNullHandling::PreserveAndExpandEmpty
            }
        } as i32;
        Self {
            null_handling,
            recursions: opts
                .recursions
                .iter()
                .map(|r| RecursionUnnestOption {
                    input_column: Some((&r.input_column).into()),
                    output_column: Some((&r.output_column).into()),
                    depth: r.depth as u32,
                })
                .collect(),
        }
    }
}

impl From<&StringifiedPlan> for protobuf::StringifiedPlan {
    fn from(stringified_plan: &StringifiedPlan) -> Self {
        Self {
            plan_type: match stringified_plan.clone().plan_type {
                PlanType::InitialLogicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(InitialLogicalPlan(EmptyMessage {})),
                }),
                PlanType::AnalyzedLogicalPlan { analyzer_name } => {
                    Some(protobuf::PlanType {
                        plan_type_enum: Some(AnalyzedLogicalPlan(
                            AnalyzedLogicalPlanType { analyzer_name },
                        )),
                    })
                }
                PlanType::FinalAnalyzedLogicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalAnalyzedLogicalPlan(EmptyMessage {})),
                }),
                PlanType::OptimizedLogicalPlan { optimizer_name } => {
                    Some(protobuf::PlanType {
                        plan_type_enum: Some(OptimizedLogicalPlan(
                            OptimizedLogicalPlanType { optimizer_name },
                        )),
                    })
                }
                PlanType::FinalLogicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalLogicalPlan(EmptyMessage {})),
                }),
                PlanType::InitialPhysicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(InitialPhysicalPlan(EmptyMessage {})),
                }),
                PlanType::OptimizedPhysicalPlan { optimizer_name } => {
                    Some(protobuf::PlanType {
                        plan_type_enum: Some(OptimizedPhysicalPlan(
                            OptimizedPhysicalPlanType { optimizer_name },
                        )),
                    })
                }
                PlanType::FinalPhysicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalPhysicalPlan(EmptyMessage {})),
                }),
                PlanType::InitialPhysicalPlanWithStats => Some(protobuf::PlanType {
                    plan_type_enum: Some(InitialPhysicalPlanWithStats(EmptyMessage {})),
                }),
                PlanType::InitialPhysicalPlanWithSchema => Some(protobuf::PlanType {
                    plan_type_enum: Some(InitialPhysicalPlanWithSchema(EmptyMessage {})),
                }),
                PlanType::FinalPhysicalPlanWithStats => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalPhysicalPlanWithStats(EmptyMessage {})),
                }),
                PlanType::FinalPhysicalPlanWithSchema => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalPhysicalPlanWithSchema(EmptyMessage {})),
                }),
                PlanType::PhysicalPlanError => Some(protobuf::PlanType {
                    plan_type_enum: Some(PhysicalPlanError(EmptyMessage {})),
                }),
            },
            plan: stringified_plan.plan.to_string(),
        }
    }
}

impl From<TableReference> for protobuf::TableReference {
    fn from(t: TableReference) -> Self {
        use protobuf::table_reference::TableReferenceEnum;
        let table_reference_enum = match t {
            TableReference::Bare { table } => {
                TableReferenceEnum::Bare(protobuf::BareTableReference {
                    table: table.to_string(),
                })
            }
            TableReference::Partial { schema, table } => {
                TableReferenceEnum::Partial(protobuf::PartialTableReference {
                    schema: schema.to_string(),
                    table: table.to_string(),
                })
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => TableReferenceEnum::Full(protobuf::FullTableReference {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            }),
        };

        protobuf::TableReference {
            table_reference_enum: Some(table_reference_enum),
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
            JoinType::LeftSemi => protobuf::JoinType::Leftsemi,
            JoinType::RightSemi => protobuf::JoinType::Rightsemi,
            JoinType::LeftAnti => protobuf::JoinType::Leftanti,
            JoinType::RightAnti => protobuf::JoinType::Rightanti,
            JoinType::LeftMark => protobuf::JoinType::Leftmark,
            JoinType::RightMark => protobuf::JoinType::Rightmark,
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

impl From<NullEquality> for protobuf::NullEquality {
    fn from(t: NullEquality) -> Self {
        match t {
            NullEquality::NullEqualsNothing => protobuf::NullEquality::NullEqualsNothing,
            NullEquality::NullEqualsNull => protobuf::NullEquality::NullEqualsNull,
        }
    }
}

/// Encode any slice of file-like values as a [`protobuf::FileGroup`].
///
/// `datafusion-datasource` cannot host this impl: `&T` is `#[fundamental]` but
/// `[T]` is not, so `&[PartitionedFile]` counts as foreign there and the orphan
/// rule rejects it. Here the *self* type is local, which is all the orphan rule
/// needs — and staying generic over the element means this crate never has to
/// name `PartitionedFile`, which lives above it in the dependency graph.
///
/// The element bound is satisfied by
/// `impl TryFrom<&PartitionedFile> for protobuf::PartitionedFile` in
/// `datafusion-datasource`, so `protobuf::FileGroup::try_from(&files[..])`
/// resolves for callers exactly as it did before the proto types were split out.
impl<T> TryFrom<&[T]> for protobuf::FileGroup
where
    for<'a> &'a T: TryInto<protobuf::PartitionedFile, Error = DataFusionError>,
{
    type Error = DataFusionError;

    fn try_from(files: &[T]) -> Result<Self, Self::Error> {
        Ok(protobuf::FileGroup {
            files: files
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::{NullHandling, RecursionUnnestOption};

    use super::*;

    #[test]
    fn table_reference_roundtrip() {
        for reference in [
            TableReference::bare("t"),
            TableReference::partial("s", "t"),
            TableReference::full("c", "s", "t"),
        ] {
            let encoded = protobuf::TableReference::from(reference.clone());
            let decoded = TableReference::try_from(encoded).unwrap();
            assert_eq!(decoded, reference);
        }
    }

    #[test]
    fn table_reference_from_proto_rejects_missing_oneof() {
        let proto = protobuf::TableReference {
            table_reference_enum: None,
        };
        let err = TableReference::try_from(proto).unwrap_err();
        assert!(
            err.to_string().contains("table_reference_enum"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn join_enums_roundtrip() {
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::LeftMark,
            JoinType::RightMark,
        ] {
            assert_eq!(
                JoinType::from(protobuf::JoinType::from(join_type)),
                join_type
            );
        }
        for constraint in [JoinConstraint::On, JoinConstraint::Using] {
            assert_eq!(
                JoinConstraint::from(protobuf::JoinConstraint::from(constraint)),
                constraint
            );
        }
        for null_equality in [
            NullEquality::NullEqualsNothing,
            NullEquality::NullEqualsNull,
        ] {
            assert_eq!(
                NullEquality::from(protobuf::NullEquality::from(null_equality)),
                null_equality
            );
        }
    }

    #[test]
    fn unnest_options_roundtrip() {
        let options = UnnestOptions {
            null_handling: NullHandling::Drop,
            recursions: vec![RecursionUnnestOption {
                input_column: "a".into(),
                output_column: "b".into(),
                depth: 2,
            }],
        };

        let encoded = protobuf::UnnestOptions::from(&options);
        let decoded = UnnestOptions::from(&encoded);

        assert_eq!(decoded.null_handling, options.null_handling);
        assert_eq!(decoded.recursions, options.recursions);
    }

    #[test]
    fn stringified_plan_roundtrip() {
        let plan = StringifiedPlan::new(
            PlanType::OptimizedLogicalPlan {
                optimizer_name: "push_down_filter".to_string(),
            },
            "some plan",
        );

        let encoded = protobuf::StringifiedPlan::from(&plan);
        let decoded = StringifiedPlan::from(&encoded);

        assert_eq!(decoded.plan_type, plan.plan_type);
        assert_eq!(decoded.plan, plan.plan);
    }
}
