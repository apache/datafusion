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

use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion_common::{
    exec_datafusion_err, internal_err, plan_datafusion_err, NullEquality,
    RecursionUnnestOption, Result, ScalarValue, TableReference, UnnestOptions,
};
use datafusion_execution::registry::FunctionRegistry;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::expr::{Alias, NullTreatment, Placeholder, Sort};
use datafusion_expr::expr::{Unnest, WildcardOptions};
use datafusion_expr::{
    expr::{self, InList, WindowFunction},
    logical_plan::{PlanType, StringifiedPlan},
    Between, BinaryExpr, Case, Cast, Expr, GroupingSet,
    GroupingSet::GroupingSets,
    JoinConstraint, JoinType, Like, Operator, TryCast, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};
use datafusion_expr::{ExprFunctionExt, WriteOp};
use datafusion_proto_common::{from_proto::FromOptionalField, FromProtoError as Error};

use crate::protobuf::plan_type::PlanTypeEnum::{
    FinalPhysicalPlanWithSchema, InitialPhysicalPlanWithSchema,
};
use crate::protobuf::{
    self,
    plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, FinalPhysicalPlanWithStats, InitialLogicalPlan,
        InitialPhysicalPlan, InitialPhysicalPlanWithStats, OptimizedLogicalPlan,
        OptimizedPhysicalPlan, PhysicalPlanError,
    },
    AnalyzedLogicalPlanType, CubeNode, GroupingSetNode, OptimizedLogicalPlanType,
    OptimizedPhysicalPlanType, PlaceholderNode, RollupNode,
};

use super::LogicalExtensionCodec;

impl From<&protobuf::UnnestOptions> for UnnestOptions {
    fn from(opts: &protobuf::UnnestOptions) -> Self {
        Self {
            preserve_nulls: opts.preserve_nulls,
            recursions: opts
                .recursions
                .iter()
                .map(|r| RecursionUnnestOption {
                    input_column: r.input_column.as_ref().unwrap().into(),
                    output_column: r.output_column.as_ref().unwrap().into(),
                    depth: r.depth as usize,
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl From<protobuf::WindowFrameUnits> for WindowFrameUnits {
    fn from(units: protobuf::WindowFrameUnits) -> Self {
        match units {
            protobuf::WindowFrameUnits::Rows => Self::Rows,
            protobuf::WindowFrameUnits::Range => Self::Range,
            protobuf::WindowFrameUnits::Groups => Self::Groups,
        }
    }
}

impl TryFrom<protobuf::TableReference> for TableReference {
    type Error = Error;

    fn try_from(value: protobuf::TableReference) -> Result<Self, Self::Error> {
        use protobuf::table_reference::TableReferenceEnum;
        let table_reference_enum = value
            .table_reference_enum
            .ok_or_else(|| Error::required("table_reference_enum"))?;

        match table_reference_enum {
            TableReferenceEnum::Bare(protobuf::BareTableReference { table }) => {
                Ok(TableReference::bare(table))
            }
            TableReferenceEnum::Partial(protobuf::PartialTableReference {
                schema,
                table,
            }) => Ok(TableReference::partial(schema, table)),
            TableReferenceEnum::Full(protobuf::FullTableReference {
                catalog,
                schema,
                table,
            }) => Ok(TableReference::full(catalog, schema, table)),
        }
    }
}

impl From<&protobuf::StringifiedPlan> for StringifiedPlan {
    fn from(stringified_plan: &protobuf::StringifiedPlan) -> Self {
        Self {
            plan_type: match stringified_plan
                .plan_type
                .as_ref()
                .and_then(|pt| pt.plan_type_enum.as_ref())
                .unwrap_or_else(|| {
                    panic!(
                        "Cannot create protobuf::StringifiedPlan from {stringified_plan:?}"
                    )
                }) {
                InitialLogicalPlan(_) => PlanType::InitialLogicalPlan,
                AnalyzedLogicalPlan(AnalyzedLogicalPlanType { analyzer_name }) => {
                    PlanType::AnalyzedLogicalPlan {
                        analyzer_name:analyzer_name.clone()
                    }
                }
                FinalAnalyzedLogicalPlan(_) => PlanType::FinalAnalyzedLogicalPlan,
                OptimizedLogicalPlan(OptimizedLogicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedLogicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalLogicalPlan(_) => PlanType::FinalLogicalPlan,
                InitialPhysicalPlan(_) => PlanType::InitialPhysicalPlan,
                InitialPhysicalPlanWithStats(_) => PlanType::InitialPhysicalPlanWithStats,
                InitialPhysicalPlanWithSchema(_) => PlanType::InitialPhysicalPlanWithSchema,
                OptimizedPhysicalPlan(OptimizedPhysicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedPhysicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalPhysicalPlan(_) => PlanType::FinalPhysicalPlan,
                FinalPhysicalPlanWithStats(_) => PlanType::FinalPhysicalPlanWithStats,
                FinalPhysicalPlanWithSchema(_) => PlanType::FinalPhysicalPlanWithSchema,
                PhysicalPlanError(_) => PlanType::PhysicalPlanError,
            },
            plan: Arc::new(stringified_plan.plan.clone()),
        }
    }
}

impl TryFrom<protobuf::WindowFrame> for WindowFrame {
    type Error = Error;

    fn try_from(window: protobuf::WindowFrame) -> Result<Self, Self::Error> {
        let units = protobuf::WindowFrameUnits::try_from(window.window_frame_units)
            .map_err(|_| Error::unknown("WindowFrameUnits", window.window_frame_units))?
            .into();
        let start_bound = window.start_bound.required("start_bound")?;
        let end_bound = window
            .end_bound
            .map(|end_bound| match end_bound {
                protobuf::window_frame::EndBound::Bound(end_bound) => {
                    end_bound.try_into()
                }
            })
            .transpose()?
            .unwrap_or(WindowFrameBound::CurrentRow);
        Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
    }
}

impl TryFrom<protobuf::WindowFrameBound> for WindowFrameBound {
    type Error = Error;

    fn try_from(bound: protobuf::WindowFrameBound) -> Result<Self, Self::Error> {
        let bound_type =
            protobuf::WindowFrameBoundType::try_from(bound.window_frame_bound_type)
                .map_err(|_| {
                    Error::unknown("WindowFrameBoundType", bound.window_frame_bound_type)
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

impl From<protobuf::JoinType> for JoinType {
    fn from(t: protobuf::JoinType) -> Self {
        match t {
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

impl From<protobuf::NullEquality> for NullEquality {
    fn from(t: protobuf::NullEquality) -> Self {
        match t {
            protobuf::NullEquality::NullEqualsNothing => NullEquality::NullEqualsNothing,
            protobuf::NullEquality::NullEqualsNull => NullEquality::NullEqualsNull,
        }
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

pub fn parse_expr(
    proto: &protobuf::LogicalExprNode,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Expr, Error> {
    use protobuf::{logical_expr_node::ExprType, window_expr_node};

    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| Error::required("expr_type"))?;

    match expr_type {
        ExprType::BinaryExpr(binary_expr) => {
            let op = from_proto_binary_op(&binary_expr.op)?;
            let operands = parse_exprs(&binary_expr.operands, registry, codec)?;

            if operands.len() < 2 {
                return Err(proto_error(
                    "A binary expression must always have at least 2 operands",
                ));
            }

            // Reduce the linearized operands (ordered by left innermost to right
            // outermost) into a single expression tree.
            Ok(operands
                .into_iter()
                .reduce(|left, right| {
                    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
                })
                .expect("Binary expression could not be reduced to a single expression."))
        }
        ExprType::Column(column) => Ok(Expr::Column(column.into())),
        ExprType::Literal(literal) => {
            let scalar_value: ScalarValue = literal.try_into()?;
            Ok(Expr::Literal(scalar_value, None))
        }
        ExprType::WindowExpr(expr) => {
            let window_function = expr
                .window_function
                .as_ref()
                .ok_or_else(|| Error::required("window_function"))?;
            let partition_by = parse_exprs(&expr.partition_by, registry, codec)?;
            let mut order_by = parse_sorts(&expr.order_by, registry, codec)?;
            let window_frame = expr
                .window_frame
                .as_ref()
                .map::<Result<WindowFrame, _>, _>(|window_frame| {
                    let window_frame: WindowFrame = window_frame.clone().try_into()?;
                    window_frame
                        .regularize_order_bys(&mut order_by)
                        .map(|_| window_frame)
                })
                .transpose()?
                .ok_or_else(|| {
                    exec_datafusion_err!("missing window frame during deserialization")
                })?;

            let null_treatment = match expr.null_treatment {
                Some(null_treatment) => {
                    let null_treatment  =  protobuf::NullTreatment::try_from(null_treatment)
                    .map_err(|_| {
                        proto_error(format!(
                            "Received a WindowExprNode message with unknown NullTreatment {null_treatment}",
                        ))
                    })?;
                    Some(NullTreatment::from(null_treatment))
                }
                None => None,
            };

            let agg_fn = match window_function {
                window_expr_node::WindowFunction::Udaf(udaf_name) => {
                    let udaf_function = match &expr.fun_definition {
                        Some(buf) => codec.try_decode_udaf(udaf_name, buf)?,
                        None => registry
                            .udaf(udaf_name)
                            .or_else(|_| codec.try_decode_udaf(udaf_name, &[]))?,
                    };
                    expr::WindowFunctionDefinition::AggregateUDF(udaf_function)
                }
                window_expr_node::WindowFunction::Udwf(udwf_name) => {
                    let udwf_function = match &expr.fun_definition {
                        Some(buf) => codec.try_decode_udwf(udwf_name, buf)?,
                        None => registry
                            .udwf(udwf_name)
                            .or_else(|_| codec.try_decode_udwf(udwf_name, &[]))?,
                    };
                    expr::WindowFunctionDefinition::WindowUDF(udwf_function)
                }
            };

            let args = parse_exprs(&expr.exprs, registry, codec)?;
            let mut builder = Expr::from(WindowFunction::new(agg_fn, args))
                .partition_by(partition_by)
                .order_by(order_by)
                .window_frame(window_frame)
                .null_treatment(null_treatment);

            if expr.distinct {
                builder = builder.distinct();
            };

            if let Some(filter) =
                parse_optional_expr(expr.filter.as_deref(), registry, codec)?
            {
                builder = builder.filter(filter);
            }

            builder.build().map_err(Error::DataFusionError)
        }
        ExprType::Alias(alias) => Ok(Expr::Alias(Alias::new(
            parse_required_expr(alias.expr.as_deref(), registry, "expr", codec)?,
            alias
                .relation
                .first()
                .map(|r| TableReference::try_from(r.clone()))
                .transpose()?,
            alias.alias.clone(),
        ))),
        ExprType::IsNullExpr(is_null) => Ok(Expr::IsNull(Box::new(parse_required_expr(
            is_null.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotNullExpr(is_not_null) => Ok(Expr::IsNotNull(Box::new(
            parse_required_expr(is_not_null.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::NotExpr(not) => Ok(Expr::Not(Box::new(parse_required_expr(
            not.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsTrue(msg) => Ok(Expr::IsTrue(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsFalse(msg) => Ok(Expr::IsFalse(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsUnknown(msg) => Ok(Expr::IsUnknown(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotTrue(msg) => Ok(Expr::IsNotTrue(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotFalse(msg) => Ok(Expr::IsNotFalse(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotUnknown(msg) => Ok(Expr::IsNotUnknown(Box::new(
            parse_required_expr(msg.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::Between(between) => Ok(Expr::Between(Between::new(
            Box::new(parse_required_expr(
                between.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            between.negated,
            Box::new(parse_required_expr(
                between.low.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                between.high.as_deref(),
                registry,
                "expr",
                codec,
            )?),
        ))),
        ExprType::Like(like) => Ok(Expr::Like(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            false,
        ))),
        ExprType::Ilike(like) => Ok(Expr::Like(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            true,
        ))),
        ExprType::SimilarTo(like) => Ok(Expr::SimilarTo(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            false,
        ))),
        ExprType::Case(case) => {
            let when_then_expr = case
                .when_then_expr
                .iter()
                .map(|e| {
                    let when_expr = parse_required_expr(
                        e.when_expr.as_ref(),
                        registry,
                        "when_expr",
                        codec,
                    )?;
                    let then_expr = parse_required_expr(
                        e.then_expr.as_ref(),
                        registry,
                        "then_expr",
                        codec,
                    )?;
                    Ok((Box::new(when_expr), Box::new(then_expr)))
                })
                .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, Error>>()?;
            Ok(Expr::Case(Case::new(
                parse_optional_expr(case.expr.as_deref(), registry, codec)?.map(Box::new),
                when_then_expr,
                parse_optional_expr(case.else_expr.as_deref(), registry, codec)?
                    .map(Box::new),
            )))
        }
        ExprType::Cast(cast) => {
            let expr = Box::new(parse_required_expr(
                cast.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?);
            let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
            Ok(Expr::Cast(Cast::new(expr, data_type)))
        }
        ExprType::TryCast(cast) => {
            let expr = Box::new(parse_required_expr(
                cast.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?);
            let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
            Ok(Expr::TryCast(TryCast::new(expr, data_type)))
        }
        ExprType::Negative(negative) => Ok(Expr::Negative(Box::new(
            parse_required_expr(negative.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::Unnest(unnest) => {
            let mut exprs = parse_exprs(&unnest.exprs, registry, codec)?;
            if exprs.len() != 1 {
                return Err(proto_error("Unnest must have exactly one expression"));
            }
            Ok(Expr::Unnest(Unnest::new(exprs.swap_remove(0))))
        }
        ExprType::InList(in_list) => Ok(Expr::InList(InList::new(
            Box::new(parse_required_expr(
                in_list.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            parse_exprs(&in_list.list, registry, codec)?,
            in_list.negated,
        ))),
        ExprType::Wildcard(protobuf::Wildcard { qualifier }) => {
            let qualifier = qualifier.to_owned().map(|x| x.try_into()).transpose()?;
            #[expect(deprecated)]
            Ok(Expr::Wildcard {
                qualifier,
                options: Box::new(WildcardOptions::default()),
            })
        }
        ExprType::ScalarUdfExpr(protobuf::ScalarUdfExprNode {
            fun_name,
            args,
            fun_definition,
        }) => {
            let scalar_fn = match fun_definition {
                Some(buf) => codec.try_decode_udf(fun_name, buf)?,
                None => registry
                    .udf(fun_name.as_str())
                    .or_else(|_| codec.try_decode_udf(fun_name, &[]))?,
            };
            Ok(Expr::ScalarFunction(expr::ScalarFunction::new_udf(
                scalar_fn,
                parse_exprs(args, registry, codec)?,
            )))
        }
        ExprType::AggregateUdfExpr(pb) => {
            let agg_fn = match &pb.fun_definition {
                Some(buf) => codec.try_decode_udaf(&pb.fun_name, buf)?,
                None => registry
                    .udaf(&pb.fun_name)
                    .or_else(|_| codec.try_decode_udaf(&pb.fun_name, &[]))?,
            };
            let null_treatment = match pb.null_treatment {
                Some(null_treatment) => {
                    let null_treatment  =  protobuf::NullTreatment::try_from(null_treatment)
                    .map_err(|_| {
                        proto_error(format!(
                            "Received an AggregateUdfExprNode message with unknown NullTreatment {null_treatment}",
                        ))
                    })?;
                    Some(NullTreatment::from(null_treatment))
                }
                None => None,
            };

            Ok(Expr::AggregateFunction(expr::AggregateFunction::new_udf(
                agg_fn,
                parse_exprs(&pb.args, registry, codec)?,
                pb.distinct,
                parse_optional_expr(pb.filter.as_deref(), registry, codec)?.map(Box::new),
                parse_sorts(&pb.order_by, registry, codec)?,
                null_treatment,
            )))
        }

        ExprType::GroupingSet(GroupingSetNode { expr }) => {
            Ok(Expr::GroupingSet(GroupingSets(
                expr.iter()
                    .map(|expr_list| parse_exprs(&expr_list.expr, registry, codec))
                    .collect::<Result<Vec<_>, Error>>()?,
            )))
        }
        ExprType::Cube(CubeNode { expr }) => Ok(Expr::GroupingSet(GroupingSet::Cube(
            parse_exprs(expr, registry, codec)?,
        ))),
        ExprType::Rollup(RollupNode { expr }) => Ok(Expr::GroupingSet(
            GroupingSet::Rollup(parse_exprs(expr, registry, codec)?),
        )),
        ExprType::Placeholder(PlaceholderNode {
            id,
            data_type,
            nullable,
            metadata,
        }) => match data_type {
            None => Ok(Expr::Placeholder(Placeholder::new_with_field(
                id.clone(),
                None,
            ))),
            Some(data_type) => {
                let field =
                    Field::new("", data_type.try_into()?, nullable.unwrap_or(true))
                        .with_metadata(metadata.clone());
                Ok(Expr::Placeholder(Placeholder::new_with_field(
                    id.clone(),
                    Some(field.into()),
                )))
            }
        },
    }
}

/// Parse a vector of `protobuf::LogicalExprNode`s.
pub fn parse_exprs<'a, I>(
    protos: I,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Vec<Expr>, Error>
where
    I: IntoIterator<Item = &'a protobuf::LogicalExprNode>,
{
    let res = protos
        .into_iter()
        .map(|elem| {
            parse_expr(elem, registry, codec).map_err(|e| plan_datafusion_err!("{}", e))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(res)
}

pub fn parse_sorts<'a, I>(
    protos: I,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Vec<Sort>, Error>
where
    I: IntoIterator<Item = &'a protobuf::SortExprNode>,
{
    protos
        .into_iter()
        .map(|sort| parse_sort(sort, registry, codec))
        .collect::<Result<Vec<Sort>, Error>>()
}

pub fn parse_sort(
    sort: &protobuf::SortExprNode,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Sort, Error> {
    Ok(Sort::new(
        parse_required_expr(sort.expr.as_ref(), registry, "expr", codec)?,
        sort.asc,
        sort.nulls_first,
    ))
}

/// Parse an optional escape_char for Like, ILike, SimilarTo
fn parse_escape_char(s: &str) -> Result<Option<char>> {
    match s.len() {
        0 => Ok(None),
        1 => Ok(s.chars().next()),
        _ => internal_err!("Invalid length for escape char"),
    }
}

pub fn from_proto_binary_op(op: &str) -> Result<Operator, Error> {
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
        "IsDistinctFrom" => Ok(Operator::IsDistinctFrom),
        "IsNotDistinctFrom" => Ok(Operator::IsNotDistinctFrom),
        "BitwiseAnd" => Ok(Operator::BitwiseAnd),
        "BitwiseOr" => Ok(Operator::BitwiseOr),
        "BitwiseXor" => Ok(Operator::BitwiseXor),
        "BitwiseShiftLeft" => Ok(Operator::BitwiseShiftLeft),
        "BitwiseShiftRight" => Ok(Operator::BitwiseShiftRight),
        "RegexIMatch" => Ok(Operator::RegexIMatch),
        "RegexMatch" => Ok(Operator::RegexMatch),
        "RegexNotIMatch" => Ok(Operator::RegexNotIMatch),
        "RegexNotMatch" => Ok(Operator::RegexNotMatch),
        "LikeMatch" => Ok(Operator::LikeMatch),
        "ILikeMatch" => Ok(Operator::ILikeMatch),
        "NotLikeMatch" => Ok(Operator::NotLikeMatch),
        "NotILikeMatch" => Ok(Operator::NotILikeMatch),
        "StringConcat" => Ok(Operator::StringConcat),
        "AtArrow" => Ok(Operator::AtArrow),
        "ArrowAt" => Ok(Operator::ArrowAt),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{other:?}'"
        ))),
    }
}

fn parse_optional_expr(
    p: Option<&protobuf::LogicalExprNode>,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Option<Expr>, Error> {
    match p {
        Some(expr) => parse_expr(expr, registry, codec).map(Some),
        None => Ok(None),
    }
}

fn parse_required_expr(
    p: Option<&protobuf::LogicalExprNode>,
    registry: &dyn FunctionRegistry,
    field: impl Into<String>,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Expr, Error> {
    match p {
        Some(expr) => parse_expr(expr, registry, codec),
        None => Err(Error::required(field)),
    }
}

fn proto_error<S: Into<String>>(message: S) -> Error {
    Error::General(message.into())
}
