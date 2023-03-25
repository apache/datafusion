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

use std::{collections::HashMap, mem, sync::Arc};

use datafusion::{
    arrow::datatypes::DataType,
    error::{DataFusionError, Result},
    logical_expr::{WindowFrame, WindowFrameBound},
    prelude::JoinType,
    scalar::ScalarValue,
};

use datafusion::common::DFSchemaRef;
#[allow(unused_imports)]
use datafusion::logical_expr::aggregate_function;
use datafusion::logical_expr::expr::{BinaryExpr, Case, Cast, Sort, WindowFunction};
use datafusion::logical_expr::{expr, Between, JoinConstraint, LogicalPlan, Operator};
use datafusion::prelude::{binary_expr, Expr};
use substrait::proto::{
    aggregate_function::AggregationInvocation,
    aggregate_rel::{Grouping, Measure},
    expression::{
        field_reference::ReferenceType,
        if_then::IfClause,
        literal::{Decimal, LiteralType},
        mask_expression::{StructItem, StructSelect},
        reference_segment,
        window_function::bound as SubstraitBound,
        window_function::bound::Kind as BoundKind,
        window_function::Bound,
        FieldReference, IfThen, Literal, MaskExpression, ReferenceSegment, RexType,
        ScalarFunction, WindowFunction as SubstraitWindowFunction,
    },
    extensions::{
        self,
        simple_extension_declaration::{ExtensionFunction, MappingType},
    },
    function_argument::ArgType,
    join_rel, plan_rel, r#type,
    read_rel::{NamedTable, ReadType},
    rel::RelType,
    sort_field::{SortDirection, SortKind},
    AggregateFunction, AggregateRel, AggregationPhase, Expression, FetchRel, FilterRel,
    FunctionArgument, JoinRel, NamedStruct, Plan, PlanRel, ProjectRel, ReadRel, Rel,
    RelRoot, SortField, SortRel,
};

/// Convert DataFusion LogicalPlan to Substrait Plan
pub fn to_substrait_plan(plan: &LogicalPlan) -> Result<Box<Plan>> {
    // Parse relation nodes
    let mut extension_info: (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ) = (vec![], HashMap::new());
    // Generate PlanRel(s)
    // Note: Only 1 relation tree is currently supported
    let plan_rels = vec![PlanRel {
        rel_type: Some(plan_rel::RelType::Root(RelRoot {
            input: Some(*to_substrait_rel(plan, &mut extension_info)?),
            names: plan.schema().field_names(),
        })),
    }];

    let (function_extensions, _) = extension_info;

    // Return parsed plan
    Ok(Box::new(Plan {
        version: None, // TODO: https://github.com/apache/arrow-datafusion/issues/4949
        extension_uris: vec![],
        extensions: function_extensions,
        relations: plan_rels,
        advanced_extensions: None,
        expected_type_urls: vec![],
    }))
}

/// Convert DataFusion LogicalPlan to Substrait Rel
pub fn to_substrait_rel(
    plan: &LogicalPlan,
    extension_info: &mut (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ),
) -> Result<Box<Rel>> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let projection = scan.projection.as_ref().map(|p| {
                p.iter()
                    .map(|i| StructItem {
                        field: *i as i32,
                        child: None,
                    })
                    .collect()
            });

            if let Some(struct_items) = projection {
                Ok(Box::new(Rel {
                    rel_type: Some(RelType::Read(Box::new(ReadRel {
                        common: None,
                        base_schema: Some(NamedStruct {
                            names: scan
                                .source
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| f.name().to_owned())
                                .collect(),
                            r#struct: None,
                        }),
                        filter: None,
                        best_effort_filter: None,
                        projection: Some(MaskExpression {
                            select: Some(StructSelect { struct_items }),
                            maintain_singular_struct: false,
                        }),
                        advanced_extension: None,
                        read_type: Some(ReadType::NamedTable(NamedTable {
                            names: scan.table_name.to_vec(),
                            advanced_extension: None,
                        })),
                    }))),
                }))
            } else {
                Err(DataFusionError::NotImplemented(
                    "TableScan without projection is not supported".to_string(),
                ))
            }
        }
        LogicalPlan::Projection(p) => {
            let expressions = p
                .expr
                .iter()
                .map(|e| to_substrait_rex(e, p.input.schema(), extension_info))
                .collect::<Result<Vec<_>>>()?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Project(Box::new(ProjectRel {
                    common: None,
                    input: Some(to_substrait_rel(p.input.as_ref(), extension_info)?),
                    expressions,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Filter(filter) => {
            let input = to_substrait_rel(filter.input.as_ref(), extension_info)?;
            let filter_expr = to_substrait_rex(
                &filter.predicate,
                filter.input.schema(),
                extension_info,
            )?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Filter(Box::new(FilterRel {
                    common: None,
                    input: Some(input),
                    condition: Some(Box::new(filter_expr)),
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Limit(limit) => {
            let input = to_substrait_rel(limit.input.as_ref(), extension_info)?;
            let limit_fetch = limit.fetch.unwrap_or(0);
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Fetch(Box::new(FetchRel {
                    common: None,
                    input: Some(input),
                    offset: limit.skip as i64,
                    count: limit_fetch as i64,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Sort(sort) => {
            let input = to_substrait_rel(sort.input.as_ref(), extension_info)?;
            let sort_fields = sort
                .expr
                .iter()
                .map(|e| substrait_sort_field(e, sort.input.schema(), extension_info))
                .collect::<Result<Vec<_>>>()?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Sort(Box::new(SortRel {
                    common: None,
                    input: Some(input),
                    sorts: sort_fields,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Aggregate(agg) => {
            let input = to_substrait_rel(agg.input.as_ref(), extension_info)?;
            // Translate aggregate expression to Substrait's groupings (repeated repeated Expression)
            let grouping = agg
                .group_expr
                .iter()
                .map(|e| to_substrait_rex(e, agg.input.schema(), extension_info))
                .collect::<Result<Vec<_>>>()?;
            let measures = agg
                .aggr_expr
                .iter()
                .map(|e| to_substrait_agg_measure(e, agg.input.schema(), extension_info))
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                    common: None,
                    input: Some(input),
                    groupings: vec![Grouping {
                        grouping_expressions: grouping,
                    }], //groupings,
                    measures,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Distinct(distinct) => {
            // Use Substrait's AggregateRel with empty measures to represent `select distinct`
            let input = to_substrait_rel(distinct.input.as_ref(), extension_info)?;
            // Get grouping keys from the input relation's number of output fields
            let grouping = (0..distinct.input.schema().fields().len())
                .map(substrait_field_ref)
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                    common: None,
                    input: Some(input),
                    groupings: vec![Grouping {
                        grouping_expressions: grouping,
                    }],
                    measures: vec![],
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Join(join) => {
            let left = to_substrait_rel(join.left.as_ref(), extension_info)?;
            let right = to_substrait_rel(join.right.as_ref(), extension_info)?;
            let join_type = to_substrait_jointype(join.join_type);
            // we only support basic joins so return an error for anything not yet supported
            if join.filter.is_some() {
                return Err(DataFusionError::NotImplemented("join filter".to_string()));
            }
            match join.join_constraint {
                JoinConstraint::On => {}
                _ => {
                    return Err(DataFusionError::NotImplemented(
                        "join constraint".to_string(),
                    ))
                }
            }
            // map the left and right columns to binary expressions in the form `l = r`
            // build a single expression for the ON condition, such as `l.a = r.a AND l.b = r.b`
            let eq_op = if join.null_equals_null {
                Operator::IsNotDistinctFrom
            } else {
                Operator::Eq
            };
            let join_expression = join
                .on
                .iter()
                .map(|(l, r)| binary_expr(l.clone(), eq_op, r.clone()))
                .reduce(|acc: Expr, expr: Expr| acc.and(expr));
            // join schema from left and right to maintain all nececesary columns from inputs
            // note that we cannot simple use join.schema here since we discard some input columns
            // when performing semi and anti joins
            let join_schema = join.left.schema().join(join.right.schema());
            if let Some(e) = join_expression {
                Ok(Box::new(Rel {
                    rel_type: Some(RelType::Join(Box::new(JoinRel {
                        common: None,
                        left: Some(left),
                        right: Some(right),
                        r#type: join_type as i32,
                        expression: Some(Box::new(to_substrait_rex(
                            &e,
                            &Arc::new(join_schema?),
                            extension_info,
                        )?)),
                        post_join_filter: None,
                        advanced_extension: None,
                    }))),
                }))
            } else {
                Err(DataFusionError::NotImplemented(
                    "Empty join condition".to_string(),
                ))
            }
        }
        LogicalPlan::SubqueryAlias(alias) => {
            // Do nothing if encounters SubqueryAlias
            // since there is no corresponding relation type in Substrait
            to_substrait_rel(alias.input.as_ref(), extension_info)
        }
        LogicalPlan::Window(window) => {
            let input = to_substrait_rel(window.input.as_ref(), extension_info)?;
            // If the input is a Project relation, we can just append the WindowFunction expressions
            // before returning
            // Otherwise, wrap the input in a Project relation before appending the WindowFunction
            // expressions
            let mut project_rel: Box<ProjectRel> = match &input.as_ref().rel_type {
                Some(RelType::Project(p)) => Box::new(*p.clone()),
                _ => {
                    // Create Projection with field referencing all output fields in the input relation
                    let expressions = (0..window.input.schema().fields().len())
                        .map(substrait_field_ref)
                        .collect::<Result<Vec<_>>>()?;
                    Box::new(ProjectRel {
                        common: None,
                        input: Some(input),
                        expressions,
                        advanced_extension: None,
                    })
                }
            };
            // Parse WindowFunction expression
            let mut window_exprs = vec![];
            for expr in &window.window_expr {
                window_exprs.push(to_substrait_rex(
                    expr,
                    window.input.schema(),
                    extension_info,
                )?);
            }
            // Append parsed WindowFunction expressions
            project_rel.expressions.extend(window_exprs);
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Project(project_rel)),
            }))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported operator: {plan:?}"
        ))),
    }
}

fn to_substrait_jointype(join_type: JoinType) -> join_rel::JoinType {
    match join_type {
        JoinType::Inner => join_rel::JoinType::Inner,
        JoinType::Left => join_rel::JoinType::Left,
        JoinType::Right => join_rel::JoinType::Right,
        JoinType::Full => join_rel::JoinType::Outer,
        JoinType::LeftAnti => join_rel::JoinType::Anti,
        JoinType::LeftSemi => join_rel::JoinType::Semi,
        JoinType::RightAnti | JoinType::RightSemi => unimplemented!(),
    }
}

pub fn operator_to_name(op: Operator) -> &'static str {
    match op {
        Operator::Eq => "equal",
        Operator::NotEq => "not_equal",
        Operator::Lt => "lt",
        Operator::LtEq => "lte",
        Operator::Gt => "gt",
        Operator::GtEq => "gte",
        Operator::Plus => "add",
        Operator::Minus => "substract",
        Operator::Multiply => "multiply",
        Operator::Divide => "divide",
        Operator::Modulo => "mod",
        Operator::And => "and",
        Operator::Or => "or",
        Operator::IsDistinctFrom => "is_distinct_from",
        Operator::IsNotDistinctFrom => "is_not_distinct_from",
        Operator::RegexMatch => "regex_match",
        Operator::RegexIMatch => "regex_imatch",
        Operator::RegexNotMatch => "regex_not_match",
        Operator::RegexNotIMatch => "regex_not_imatch",
        Operator::BitwiseAnd => "bitwise_and",
        Operator::BitwiseOr => "bitwise_or",
        Operator::StringConcat => "str_concat",
        Operator::BitwiseXor => "bitwise_xor",
        Operator::BitwiseShiftRight => "bitwise_shift_right",
        Operator::BitwiseShiftLeft => "bitwise_shift_left",
    }
}

#[allow(deprecated)]
pub fn to_substrait_agg_measure(
    expr: &Expr,
    schema: &DFSchemaRef,
    extension_info: &mut (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ),
) -> Result<Measure> {
    match expr {
        Expr::AggregateFunction(expr::AggregateFunction { fun, args, distinct, filter }) => {
            let mut arguments: Vec<FunctionArgument> = vec![];
            for arg in args {
                arguments.push(FunctionArgument { arg_type: Some(ArgType::Value(to_substrait_rex(arg, schema, extension_info)?)) });
            }
            let function_name = fun.to_string().to_lowercase();
            let function_anchor = _register_function(function_name, extension_info);
            Ok(Measure {
                measure: Some(AggregateFunction {
                    function_reference: function_anchor,
                    arguments,
                    sorts: vec![],
                    output_type: None,
                    invocation: match distinct {
                        true => AggregationInvocation::Distinct as i32,
                        false => AggregationInvocation::All as i32,
                    },
                    phase: AggregationPhase::Unspecified as i32,
                    args: vec![],
                    options: vec![],
                }),
                filter: match filter {
                    Some(f) => Some(to_substrait_rex(f, schema, extension_info)?),
                    None => None
                }
            })
        }
        Expr::Alias(expr, _name) => {
            to_substrait_agg_measure(expr, schema, extension_info)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Expression must be compatible with aggregation. Unsupported expression: {:?}. ExpressionType: {:?}",
            expr,
            expr.variant_name()
        ))),
    }
}

fn _register_function(
    function_name: String,
    extension_info: &mut (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ),
) -> u32 {
    let (function_extensions, function_set) = extension_info;
    let function_name = function_name.to_lowercase();
    // To prevent ambiguous references between ScalarFunctions and AggregateFunctions,
    // a plan-relative identifier starting from 0 is used as the function_anchor.
    // The consumer is responsible for correctly registering <function_anchor,function_name>
    // mapping info stored in the extensions by the producer.
    let function_anchor = match function_set.get(&function_name) {
        Some(function_anchor) => {
            // Function has been registered
            *function_anchor
        }
        None => {
            // Function has NOT been registered
            let function_anchor = function_set.len() as u32;
            function_set.insert(function_name.clone(), function_anchor);

            let function_extension = ExtensionFunction {
                extension_uri_reference: u32::MAX,
                function_anchor,
                name: function_name,
            };
            let simple_extension = extensions::SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionFunction(function_extension)),
            };
            function_extensions.push(simple_extension);
            function_anchor
        }
    };

    // Return function anchor
    function_anchor
}

/// Return Substrait scalar function with two arguments
#[allow(deprecated)]
pub fn make_binary_op_scalar_func(
    lhs: &Expression,
    rhs: &Expression,
    op: Operator,
    extension_info: &mut (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ),
) -> Expression {
    let function_name = operator_to_name(op).to_string().to_lowercase();
    let function_anchor = _register_function(function_name, extension_info);
    Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![
                FunctionArgument {
                    arg_type: Some(ArgType::Value(lhs.clone())),
                },
                FunctionArgument {
                    arg_type: Some(ArgType::Value(rhs.clone())),
                },
            ],
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    }
}

/// Convert DataFusion Expr to Substrait Rex
pub fn to_substrait_rex(
    expr: &Expr,
    schema: &DFSchemaRef,
    extension_info: &mut (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ),
) -> Result<Expression> {
    match expr {
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            if *negated {
                // `expr NOT BETWEEN low AND high` can be translated into (expr < low OR high < expr)
                let substrait_expr = to_substrait_rex(expr, schema, extension_info)?;
                let substrait_low = to_substrait_rex(low, schema, extension_info)?;
                let substrait_high = to_substrait_rex(high, schema, extension_info)?;

                let l_expr = make_binary_op_scalar_func(
                    &substrait_expr,
                    &substrait_low,
                    Operator::Lt,
                    extension_info,
                );
                let r_expr = make_binary_op_scalar_func(
                    &substrait_high,
                    &substrait_expr,
                    Operator::Lt,
                    extension_info,
                );

                Ok(make_binary_op_scalar_func(
                    &l_expr,
                    &r_expr,
                    Operator::Or,
                    extension_info,
                ))
            } else {
                // `expr BETWEEN low AND high` can be translated into (low <= expr AND expr <= high)
                let substrait_expr = to_substrait_rex(expr, schema, extension_info)?;
                let substrait_low = to_substrait_rex(low, schema, extension_info)?;
                let substrait_high = to_substrait_rex(high, schema, extension_info)?;

                let l_expr = make_binary_op_scalar_func(
                    &substrait_low,
                    &substrait_expr,
                    Operator::LtEq,
                    extension_info,
                );
                let r_expr = make_binary_op_scalar_func(
                    &substrait_expr,
                    &substrait_high,
                    Operator::LtEq,
                    extension_info,
                );

                Ok(make_binary_op_scalar_func(
                    &l_expr,
                    &r_expr,
                    Operator::And,
                    extension_info,
                ))
            }
        }
        Expr::Column(col) => {
            let index = schema.index_of_column(col)?;
            substrait_field_ref(index)
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let l = to_substrait_rex(left, schema, extension_info)?;
            let r = to_substrait_rex(right, schema, extension_info)?;

            Ok(make_binary_op_scalar_func(&l, &r, *op, extension_info))
        }
        Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) => {
            let mut ifs: Vec<IfClause> = vec![];
            // Parse base
            if let Some(e) = expr {
                // Base expression exists
                ifs.push(IfClause {
                    r#if: Some(to_substrait_rex(e, schema, extension_info)?),
                    then: None,
                });
            }
            // Parse `when`s
            for (r#if, then) in when_then_expr {
                ifs.push(IfClause {
                    r#if: Some(to_substrait_rex(r#if, schema, extension_info)?),
                    then: Some(to_substrait_rex(then, schema, extension_info)?),
                });
            }

            // Parse outer `else`
            let r#else: Option<Box<Expression>> = match else_expr {
                Some(e) => Some(Box::new(to_substrait_rex(e, schema, extension_info)?)),
                None => None,
            };

            Ok(Expression {
                rex_type: Some(RexType::IfThen(Box::new(IfThen { ifs, r#else }))),
            })
        }
        Expr::Cast(Cast { expr, data_type }) => {
            Ok(Expression {
                rex_type: Some(RexType::Cast(Box::new(
                    substrait::proto::expression::Cast {
                        r#type: Some(to_substrait_type(data_type)?),
                        input: Some(Box::new(to_substrait_rex(
                            expr,
                            schema,
                            extension_info,
                        )?)),
                        failure_behavior: 0, // FAILURE_BEHAVIOR_UNSPECIFIED
                    },
                ))),
            })
        }
        Expr::Literal(value) => {
            let literal_type = match value {
                ScalarValue::Int8(Some(n)) => Some(LiteralType::I8(*n as i32)),
                ScalarValue::UInt8(Some(n)) => Some(LiteralType::I8(*n as i32)),
                ScalarValue::Int16(Some(n)) => Some(LiteralType::I16(*n as i32)),
                ScalarValue::UInt16(Some(n)) => Some(LiteralType::I16(*n as i32)),
                ScalarValue::Int32(Some(n)) => Some(LiteralType::I32(*n)),
                ScalarValue::UInt32(Some(n)) => Some(LiteralType::I32(unsafe {
                    mem::transmute_copy::<u32, i32>(n)
                })),
                ScalarValue::Int64(Some(n)) => Some(LiteralType::I64(*n)),
                ScalarValue::UInt64(Some(n)) => Some(LiteralType::I64(unsafe {
                    mem::transmute_copy::<u64, i64>(n)
                })),
                ScalarValue::Boolean(Some(b)) => Some(LiteralType::Boolean(*b)),
                ScalarValue::Float32(Some(f)) => Some(LiteralType::Fp32(*f)),
                ScalarValue::Float64(Some(f)) => Some(LiteralType::Fp64(*f)),
                ScalarValue::Decimal128(v, p, s) if v.is_some() => {
                    Some(LiteralType::Decimal(Decimal {
                        value: v.unwrap().to_le_bytes().to_vec(),
                        precision: *p as i32,
                        scale: *s as i32,
                    }))
                }
                ScalarValue::Utf8(Some(s)) => Some(LiteralType::String(s.clone())),
                ScalarValue::LargeUtf8(Some(s)) => Some(LiteralType::String(s.clone())),
                ScalarValue::Binary(Some(b)) => Some(LiteralType::Binary(b.clone())),
                ScalarValue::LargeBinary(Some(b)) => Some(LiteralType::Binary(b.clone())),
                ScalarValue::Date32(Some(d)) => Some(LiteralType::Date(*d)),
                _ => Some(try_to_substrait_null(value)?),
            };

            let type_variation_reference = if value.is_unsigned() { 1 } else { 0 };

            Ok(Expression {
                rex_type: Some(RexType::Literal(Literal {
                    nullable: true,
                    type_variation_reference,
                    literal_type,
                })),
            })
        }
        Expr::Alias(expr, _alias) => to_substrait_rex(expr, schema, extension_info),
        Expr::WindowFunction(WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        }) => {
            // function reference
            let function_name = fun.to_string().to_lowercase();
            let function_anchor = _register_function(function_name, extension_info);
            // arguments
            let mut arguments: Vec<FunctionArgument> = vec![];
            for arg in args {
                arguments.push(FunctionArgument {
                    arg_type: Some(ArgType::Value(to_substrait_rex(
                        arg,
                        schema,
                        extension_info,
                    )?)),
                });
            }
            // partition by expressions
            let partition_by = partition_by
                .iter()
                .map(|e| to_substrait_rex(e, schema, extension_info))
                .collect::<Result<Vec<_>>>()?;
            // order by expressions
            let order_by = order_by
                .iter()
                .map(|e| substrait_sort_field(e, schema, extension_info))
                .collect::<Result<Vec<_>>>()?;
            // window frame
            let bounds = to_substrait_bounds(window_frame)?;
            Ok(make_substrait_window_function(
                function_anchor,
                arguments,
                partition_by,
                order_by,
                bounds,
            ))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported expression: {expr:?}"
        ))),
    }
}

fn to_substrait_type(dt: &DataType) -> Result<substrait::proto::Type> {
    let default_type_ref = 0;
    let default_nullability = r#type::Nullability::Required as i32;
    match dt {
        DataType::Null => Err(DataFusionError::Internal(
            "Null cast is not valid".to_string(),
        )),
        DataType::Boolean => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Bool(r#type::Boolean {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        }),
        DataType::Int8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I8(r#type::I8 {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        }),
        DataType::Int16 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I16(r#type::I16 {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        }),
        DataType::Int32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I32(r#type::I32 {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        }),
        DataType::Int64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        }),
        DataType::Decimal128(p, s) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Decimal(r#type::Decimal {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
                scale: *s as i32,
                precision: *p as i32,
            })),
        }),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported cast type: {dt:?}"
        ))),
    }
}

#[allow(deprecated)]
fn make_substrait_window_function(
    function_reference: u32,
    arguments: Vec<FunctionArgument>,
    partitions: Vec<Expression>,
    sorts: Vec<SortField>,
    bounds: (Bound, Bound),
) -> Expression {
    Expression {
        rex_type: Some(RexType::WindowFunction(SubstraitWindowFunction {
            function_reference,
            arguments,
            partitions,
            sorts,
            options: vec![],
            output_type: None,
            phase: 0,      // default to AGGREGATION_PHASE_UNSPECIFIED
            invocation: 0, // TODO: fix
            lower_bound: Some(bounds.0),
            upper_bound: Some(bounds.1),
            args: vec![],
        })),
    }
}

fn to_substrait_bound(bound: &WindowFrameBound) -> Bound {
    match bound {
        WindowFrameBound::CurrentRow => Bound {
            kind: Some(BoundKind::CurrentRow(SubstraitBound::CurrentRow {})),
        },
        WindowFrameBound::Preceding(s) => match s {
            ScalarValue::UInt8(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt16(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt32(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt64(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int8(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int16(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int32(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int64(Some(v)) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                    offset: *v,
                })),
            },
            _ => Bound {
                kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
            },
        },
        WindowFrameBound::Following(s) => match s {
            ScalarValue::UInt8(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt16(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt32(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::UInt64(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int8(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int16(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int32(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v as i64,
                })),
            },
            ScalarValue::Int64(Some(v)) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following {
                    offset: *v,
                })),
            },
            _ => Bound {
                kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
            },
        },
    }
}

fn to_substrait_bounds(window_frame: &WindowFrame) -> Result<(Bound, Bound)> {
    Ok((
        to_substrait_bound(&window_frame.start_bound),
        to_substrait_bound(&window_frame.end_bound),
    ))
}

fn try_to_substrait_null(v: &ScalarValue) -> Result<LiteralType> {
    let default_type_ref = 0;
    let default_nullability = r#type::Nullability::Nullable as i32;
    match v {
        ScalarValue::Int8(None) => Ok(LiteralType::Null(substrait::proto::Type {
            kind: Some(r#type::Kind::I8(r#type::I8 {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        })),
        ScalarValue::Int16(None) => Ok(LiteralType::Null(substrait::proto::Type {
            kind: Some(r#type::Kind::I16(r#type::I16 {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        })),
        ScalarValue::Int32(None) => Ok(LiteralType::Null(substrait::proto::Type {
            kind: Some(r#type::Kind::I32(r#type::I32 {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        })),
        ScalarValue::Int64(None) => Ok(LiteralType::Null(substrait::proto::Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: default_type_ref,
                nullability: default_nullability,
            })),
        })),
        ScalarValue::Decimal128(None, p, s) => {
            Ok(LiteralType::Null(substrait::proto::Type {
                kind: Some(r#type::Kind::Decimal(r#type::Decimal {
                    scale: *s as i32,
                    precision: *p as i32,
                    type_variation_reference: default_type_ref,
                    nullability: default_nullability,
                })),
            }))
        }
        // TODO: Extend support for remaining data types
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported literal: {v:?}"
        ))),
    }
}

fn substrait_sort_field(
    expr: &Expr,
    schema: &DFSchemaRef,
    extension_info: &mut (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ),
) -> Result<SortField> {
    match expr {
        Expr::Sort(Sort {
            expr,
            asc,
            nulls_first,
        }) => {
            let e = to_substrait_rex(expr, schema, extension_info)?;
            let d = match (asc, nulls_first) {
                (true, true) => SortDirection::AscNullsFirst,
                (true, false) => SortDirection::AscNullsLast,
                (false, true) => SortDirection::DescNullsFirst,
                (false, false) => SortDirection::DescNullsLast,
            };
            Ok(SortField {
                expr: Some(e),
                sort_kind: Some(SortKind::Direction(d as i32)),
            })
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Expecting sort expression but got {expr:?}"
        ))),
    }
}

fn substrait_field_ref(index: usize) -> Result<Expression> {
    Ok(Expression {
        rex_type: Some(RexType::Selection(Box::new(FieldReference {
            reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                reference_type: Some(reference_segment::ReferenceType::StructField(
                    Box::new(reference_segment::StructField {
                        field: index as i32,
                        child: None,
                    }),
                )),
            })),
            root_type: None,
        }))),
    })
}
