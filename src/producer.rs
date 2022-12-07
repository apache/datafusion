use std::collections::HashMap;

use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{DFSchemaRef, Expr, JoinConstraint, LogicalPlan, Operator},
    prelude::JoinType,
    scalar::ScalarValue,
};

use substrait::protobuf::{
    aggregate_function::AggregationInvocation,
    aggregate_rel::{Grouping, Measure},
    expression::{
        field_reference::ReferenceType,
        if_then::IfClause,
        literal::LiteralType,
        mask_expression::{StructItem, StructSelect},
        reference_segment,
        FieldReference, IfThen, Literal, MaskExpression, ReferenceSegment, RexType, ScalarFunction,
    },
    extensions::{self, simple_extension_declaration::{MappingType, ExtensionFunction}},
    function_argument::ArgType,
    plan_rel,
    read_rel::{NamedTable, ReadType},
    rel::RelType,
    sort_field::{
        SortDirection,
        SortKind,
    },
    AggregateRel, Expression, FetchRel, FilterRel, FunctionArgument, JoinRel, NamedStruct, ProjectRel, ReadRel, SortField, SortRel,
    PlanRel,
    Plan, Rel, RelRoot, AggregateFunction,
};

/// Convert DataFusion LogicalPlan to Substrait Plan
pub fn to_substrait_plan(plan: &LogicalPlan) -> Result<Box<Plan>> {
    // Parse relation nodes
    let mut extension_info: (Vec<extensions::SimpleExtensionDeclaration>, HashMap<String, u32>) = (vec![], HashMap::new());
    // Generate PlanRel(s)
    // Note: Only 1 relation tree is currently supported
    let plan_rels = vec![PlanRel {
        rel_type: Some(plan_rel::RelType::Root(
            RelRoot {
                input: Some(*to_substrait_rel(plan, &mut extension_info)?),
                names: plan.schema().field_names(),
            }
        ))
    }];

    let (function_extensions, _) = extension_info;

    // Return parsed plan
    Ok(Box::new(Plan {
        extension_uris: vec![],
        extensions: function_extensions,
        relations: plan_rels,
        advanced_extensions: None,
        expected_type_urls: vec![],
    }))

}

/// Convert DataFusion LogicalPlan to Substrait Rel
pub fn to_substrait_rel(plan: &LogicalPlan, extension_info: &mut (Vec<extensions::SimpleExtensionDeclaration>, HashMap<String, u32>)) -> Result<Box<Rel>> {
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
                                .projected_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().to_owned())
                                .collect(),
                            r#struct: None,
                        }),
                        filter: None,
                        projection: Some(MaskExpression {
                            select: Some(StructSelect { struct_items }),
                            maintain_singular_struct: false,
                        }),
                        advanced_extension: None,
                        read_type: Some(ReadType::NamedTable(NamedTable {
                            names: vec![scan.table_name.clone()],
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
            let filter_expr = to_substrait_rex(&filter.predicate, filter.input.schema(), extension_info)?;
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
            let limit_fetch = match limit.fetch {
                Some(count) => count,
                None => 0,
            };
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
                    groupings: vec![Grouping { grouping_expressions: grouping }], //groupings, 
                    measures: measures,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Distinct(distinct) => {
            // Use Substrait's AggregateRel with empty measures to represent `select distinct`
            let input = to_substrait_rel(distinct.input.as_ref(), extension_info)?;
            // Get grouping keys from the input relation's number of output fields
            let grouping = (0..distinct.input.schema().fields().len())
                .map(|x: usize| substrait_field_ref(x))
                .collect::<Result<Vec<_>>>()?;

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                    common: None,
                    input: Some(input),
                    groupings: vec![Grouping { grouping_expressions: grouping }],
                    measures: vec![],
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Join(join) => {
            let left = to_substrait_rel(join.left.as_ref(), extension_info)?;
            let right = to_substrait_rel(join.right.as_ref(), extension_info)?;
            let join_type = match join.join_type {
                JoinType::Inner => 1,
                JoinType::Left => 2,
                JoinType::Right => 3,
                JoinType::Full => 4,
                JoinType::Anti => 5,
                JoinType::Semi => 6,
            };
            // we only support basic joins so return an error for anything not yet supported
            if join.null_equals_null {
                return Err(DataFusionError::NotImplemented(
                    "join null_equals_null".to_string(),
                ));
            }
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
            let join_expression: Vec<Expr> = join
                .on
                .iter()
                .map(|(l, r)| Expr::Column(l.clone()).eq(Expr::Column(r.clone())))
                .collect();
            // build a single expression for the ON condition, such as `l.a = r.a AND l.b = r.b`
            let join_expression = join_expression
                .into_iter()
                .reduce(|acc: Expr, expr: Expr| acc.and(expr));
            if let Some(e) = join_expression {
                Ok(Box::new(Rel {
                    rel_type: Some(RelType::Join(Box::new(JoinRel {
                        common: None,
                        left: Some(left),
                        right: Some(right),
                        r#type: join_type,
                        expression: Some(Box::new(to_substrait_rex(&e, &join.schema, extension_info)?)),
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
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported operator: {:?}",
            plan
        ))),
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
        Operator::Like => "like",
        Operator::NotLike => "not_like",
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

pub fn to_substrait_agg_measure(expr: &Expr, schema: &DFSchemaRef, extension_info: &mut (Vec<extensions::SimpleExtensionDeclaration>, HashMap<String, u32>)) -> Result<Measure> {
    match expr {
        Expr::AggregateFunction { fun, args, distinct, filter } => {
            let mut arguments: Vec<FunctionArgument> = vec![];
            for arg in args {
                arguments.push(FunctionArgument { arg_type: Some(ArgType::Value(to_substrait_rex(arg, schema, extension_info)?)) });
            }
            let function_name = fun.to_string().to_lowercase();
            let function_anchor = _register_function(function_name, extension_info);
            Ok(Measure {
                measure: Some(AggregateFunction {
                    function_reference: function_anchor,
                    arguments: arguments,
                    sorts: vec![],
                    output_type: None,
                    invocation: match distinct {
                        true => AggregationInvocation::Distinct as i32,
                        false => AggregationInvocation::All as i32,
                    },
                    phase: substrait::protobuf::AggregationPhase::Unspecified as i32,
                    args: vec![],
                }),
                filter: match filter {
                    Some(f) => Some(to_substrait_rex(f, schema, extension_info)?),
                    None => None
                }
            })
        },
        _ => Err(DataFusionError::Internal(format!(
            "Expression must be compatible with aggregation. Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn _register_function(function_name: String, extension_info: &mut (Vec<extensions::SimpleExtensionDeclaration>, HashMap<String, u32>)) -> u32 {
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
        },
        None => {
            // Function has NOT been registered
            let function_anchor = function_set.len() as u32;
            function_set.insert(function_name.clone(), function_anchor);

            let function_extension = ExtensionFunction {
                extension_uri_reference: u32::MAX,
                function_anchor: function_anchor,
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
pub fn make_binary_op_scalar_func(lhs: &Expression, rhs: &Expression, op: Operator, extension_info: &mut (Vec<extensions::SimpleExtensionDeclaration>, HashMap<String, u32>)) -> Expression {
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
        })),
    }
}

/// Convert DataFusion Expr to Substrait Rex
pub fn to_substrait_rex(expr: &Expr, schema: &DFSchemaRef, extension_info: &mut (Vec<extensions::SimpleExtensionDeclaration>, HashMap<String, u32>)) -> Result<Expression> {
    match expr {
        Expr::Between { expr, negated, low, high } => {
            if *negated {
                // `expr NOT BETWEEN low AND high` can be translated into (expr < low OR high < expr)
                let substrait_expr = to_substrait_rex(expr, schema, extension_info)?;
                let substrait_low = to_substrait_rex(low, schema, extension_info)?;
                let substrait_high = to_substrait_rex(high, schema, extension_info)?;

                let l_expr = make_binary_op_scalar_func(&substrait_expr, &substrait_low, Operator::Lt, extension_info);
                let r_expr = make_binary_op_scalar_func(&substrait_high, &substrait_expr, Operator::Lt, extension_info);

                Ok(make_binary_op_scalar_func(&l_expr, &r_expr, Operator::Or, extension_info))
            } else {
                // `expr BETWEEN low AND high` can be translated into (low <= expr AND expr <= high)
                let substrait_expr = to_substrait_rex(expr, schema, extension_info)?;
                let substrait_low = to_substrait_rex(low, schema, extension_info)?;
                let substrait_high = to_substrait_rex(high, schema, extension_info)?;

                let l_expr = make_binary_op_scalar_func(&substrait_low, &substrait_expr, Operator::LtEq, extension_info);
                let r_expr = make_binary_op_scalar_func(&substrait_expr, &substrait_high, Operator::LtEq, extension_info);

                Ok(make_binary_op_scalar_func(&l_expr, &r_expr, Operator::And, extension_info))
            }
        }
        Expr::Column(col) => {
            let index = schema.index_of_column(&col)?;
            substrait_field_ref(index)
        }
        Expr::BinaryExpr { left, op, right } => {
            let l = to_substrait_rex(left, schema, extension_info)?;
            let r = to_substrait_rex(right, schema, extension_info)?;

            Ok(make_binary_op_scalar_func(&l, &r, *op, extension_info))
        }
        Expr::Case { expr, when_then_expr, else_expr } => {
            let mut ifs: Vec<IfClause> = vec![];
            // Parse base
            if let Some(e) = expr { // Base expression exists
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
                rex_type: Some(RexType::IfThen(Box::new(IfThen {
                    ifs: ifs,
                    r#else: r#else
                }))),
            })
        }
        Expr::Literal(value) => {
            let literal_type = match value {
                ScalarValue::Int8(Some(n)) => Some(LiteralType::I8(*n as i32)),
                ScalarValue::Int16(Some(n)) => Some(LiteralType::I16(*n as i32)),
                ScalarValue::Int32(Some(n)) => Some(LiteralType::I32(*n)),
                ScalarValue::Int64(Some(n)) => Some(LiteralType::I64(*n)),
                ScalarValue::Boolean(Some(b)) => Some(LiteralType::Boolean(*b)),
                ScalarValue::Float32(Some(f)) => Some(LiteralType::Fp32(*f)),
                ScalarValue::Float64(Some(f)) => Some(LiteralType::Fp64(*f)),
                ScalarValue::Utf8(Some(s)) => Some(LiteralType::String(s.clone())),
                ScalarValue::LargeUtf8(Some(s)) => Some(LiteralType::String(s.clone())),
                ScalarValue::Binary(Some(b)) => Some(LiteralType::Binary(b.clone())),
                ScalarValue::LargeBinary(Some(b)) => Some(LiteralType::Binary(b.clone())),
                ScalarValue::Date32(Some(d)) => Some(LiteralType::Date(*d)),
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported literal: {:?}",
                        value
                    )))
                }
            };
            Ok(Expression {
                rex_type: Some(RexType::Literal(Literal {
                    nullable: true,
                    type_variation_reference: 0,
                    literal_type,
                })),
            })
        }
        Expr::Alias(expr, _alias) => {
            to_substrait_rex(expr, schema, extension_info)
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn substrait_sort_field(expr: &Expr, schema: &DFSchemaRef, extension_info: &mut (Vec<extensions::SimpleExtensionDeclaration>, HashMap<String, u32>)) -> Result<SortField> {
    match expr {
        Expr::Sort { expr, asc, nulls_first } => {
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
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "Expecting sort expression but got {:?}",
            expr
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
