use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{DFSchemaRef, Expr, JoinConstraint, LogicalPlan, Operator},
    prelude::JoinType,
    scalar::ScalarValue,
};
use substrait::protobuf::{
    expression::{
        field_reference::ReferenceType,
        literal::LiteralType,
        mask_expression::{StructItem, StructSelect},
        FieldReference, Literal, MaskExpression, RexType, ScalarFunction,
    },
    sort_field::{
        SortDirection,
        SortKind,
    },
    function_argument::ArgType,
    plan_rel,
    read_rel::{NamedTable, ReadType},
    rel::RelType,
    Expression, FetchRel, FilterRel, FunctionArgument, JoinRel, NamedStruct, ProjectRel, ReadRel, SortField, SortRel,
    PlanRel,
    Plan, Rel, extensions::{self, simple_extension_declaration::{MappingType, ExtensionFunction}},
};

/// Convert DataFusion LogicalPlan to Substrait Plan
pub fn to_substrait_plan(plan: &LogicalPlan) -> Result<Box<Plan>> {
    // Parse relation nodes
    let mut extensions: Vec<extensions::SimpleExtensionDeclaration> = vec![];
    // Generate PlanRel(s)
    // Note: Only 1 relation tree is currently supported
    let plan_rels = vec![PlanRel {
        rel_type: Some(plan_rel::RelType::Rel(*to_substrait_rel(plan, &mut extensions)?))
    }];

    // Return parsed plan
    Ok(Box::new(Plan {
        extension_uris: vec![],
        extensions: extensions,
        relations: plan_rels,
        advanced_extensions: None,
        expected_type_urls: vec![],
    }))

}

/// Convert DataFusion LogicalPlan to Substrait Rel
pub fn to_substrait_rel(plan: &LogicalPlan, extensions: &mut Vec<extensions::SimpleExtensionDeclaration>) -> Result<Box<Rel>> {
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
                .map(|e| to_substrait_rex(e, p.input.schema(), extensions))
                .collect::<Result<Vec<_>>>()?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Project(Box::new(ProjectRel {
                    common: None,
                    input: Some(to_substrait_rel(p.input.as_ref(), extensions)?),
                    expressions,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Filter(filter) => {
            let input = to_substrait_rel(filter.input.as_ref(), extensions)?;
            let filter_expr = to_substrait_rex(&filter.predicate, filter.input.schema(), extensions)?;
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
            let input = to_substrait_rel(limit.input.as_ref(), extensions)?;
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
            let input = to_substrait_rel(sort.input.as_ref(), extensions)?;
            let sort_fields = sort
                .expr
                .iter()
                .map(|e| substrait_sort_field(e, sort.input.schema(), extensions))
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
        LogicalPlan::Join(join) => {
            let left = to_substrait_rel(join.left.as_ref(), extensions)?;
            let right = to_substrait_rel(join.right.as_ref(), extensions)?;
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
                        expression: Some(Box::new(to_substrait_rex(&e, &join.schema, extensions)?)),
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
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported operator: {:?}",
            plan
        ))),
    }
}

pub fn operator_to_reference(op: Operator) -> (u32, &'static str) {
    match op {
        Operator::Eq => (1, "equal"),
        Operator::NotEq => (2, "not_equal"),
        Operator::Lt => (3, "lt"),
        Operator::LtEq => (4, "lte"),
        Operator::Gt => (5, "gt"),
        Operator::GtEq => (6, "gte"),
        Operator::Plus => (7, "add"),
        Operator::Minus => (8, "substract"),
        Operator::Multiply => (9, "multiply"),
        Operator::Divide => (10, "divide"),
        Operator::Modulo => (11, "mod"),
        Operator::And => (12, "and"),
        Operator::Or => (13, "or"),
        Operator::Like => (14, "like"),
        Operator::NotLike => (15, "not_like"),
        Operator::IsDistinctFrom => (16, "is_distinct_from"),
        Operator::IsNotDistinctFrom => (17, "is_not_distinct_from"),
        Operator::RegexMatch => (18, "regex_match"),
        Operator::RegexIMatch => (19, "regex_imatch"),
        Operator::RegexNotMatch => (20, "regex_not_match"),
        Operator::RegexNotIMatch => (21, "regex_not_imatch"),
        Operator::BitwiseAnd => (22, "bitwise_and"),
        Operator::BitwiseOr => (23, "bitwise_or"),
        Operator::StringConcat => (24, "str_concat"),
        Operator::BitwiseXor => (25, "bitwise_xor"),
        Operator::BitwiseShiftRight => (26, "bitwise_shift_right"),
        Operator::BitwiseShiftLeft => (27, "bitwise_shift_left"),
    }
}

/// Convert DataFusion Expr to Substrait Rex
pub fn to_substrait_rex(expr: &Expr, schema: &DFSchemaRef, extensions: &mut Vec<extensions::SimpleExtensionDeclaration>) -> Result<Expression> {
    match expr {
        Expr::Column(col) => {
            let index = schema.index_of_column(&col)?;
            substrait_field_ref(index)
        }
        Expr::BinaryExpr { left, op, right } => {
            let l = to_substrait_rex(left, schema, extensions)?;
            let r = to_substrait_rex(right, schema, extensions)?;
            let (function_reference, function_name) = operator_to_reference(*op);
            let extension_function = ExtensionFunction {
                extension_uri_reference: extensions.len() as u32,
                function_anchor: function_reference,
                name: function_name.to_string(),
            };
            let extension = extensions::SimpleExtensionDeclaration {
                mapping_type: Some(MappingType::ExtensionFunction(extension_function)),
            };
            extensions.push(extension);
            Ok(Expression {
                rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                    function_reference,
                    arguments: vec![
                        FunctionArgument {
                            arg_type: Some(ArgType::Value(l)),
                        },
                        FunctionArgument {
                            arg_type: Some(ArgType::Value(r)),
                        },
                    ],
                    output_type: None,
                    args: vec![],
                })),
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
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn substrait_sort_field(expr: &Expr, schema: &DFSchemaRef, extensions: &mut Vec<extensions::SimpleExtensionDeclaration>) -> Result<SortField> {
    match expr {
        Expr::Sort { expr, asc, nulls_first } => {
            let e = to_substrait_rex(expr, schema, extensions)?;
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
            reference_type: Some(ReferenceType::MaskedReference(MaskExpression {
                select: Some(StructSelect {
                    struct_items: vec![StructItem {
                        field: index as i32,
                        child: None,
                    }],
                }),
                maintain_singular_struct: false,
            })),
            root_type: None,
        }))),
    })
}
