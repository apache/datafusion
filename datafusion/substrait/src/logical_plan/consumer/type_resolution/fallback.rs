// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::arrow::array::{ArrayData, make_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{
    Aggregate, Case, Distinct, Expr, ExprSchemable, Limit, LogicalPlan, Projection, Sort,
    SubqueryAlias, Union,
};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

// Opaque functions and custom relations can supply names only after conversion.
// Use their actual types, retaining successful calls with independent arguments.
pub(super) fn resolve_expression(expr: &mut Expr, schema: &DFSchema) {
    let original = if let Expr::ScalarFunction(function) = expr {
        if !function.args.iter().any(is_struct_literal) || expr.get_type(schema).is_ok() {
            return;
        }
        Some(expr.clone())
    } else {
        None
    };
    match expr {
        Expr::ScalarFunction(function) => {
            name_struct_literals_like_siblings(&mut function.args, schema)
        }
        Expr::WindowFunction(function) => {
            name_struct_literals_like_siblings(&mut function.params.args, schema)
        }
        Expr::Case(case) => name_struct_literals_like_siblings(
            case.when_then_expr
                .iter_mut()
                .map(|(_, then)| then.as_mut())
                .chain(case.else_expr.iter_mut().map(Box::as_mut)),
            schema,
        ),
        Expr::BinaryExpr(binary) => name_struct_literals_like_siblings(
            [binary.left.as_mut(), binary.right.as_mut()],
            schema,
        ),
        Expr::Between(between) => name_struct_literals_like_siblings(
            [
                between.expr.as_mut(),
                between.low.as_mut(),
                between.high.as_mut(),
            ],
            schema,
        ),
        Expr::InList(list) => name_struct_literals_like_siblings(
            std::iter::once(list.expr.as_mut()).chain(list.list.iter_mut()),
            schema,
        ),
        _ => {}
    }
    if let Some(original) = original
        && expr.get_type(schema).is_err()
    {
        *expr = original;
    }
}

fn name_struct_literals_like_siblings<'a>(
    exprs: impl IntoIterator<Item = &'a mut Expr>,
    input_schema: &DFSchema,
) {
    let mut exprs = exprs.into_iter().collect::<Vec<_>>();
    let Some(named_type) = exprs
        .iter()
        .filter(|e| !is_literal(e) && !matches!(***e, Expr::Lambda(_)))
        .filter_map(|e| e.get_type(input_schema).ok())
        .find(contains_struct)
    else {
        return;
    };

    for expr in exprs.iter_mut() {
        name_struct_literal_like(expr, &named_type);
    }
}

fn name_struct_literal_like(expr: &mut Expr, named_type: &DataType) {
    match expr {
        Expr::Alias(alias) => name_struct_literal_like(&mut alias.expr, named_type),
        Expr::Literal(value, _) => {
            let value_type = value.data_type();
            if !contains_struct(&value_type) {
                return;
            }
            let Some(renamed_type) = with_field_names_of(&value_type, named_type) else {
                return;
            };
            if let Ok(renamed) = with_data_type(value, &renamed_type) {
                *value = renamed;
            }
        }
        Expr::Case(case) if case_results(case).all(is_literal) => {
            for (_, then) in case.when_then_expr.iter_mut() {
                name_struct_literal_like(then, named_type);
            }
            if let Some(else_expr) = case.else_expr.as_mut() {
                name_struct_literal_like(else_expr, named_type);
            }
        }
        _ => {}
    }
}

fn is_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => is_literal(&alias.expr),
        Expr::Literal(..) => true,
        Expr::Case(case) => case_results(case).all(is_literal),
        _ => false,
    }
}

fn is_struct_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => is_struct_literal(&alias.expr),
        Expr::Literal(value, _) => contains_struct(&value.data_type()),
        Expr::Case(case) => {
            case_results(case).all(is_literal)
                && case_results(case).any(is_struct_literal)
        }
        _ => false,
    }
}

fn case_results(case: &Case) -> impl Iterator<Item = &Expr> {
    case.when_then_expr
        .iter()
        .map(|(_, then)| then.as_ref())
        .chain(case.else_expr.as_deref())
}

fn contains_struct(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(_) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => contains_struct(f.data_type()),
        DataType::Dictionary(_, value) => contains_struct(value),
        DataType::RunEndEncoded(_, values) => contains_struct(values.data_type()),
        _ => false,
    }
}

fn with_field_names_of(data_type: &DataType, named: &DataType) -> Option<DataType> {
    let renamed_field = |field: &FieldRef, named: &FieldRef| -> Option<FieldRef> {
        let data_type = with_field_names_of(field.data_type(), named.data_type())?;
        Some(Arc::new(
            Field::new(named.name(), data_type, field.is_nullable())
                .with_metadata(named.metadata().clone()),
        ))
    };
    match (data_type, named) {
        (DataType::Struct(fields), DataType::Struct(named_fields)) => {
            if fields.len() != named_fields.len() {
                return None;
            }
            let fields = fields
                .iter()
                .zip(named_fields.iter())
                .map(|(field, named)| renamed_field(field, named))
                .collect::<Option<Fields>>()?;
            Some(DataType::Struct(fields))
        }
        (DataType::Dictionary(key, value), named) => {
            let named = match named {
                DataType::Dictionary(_, named_value) => named_value.as_ref(),
                named => named,
            };
            Some(DataType::Dictionary(
                key.clone(),
                Box::new(with_field_names_of(value, named)?),
            ))
        }
        (data_type, DataType::Dictionary(_, named_value)) => {
            with_field_names_of(data_type, named_value)
        }
        (data_type, DataType::RunEndEncoded(_, named_values)) => {
            with_field_names_of(data_type, named_values.data_type())
        }
        (DataType::List(field), named) => {
            Some(DataType::List(renamed_field(field, list_element(named)?)?))
        }
        (DataType::LargeList(field), named) => Some(DataType::LargeList(renamed_field(
            field,
            list_element(named)?,
        )?)),
        (DataType::FixedSizeList(field, size), named) => {
            if let DataType::FixedSizeList(_, named_size) = named
                && named_size != size
            {
                return None;
            }
            let field = renamed_field(field, list_element(named)?)?;
            Some(DataType::FixedSizeList(field, *size))
        }
        (DataType::Map(field, sorted), DataType::Map(named, _)) => {
            Some(DataType::Map(renamed_field(field, named)?, *sorted))
        }
        (data_type, named) if !data_type.is_nested() && !named.is_nested() => {
            Some(data_type.clone())
        }
        _ => None,
    }
}

fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

fn with_data_type(
    value: &ScalarValue,
    data_type: &DataType,
) -> datafusion::common::Result<ScalarValue> {
    // A cast can reorder matching field names; renaming must preserve positions.
    let data = with_array_data_type(value.to_array()?.to_data(), data_type)?;
    ScalarValue::try_from_array(&make_array(data), 0)
}

fn with_array_data_type(
    data: ArrayData,
    data_type: &DataType,
) -> datafusion::common::Result<ArrayData> {
    let children = match data_type {
        DataType::Struct(fields) => data
            .child_data()
            .iter()
            .zip(fields.iter())
            .map(|(child, field)| with_array_data_type(child.clone(), field.data_type()))
            .collect::<datafusion::common::Result<Vec<_>>>()?,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => vec![with_array_data_type(
            data.child_data()[0].clone(),
            field.data_type(),
        )?],
        DataType::Dictionary(_, value) => {
            vec![with_array_data_type(data.child_data()[0].clone(), value)?]
        }
        _ => data.child_data().to_vec(),
    };
    Ok(data
        .into_builder()
        .data_type(data_type.clone())
        .child_data(children)
        .build()?)
}

pub(in crate::logical_plan::consumer) fn resolve_set_inputs(
    inputs: Vec<LogicalPlan>,
) -> datafusion::common::Result<Vec<LogicalPlan>> {
    if inputs.iter().skip(1).all(|input| {
        input
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type())
            .eq(inputs[0]
                .schema()
                .fields()
                .iter()
                .map(|field| field.data_type()))
    }) {
        return Ok(inputs);
    }
    let width = inputs[0].schema().fields().len();
    if inputs
        .iter()
        .any(|input| input.schema().fields().len() != width)
    {
        return Ok(inputs);
    }

    let named_types = (0..width)
        .map(|column| {
            let named = inputs.iter().find_map(|input| {
                let ty = input.schema().field(column).data_type();
                (contains_struct(ty) && !projects_literal(input, column)).then_some(ty)
            })?;
            inputs
                .iter()
                .any(|input| {
                    let ty = input.schema().field(column).data_type();
                    with_field_names_of(ty, named).is_some_and(|renamed| renamed != *ty)
                })
                .then(|| named.clone())
        })
        .collect::<Vec<_>>();
    if named_types.iter().all(Option::is_none) {
        return Ok(inputs);
    }

    inputs
        .into_iter()
        .map(|input| name_projected_struct_literals(input, &named_types))
        .collect()
}

fn name_projected_struct_literals(
    input: LogicalPlan,
    named_types: &[Option<DataType>],
) -> datafusion::common::Result<LogicalPlan> {
    match input {
        LogicalPlan::Projection(projection) => {
            let exprs = projection
                .expr
                .into_iter()
                .zip(named_types)
                .map(|(mut expr, named_type)| {
                    let Some(named_type) = named_type else {
                        return Ok(expr);
                    };
                    let name = expr.schema_name().to_string();
                    name_struct_literal_like(&mut expr, named_type);
                    expr.alias_if_changed(name)
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(LogicalPlan::Projection(Projection::try_new(
                exprs,
                projection.input,
            )?))
        }
        LogicalPlan::Union(union) => {
            let inputs = union
                .inputs
                .into_iter()
                .map(|input| {
                    name_projected_struct_literals(
                        Arc::unwrap_or_clone(input),
                        named_types,
                    )
                    .map(Arc::new)
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(LogicalPlan::Union(Union::try_new_with_loose_types(inputs)?))
        }
        _ => {
            let Some(child) = columns_input(&input) else {
                return Ok(input);
            };
            let child = name_projected_struct_literals(child.clone(), named_types)?;
            input.with_new_exprs(input.expressions(), vec![child])
        }
    }
}

fn columns_input(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Limit(Limit { input, .. })
        | LogicalPlan::Sort(Sort { input, .. })
        | LogicalPlan::Distinct(Distinct::All(input))
        | LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => Some(input),
        LogicalPlan::Aggregate(aggregate) if removes_duplicates(aggregate) => {
            Some(&aggregate.input)
        }
        _ => None,
    }
}

fn removes_duplicates(aggregate: &Aggregate) -> bool {
    let input_columns = aggregate.input.schema().columns();
    aggregate.aggr_expr.is_empty()
        && aggregate.group_expr.len() == input_columns.len()
        && aggregate
            .group_expr
            .iter()
            .zip(&input_columns)
            .all(|(expr, column)| matches!(expr, Expr::Column(c) if c == column))
}

fn projects_literal(input: &LogicalPlan, column: usize) -> bool {
    match input {
        LogicalPlan::Projection(projection) => {
            projection.expr.get(column).is_some_and(is_literal)
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .all(|input| projects_literal(input, column)),
        _ => columns_input(input).is_some_and(|input| projects_literal(input, column)),
    }
}
