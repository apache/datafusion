// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::logical_expr::SortExpr;
use substrait::proto::sort_field::{SortDirection, SortKind};
use substrait::proto::{Expression, SortField};

// Substrait wants a list of all field names, including nested fields from structs,
// also from within e.g. lists and maps. However, it does not want the list and map field names
// themselves - only proper structs fields are considered to have useful names.
pub(crate) fn flatten_names(
    field: &Field,
    skip_self: bool,
    names: &mut Vec<String>,
) -> datafusion::common::Result<()> {
    if !skip_self {
        names.push(field.name().to_string());
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            for field in fields {
                flatten_names(field, false, names)?;
            }
            Ok(())
        }
        DataType::List(l) => flatten_names(l, true, names),
        DataType::LargeList(l) => flatten_names(l, true, names),
        DataType::Map(m, _) => match m.data_type() {
            DataType::Struct(key_and_value) if key_and_value.len() == 2 => {
                flatten_names(&key_and_value[0], true, names)?;
                flatten_names(&key_and_value[1], true, names)
            }
            _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
        },
        _ => Ok(()),
    }?;
    Ok(())
}

pub(crate) fn substrait_sort_field(
    producer: &mut impl SubstraitProducer,
    sort: &SortExpr,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<SortField> {
    let SortExpr {
        expr,
        asc,
        nulls_first,
    } = sort;
    let e = producer.handle_expr(expr, schema)?;
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

pub(crate) fn to_substrait_precision(time_unit: &TimeUnit) -> i32 {
    match time_unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// Wraps an expression with a `not()` function.
pub(crate) fn negate(
    producer: &mut impl SubstraitProducer,
    expr: Expression,
) -> Expression {
    let function_anchor = producer.register_function("not".to_string());

    #[expect(deprecated)]
    Expression {
        rex_type: Some(substrait::proto::expression::RexType::ScalarFunction(
            substrait::proto::expression::ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![substrait::proto::FunctionArgument {
                    arg_type: Some(substrait::proto::function_argument::ArgType::Value(
                        expr,
                    )),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            },
        )),
    }
}
