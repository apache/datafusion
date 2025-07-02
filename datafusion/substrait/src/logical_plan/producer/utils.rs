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

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::SortExpr;
use substrait::proto::sort_field::{SortDirection, SortKind};
use substrait::proto::SortField;

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
