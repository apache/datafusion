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

use arrow::datatypes::{DataType, Field};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{avg, col, lit};
use datafusion_functions_aggregate::expr_fn::sum;
use datafusion_optimizer::common_subexpr_eliminate::{expr_to_identifier, ExprMask};
use std::{collections::HashMap, sync::Arc};

#[test]
fn id_array_visitor() -> Result<()> {
    let expr = ((sum(col("a") + lit(1))) - avg(col("c"))) * lit(2);

    let schema = Arc::new(DFSchema::from_unqualifed_fields(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]
        .into(),
        Default::default(),
    )?);

    // skip aggregates
    let mut id_array = vec![];
    expr_to_identifier(
        &expr,
        &mut HashMap::new(),
        &mut id_array,
        Arc::clone(&schema),
        ExprMask::Normal,
    )?;

    let expected = vec![
        (8, "{(SUM(a + Int32(1)) - AVG(c)) * Int32(2)|{Int32(2)}|{SUM(a + Int32(1)) - AVG(c)|{AVG(c)|{c}}|{SUM(a + Int32(1))|{a + Int32(1)|{Int32(1)}|{a}}}}}"),
        (6, "{SUM(a + Int32(1)) - AVG(c)|{AVG(c)|{c}}|{SUM(a + Int32(1))|{a + Int32(1)|{Int32(1)}|{a}}}}"),
        (3, ""),
        (2, "{a + Int32(1)|{Int32(1)}|{a}}"),
        (0, ""),
        (1, ""),
        (5, ""),
        (4, ""),
        (7, "")
    ]
    .into_iter()
    .map(|(number, id)| (number, id.into()))
    .collect::<Vec<_>>();
    assert_eq!(expected, id_array);

    // include aggregates
    let mut id_array = vec![];
    expr_to_identifier(
        &expr,
        &mut HashMap::new(),
        &mut id_array,
        Arc::clone(&schema),
        ExprMask::NormalAndAggregates,
    )?;

    let expected = vec![
        (8, "{(SUM(a + Int32(1)) - AVG(c)) * Int32(2)|{Int32(2)}|{SUM(a + Int32(1)) - AVG(c)|{AVG(c)|{c}}|{SUM(a + Int32(1))|{a + Int32(1)|{Int32(1)}|{a}}}}}"),
        (6, "{SUM(a + Int32(1)) - AVG(c)|{AVG(c)|{c}}|{SUM(a + Int32(1))|{a + Int32(1)|{Int32(1)}|{a}}}}"),
        (3, "{SUM(a + Int32(1))|{a + Int32(1)|{Int32(1)}|{a}}}"),
        (2, "{a + Int32(1)|{Int32(1)}|{a}}"),
        (0, ""),
        (1, ""),
        (5, "{AVG(c)|{c}}"),
        (4, ""),
        (7, "")
    ]
    .into_iter()
    .map(|(number, id)| (number, id.into()))
    .collect::<Vec<_>>();
    assert_eq!(expected, id_array);

    Ok(())
}
