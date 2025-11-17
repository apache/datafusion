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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::context::ExecutionProps;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::prelude::*;

/// This example shows how to use  DataFusion's `PruningPredicate` to prove
/// filter expressions can never be true based on statistics such as min/max
/// values of columns.
///
/// The process is called "pruning" and is commonly used in query engines to
/// quickly eliminate entire files / partitions / row groups of data from
/// consideration using statistical information from a catalog or other
/// metadata.
///
/// This example uses a user defined catalog to supply pruning information, as
/// one might do as part of a higher level storage engine. See
/// `parquet_index.rs` for an example that uses pruning in the context of an
/// individual query.
pub async fn pruning() -> Result<()> {
    // In this example, we'll use the PruningPredicate to determine if
    // the expression `x = 5 AND y = 10` can never be true based on statistics

    // Start with the expression `x = 5 AND y = 10`
    let expr = col("x").eq(lit(5)).and(col("y").eq(lit(10)));

    // We can analyze this predicate using information provided by the
    // `PruningStatistics` trait, in this case we'll use a simple catalog that
    // models three files.  For all rows in each file:
    //
    //  File 1: x has values between `4` and `6`
    //          y has the value 10
    //
    //  File 2: x has values between `4` and `6`
    //          y has the value of `7`
    //
    //  File 3: x has the value 1
    //          nothing is known about the value of y
    let my_catalog = MyCatalog::new();

    // Create a `PruningPredicate`.
    //
    // Note the predicate does not automatically coerce types or simplify
    // expressions. See expr_api.rs examples for how to do this if required
    let predicate = create_pruning_predicate(expr, &my_catalog.schema);

    // Evaluate the predicate for the three files in the catalog
    let prune_results = predicate.prune(&my_catalog)?;
    println!("Pruning results: {prune_results:?}");

    // The result is a `Vec` of bool values, one for each file in the catalog
    assert_eq!(
        prune_results,
        vec![
            // File 1: `x = 5 AND y = 10` can evaluate to true if x has values
            // between `4` and `6`, y has the value `10`, so the file can not be
            // skipped
            //
            // NOTE this doesn't mean there actually are rows that evaluate to
            // true, but the pruning predicate can't prove there aren't any.
            true,
            // File 2: `x = 5 AND y = 10` can never evaluate to true because y
            // has only the value of 7. Thus this file can be skipped.
            false,
            // File 3: `x = 5 AND y = 10` can never evaluate to true because x
            // has the value `1`, and for any value of `y` the expression will
            // evaluate to false (`x = 5 AND y = 10 -->` false AND null` -->
            // `false`). Thus this file can also be skipped.
            false
        ]
    );

    Ok(())
}

/// A simple model catalog that has information about the three files that store
/// data for a table with two columns (x and y).
struct MyCatalog {
    schema: SchemaRef,
    // (min, max) for x
    x_values: Vec<(Option<i32>, Option<i32>)>,
    // (min, max) for y
    y_values: Vec<(Option<i32>, Option<i32>)>,
}
impl MyCatalog {
    fn new() -> Self {
        MyCatalog {
            schema: Arc::new(Schema::new(vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Int32, false),
            ])),
            x_values: vec![
                // File 1: x has values between `4` and `6`
                (Some(4), Some(6)),
                // File 2: x has values between `4` and `6`
                (Some(4), Some(6)),
                // File 3: x has the value 1
                (Some(1), Some(1)),
            ],
            y_values: vec![
                // File 1: y has the value 10
                (Some(10), Some(10)),
                // File 2: y has the value of `7`
                (Some(7), Some(7)),
                // File 3: nothing is known about the value of y. This is
                // represented as (None, None).
                //
                // Note, returning null means the value isn't known, NOT
                // that we know the entire column is null.
                (None, None),
            ],
        }
    }
}

/// We communicate the statistical information to DataFusion by implementing the
/// PruningStatistics trait.
impl PruningStatistics for MyCatalog {
    fn num_containers(&self) -> usize {
        // there are 3 files in this "catalog", and thus each array returned
        // from min_values and max_values also has 3 elements
        3
    }

    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        // The pruning predicate evaluates the bounds for multiple expressions
        // at once, so  return an array with an element for the minimum value in
        // each file
        match column.name.as_str() {
            "x" => Some(i32_array(self.x_values.iter().map(|(min, _)| min))),
            "y" => Some(i32_array(self.y_values.iter().map(|(min, _)| min))),
            name => panic!("unknown column name: {name}"),
        }
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        // similarly to min_values, return an array with an element for the
        // maximum value in each file
        match column.name.as_str() {
            "x" => Some(i32_array(self.x_values.iter().map(|(_, max)| max))),
            "y" => Some(i32_array(self.y_values.iter().map(|(_, max)| max))),
            name => panic!("unknown column name: {name}"),
        }
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        // In this example, we know nothing about the number of nulls
        None
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        // In this example, we know nothing about the number of rows in each file
        None
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        // this method can be used to implement Bloom filter like filtering
        // but we do not illustrate that here
        None
    }
}

fn create_pruning_predicate(expr: Expr, schema: &SchemaRef) -> PruningPredicate {
    let df_schema = DFSchema::try_from(Arc::clone(schema)).unwrap();
    let props = ExecutionProps::new();
    let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
    PruningPredicate::try_new(physical_expr, Arc::clone(schema)).unwrap()
}

fn i32_array<'a>(values: impl Iterator<Item = &'a Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from_iter(values.cloned()))
}
