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

#![allow(dead_code)]
#![allow(unused_macros)]
#![allow(unused_imports)]

use arrow::{
    array::ArrayRef, record_batch::RecordBatch, util::display::array_value_to_string,
};
use datafusion::prelude::SessionContext;

/// Execute query and return result set as 2-d table of Vecs
/// `result[row][column]`
pub async fn execute(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    result_vec(&execute_to_batches(ctx, sql).await)
}

/// Specialised String representation
fn col_str(column: &ArrayRef, row_index: usize) -> String {
    if column.is_null(row_index) {
        return "NULL".to_string();
    }

    array_value_to_string(column, row_index)
        .ok()
        .unwrap_or_else(|| "???".to_string())
}
/// Converts the results into a 2d array of strings, `result[row][column]`
/// Special cases nulls to NULL for testing
fn result_vec(results: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut result = vec![];
    for batch in results {
        for row_index in 0..batch.num_rows() {
            let row_vec = batch
                .columns()
                .iter()
                .map(|column| col_str(column, row_index))
                .collect();
            result.push(row_vec);
        }
    }
    result
}

/// Execute query and return results as a Vec of RecordBatches
async fn execute_to_batches(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let df = ctx.sql(sql).await.unwrap();

    // optimize just for check schema don't change during optimization.
    df.clone().into_optimized_plan().unwrap();

    df.collect().await.unwrap()
}

macro_rules! test_expression {
    ($SQL:expr, $EXPECTED:expr) => {
        let ctx = SessionContext::new();
        ctx.register_scalar_function_package(Box::new(FunctionPackage));
        let sql = format!("SELECT {}", $SQL);
        let actual = execute(&ctx, sql.as_str()).await;
        assert_eq!(actual[0][0], $EXPECTED);
    };
}

pub(crate) use test_expression;
