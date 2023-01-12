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

mod util;

use crate::error::Result;
use crate::insert::util::LogicTestContextProvider;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::Expr as DFExpr;
use datafusion_sql::planner::{object_name_to_table_reference, PlannerContext, SqlToRel};
use sqllogictest::DBOutput;
use sqlparser::ast::{Expr, SetExpr, Statement as SQLStatement};
use std::sync::Arc;

pub async fn insert(ctx: &SessionContext, insert_stmt: SQLStatement) -> Result<DBOutput> {
    // First, use sqlparser to get table name and insert values
    let table_reference;
    let insert_values: Vec<Vec<Expr>>;
    match insert_stmt {
        SQLStatement::Insert {
            table_name, source, ..
        } => {
            table_reference = object_name_to_table_reference(table_name)?;

            // Todo: check columns match table schema
            match *source.body {
                SetExpr::Values(values) => {
                    insert_values = values.rows;
                }
                _ => {
                    // Directly panic: make it easy to find the location of the error.
                    panic!()
                }
            }
        }
        _ => unreachable!(),
    }

    // Second, get batches in table and destroy the old table
    let mut origin_batches = ctx.table(&table_reference).await?.collect().await?;
    let schema = ctx.table_provider(&table_reference).await?.schema();
    ctx.deregister_table(&table_reference)?;

    // Third, transfer insert values to `RecordBatch`
    // Attention: schema info can be ignored. (insert values don't contain schema info)
    let sql_to_rel = SqlToRel::new(&LogicTestContextProvider {});
    let num_rows = insert_values.len();
    for row in insert_values.into_iter() {
        let logical_exprs = row
            .into_iter()
            .map(|expr| {
                sql_to_rel.sql_to_expr(
                    expr,
                    &DFSchema::empty(),
                    &mut PlannerContext::new(),
                )
            })
            .collect::<std::result::Result<Vec<DFExpr>, DataFusionError>>()?;
        // Directly use `select` to get `RecordBatch`
        let dataframe = ctx.read_empty()?;
        origin_batches.extend(dataframe.select(logical_exprs)?.collect().await?)
    }

    // Replace new batches schema to old schema
    for batch in origin_batches.iter_mut() {
        *batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())?;
    }

    // Final, create new memtable with same schema.
    let new_provider = MemTable::try_new(schema, vec![origin_batches])?;
    ctx.register_table(&table_reference, Arc::new(new_provider))?;

    Ok(DBOutput::StatementComplete(num_rows as u64))
}
