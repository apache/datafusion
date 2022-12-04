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

use crate::error::{DFSqlLogicTestError, Result};
use crate::insert::util::LogicTestContextProvider;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::Expr as DFExpr;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::planner::SqlToRel;
use sqlparser::ast::{Expr, SetExpr, Statement as SQLStatement};
use std::collections::HashMap;

pub async fn insert(ctx: &SessionContext, sql: String) -> Result<String> {
    // First, use sqlparser to get table name and insert values
    let mut table_name = "".to_string();
    let mut insert_values: Vec<Vec<Expr>> = vec![];
    if let Statement::Statement(statement) = &DFParser::parse_sql(&sql)?[0] {
        if let SQLStatement::Insert {
            table_name: name,
            source,
            ..
        } = &**statement
        {
            // Todo: check columns match table schema
            table_name = name.to_string();
            match &*source.body {
                SetExpr::Values(values) => {
                    insert_values = values.0.clone();
                }
                _ => {
                    return Err(DFSqlLogicTestError::NotImplemented(
                        "Only support insert values".to_string(),
                    ));
                }
            }
        }
    } else {
        return Err(DFSqlLogicTestError::Internal(format!(
            "{:?} not an insert statement",
            sql
        )));
    }

    // Second, get table by table name
    // Here we assume table must be in memory table.
    let table_provider = ctx.table_provider(table_name.as_str())?;
    let table_batches = table_provider
        .as_any()
        .downcast_ref::<MemTable>()
        .ok_or_else(|| {
            DFSqlLogicTestError::NotImplemented(
                "only support use memory table in logictest".to_string(),
            )
        })?
        .get_batches();

    // Third, transfer insert values to `RecordBatch`
    // Attention: schema info can be ignored. (insert values don't contain schema info)
    let sql_to_rel = SqlToRel::new(&LogicTestContextProvider {});
    let mut insert_batches = Vec::with_capacity(insert_values.len());
    for row in insert_values.into_iter() {
        let logical_exprs = row
            .into_iter()
            .map(|expr| {
                sql_to_rel.sql_to_rex(expr, &DFSchema::empty(), &mut HashMap::new())
            })
            .collect::<std::result::Result<Vec<DFExpr>, DataFusionError>>()?;
        // Directly use `select` to get `RecordBatch`
        let dataframe = ctx.read_empty()?;
        insert_batches.push(dataframe.select(logical_exprs)?.collect().await?)
    }

    // Final, append the `RecordBatch` to memtable's batches
    let mut table_batches = table_batches.write();
    table_batches.extend(insert_batches);

    Ok("".to_string())
}
