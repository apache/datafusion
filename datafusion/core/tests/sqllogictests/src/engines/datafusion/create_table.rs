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

use super::error::Result;
use crate::engines::datafusion::error::DFSqlLogicTestError;
use crate::engines::datafusion::util::LogicTestContextProvider;
use crate::engines::output::DFOutput;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, OwnedTableReference};
use datafusion_sql::planner::{object_name_to_table_reference, ParserOptions, SqlToRel};
use sqllogictest::DBOutput;
use sqlparser::ast::{ColumnDef, ObjectName};
use std::sync::Arc;

pub async fn create_table(
    ctx: &SessionContext,
    name: ObjectName,
    columns: Vec<ColumnDef>,
    if_not_exists: bool,
    or_replace: bool,
) -> Result<DFOutput> {
    let table_reference =
        object_name_to_table_reference(name, ctx.enable_ident_normalization())?;
    let existing_table = ctx.table(&table_reference).await;
    match (if_not_exists, or_replace, existing_table) {
        (true, false, Ok(_)) => Ok(DBOutput::StatementComplete(0)),
        (false, true, Ok(_)) => {
            ctx.deregister_table(&table_reference)?;
            create_new_table(ctx, table_reference, columns)
        }
        (true, true, Ok(_)) => {
            Err(DFSqlLogicTestError::from(DataFusionError::Execution(
                "'IF NOT EXISTS' cannot coexist with 'REPLACE'".to_string(),
            )))
        }
        (_, _, Err(_)) => create_new_table(ctx, table_reference, columns),
        (false, false, Ok(_)) => {
            Err(DFSqlLogicTestError::from(DataFusionError::Execution(
                format!("Table '{table_reference}' already exists"),
            )))
        }
    }
}

fn create_new_table(
    ctx: &SessionContext,
    table_reference: OwnedTableReference,
    columns: Vec<ColumnDef>,
) -> Result<DFOutput> {
    let config = ctx.copied_config();
    let sql_to_rel = SqlToRel::new_with_options(
        &LogicTestContextProvider {},
        ParserOptions {
            parse_float_as_decimal: config
                .config_options()
                .sql_parser
                .parse_float_as_decimal,
            enable_ident_normalization: config
                .config_options()
                .sql_parser
                .enable_ident_normalization,
        },
    );
    let schema = Arc::new(sql_to_rel.build_schema(columns)?);
    let table_provider = Arc::new(MemTable::try_new(schema, vec![])?);
    ctx.register_table(&table_reference, table_provider)?;
    Ok(DBOutput::StatementComplete(0))
}
