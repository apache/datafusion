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

use datafusion_common::{DataFusionError, assert_contains};
use datafusion_sql::planner::SqlToRel;
use sqlparser::{dialect::GenericDialect, parser::Parser};

use crate::{MockContextProvider, MockSessionState};

fn do_query(sql: &'static str) -> DataFusionError {
    let dialect = GenericDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql(sql)
        .expect("unable to create parser")
        .parse_statement()
        .expect("unable to parse query");
    let state = MockSessionState::default();
    let context = MockContextProvider { state };
    let sql_to_rel = SqlToRel::new(&context);
    sql_to_rel
        .sql_statement_to_plan(statement)
        .expect_err("expected error")
}

#[test]
fn test_collect_select_items() {
    let query = "SELECT first_namex, last_namex FROM person";
    let error = do_query(query);
    let errors = error.iter().collect::<Vec<_>>();
    assert_eq!(errors.len(), 2);
    assert!(
        errors[0]
            .to_string()
            .contains("No field named first_namex.")
    );
    assert_contains!(errors[1].to_string(), "No field named last_namex.");
}

#[test]
fn test_collect_set_exprs() {
    let query = "SELECT first_namex FROM person UNION ALL SELECT last_namex FROM person";
    let error = do_query(query);
    let errors = error.iter().collect::<Vec<_>>();
    assert_eq!(errors.len(), 2);
    assert_contains!(errors[0].to_string(), "No field named first_namex.");
    assert_contains!(errors[1].to_string(), "No field named last_namex.");
}
