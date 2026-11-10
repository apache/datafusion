// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_common::{DataFusionError, assert_contains};
use datafusion_sql::parser::DFParser;
use datafusion_sql::planner::SqlToRel;

use crate::{MockContextProvider, MockSessionState};

fn do_query(sql: &'static str) -> DataFusionError {
    let statement = DFParser::parse_sql(sql)
        .expect("unable to parse query")
        .pop_front()
        .expect("expected a statement");
    let state = MockSessionState::default();
    let context = MockContextProvider { state };
    let sql_to_rel = SqlToRel::new(&context).with_source(sql);
    sql_to_rel
        .statement_to_plan(statement)
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
