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

use super::*;

#[tokio::test]
async fn csv_query_error() -> Result<()> {
    // sin(utf8) should error
    let mut ctx = create_ctx()?;
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT sin(c1) FROM aggregate_test_100";
    let plan = ctx.create_logical_plan(sql);
    assert!(plan.is_err());
    Ok(())
}

#[tokio::test]
async fn test_cast_expressions_error() -> Result<()> {
    // sin(utf8) should error
    let mut ctx = create_ctx()?;
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT CAST(c1 AS INT) FROM aggregate_test_100";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let result = collect(plan).await;

    match result {
        Ok(_) => panic!("expected error"),
        Err(e) => {
            assert_contains!(e.to_string(),
                             "Cast error: Cannot cast string 'c' to value of arrow::datatypes::types::Int32Type type"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_aggregation_with_bad_arguments() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT COUNT(DISTINCT) FROM aggregate_test_100";
    let logical_plan = ctx.create_logical_plan(sql);
    let err = logical_plan.unwrap_err();
    assert_eq!(
        err.to_string(),
        DataFusionError::Plan(
            "The function Count expects 1 arguments, but 0 were provided".to_string()
        )
        .to_string()
    );
    Ok(())
}

#[tokio::test]
async fn query_cte_incorrect() -> Result<()> {
    let ctx = ExecutionContext::new();

    // self reference
    let sql = "WITH t AS (SELECT * FROM t) SELECT * from u";
    let plan = ctx.create_logical_plan(sql);
    assert!(plan.is_err());
    assert_eq!(
        format!("{}", plan.unwrap_err()),
        "Error during planning: Table or CTE with name \'t\' not found"
    );

    // forward referencing
    let sql = "WITH t AS (SELECT * FROM u), u AS (SELECT 1) SELECT * from u";
    let plan = ctx.create_logical_plan(sql);
    assert!(plan.is_err());
    assert_eq!(
        format!("{}", plan.unwrap_err()),
        "Error during planning: Table or CTE with name \'u\' not found"
    );

    // wrapping should hide u
    let sql = "WITH t AS (WITH u as (SELECT 1) SELECT 1) SELECT * from u";
    let plan = ctx.create_logical_plan(sql);
    assert!(plan.is_err());
    assert_eq!(
        format!("{}", plan.unwrap_err()),
        "Error during planning: Table or CTE with name \'u\' not found"
    );

    Ok(())
}

#[tokio::test]
async fn test_select_wildcard_without_table() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * ";
    let actual = ctx.sql(sql).await;
    match actual {
        Ok(_) => panic!("expect err"),
        Err(e) => {
            assert_contains!(
                e.to_string(),
                "Error during planning: SELECT * with no tables specified is not valid"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn invalid_qualified_table_references() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;

    for table_ref in &[
        "nonexistentschema.aggregate_test_100",
        "nonexistentcatalog.public.aggregate_test_100",
        "way.too.many.namespaces.as.ident.prefixes.aggregate_test_100",
    ] {
        let sql = format!("SELECT COUNT(*) FROM {}", table_ref);
        assert!(matches!(ctx.sql(&sql).await, Err(DataFusionError::Plan(_))));
    }
    Ok(())
}
