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

use datafusion::{assert_batches_sorted_eq, physical_plan::collect};

use super::*;

#[tokio::test]
async fn intersect_all_preserves_duplicate_counts_and_nulls() -> Result<()> {
    let ctx = SessionContext::new();
    let batches = collect(
        ctx.sql(
            "SELECT * FROM VALUES (NULL), (NULL), (1), (1), (2)
             INTERSECT ALL
             SELECT * FROM VALUES (NULL), (1), (1), (1), (3)",
        )
        .await?
        .create_physical_plan()
        .await?,
        ctx.task_ctx(),
    )
    .await?;

    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 1       |",
            "|         |",
            "+---------+",
        ],
        &batches
    );
    Ok(())
}

#[tokio::test]
async fn except_all_preserves_duplicate_counts_and_nulls() -> Result<()> {
    let ctx = SessionContext::new();
    let batches = collect(
        ctx.sql(
            "SELECT * FROM VALUES (NULL), (NULL), (1), (1), (2)
             EXCEPT ALL
             SELECT * FROM VALUES (NULL), (1), (1), (1), (3)",
        )
        .await?
        .create_physical_plan()
        .await?,
        ctx.task_ctx(),
    )
    .await?;

    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 2       |",
            "|         |",
            "+---------+",
        ],
        &batches
    );
    Ok(())
}

#[tokio::test]
async fn set_operation_all_preserves_synthetic_column_names() -> Result<()> {
    let ctx = SessionContext::new();
    let select = "SELECT column1 AS __datafusion_set_operation_left_count,
                         column1 AS __datafusion_set_operation_right_count,
                         column1 AS __datafusion_set_operation_copies";
    for operation in ["INTERSECT ALL", "EXCEPT ALL"] {
        let sql =
            format!("{select} FROM VALUES (1), (1) {operation} {select} FROM VALUES (1)");
        let result = ctx.sql(&sql).await?.collect().await?;
        assert_batches_sorted_eq!(
            [
                "+---------------------------------------+----------------------------------------+-----------------------------------+",
                "| __datafusion_set_operation_left_count | __datafusion_set_operation_right_count | __datafusion_set_operation_copies |",
                "+---------------------------------------+----------------------------------------+-----------------------------------+",
                "| 1                                     | 1                                      | 1                                 |",
                "+---------------------------------------+----------------------------------------+-----------------------------------+",
            ],
            &result
        );
    }
    Ok(())
}

#[tokio::test]
async fn set_operation_all_keeps_left_qualifiers() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (x INT) AS VALUES (1), (1), (2)")
        .await?
        .collect()
        .await?;
    ctx.sql("CREATE TABLE u (x INT) AS VALUES (1), (3)")
        .await?
        .collect()
        .await?;

    let result = ctx
        .sql("SELECT t.x FROM t INTERSECT ALL SELECT u.x FROM u ORDER BY t.x")
        .await?
        .collect()
        .await?;
    assert_batches_sorted_eq!(["+---+", "| x |", "+---+", "| 1 |", "+---+"], &result);

    let t = ctx.table("t").await?;
    let u = ctx.table("u").await?;
    let result = t
        .clone()
        .intersect(u.clone())?
        .select(vec![col("t.x")])?
        .collect()
        .await?;
    assert_batches_sorted_eq!(["+---+", "| x |", "+---+", "| 1 |", "+---+"], &result);

    let result = t.except(u)?.select(vec![col("t.x")])?.collect().await?;
    assert_batches_sorted_eq!(
        ["+---+", "| x |", "+---+", "| 1 |", "| 2 |", "+---+"],
        &result
    );
    Ok(())
}

#[tokio::test]
async fn set_operation_all_with_same_name_from_two_relations() -> Result<()> {
    let ctx = SessionContext::new();
    let side = "SELECT t1.x, t2.x FROM (VALUES (1)) t1(x) CROSS JOIN (VALUES (2)) t2(x)";
    let result = ctx
        .sql(&format!("{side} INTERSECT ALL {side}"))
        .await?
        .collect()
        .await?;
    assert_batches_sorted_eq!(
        [
            "+---+---+",
            "| x | x |",
            "+---+---+",
            "| 1 | 2 |",
            "+---+---+"
        ],
        &result
    );

    let result = ctx
        .sql(&format!("{side} EXCEPT ALL {side}"))
        .await?
        .collect()
        .await?;
    assert!(result.iter().all(|batch| batch.num_rows() == 0));
    Ok(())
}

#[tokio::test]
async fn set_operation_all_without_count_or_range_is_a_plan_error() -> Result<()> {
    let state = datafusion::execution::SessionStateBuilder::new().build();
    let ctx = SessionContext::new_with_state(state);
    for operation in ["INTERSECT ALL", "EXCEPT ALL"] {
        let err = ctx
            .sql(&format!("SELECT 1 {operation} SELECT 1"))
            .await
            .unwrap_err();
        assert_contains!(
            err.strip_backtrace(),
            "Error during planning: INTERSECT ALL and EXCEPT ALL require the count aggregate function and the range scalar function to be registered"
        );
    }

    let df = ctx.sql("SELECT 1").await?;
    let err = df.clone().intersect(df.clone()).unwrap_err();
    assert_contains!(
        err.strip_backtrace(),
        "DataFrame::intersect requires the count aggregate function"
    );
    let err = df.clone().except(df).unwrap_err();
    assert_contains!(
        err.strip_backtrace(),
        "DataFrame::except requires the count aggregate function"
    );
    Ok(())
}

/// Inputs of `INTERSECT ALL` and `EXCEPT ALL` whose corresponding columns have
/// different types. `=` compares them in a third type, in which two distinct
/// values of one side can become equal (the strings '1' and '01' are both the
/// integer 1), so the rows have to be counted in that type.
struct MixedTypeCase {
    name: &'static str,
    left: &'static str,
    right: &'static str,
    /// The type of each column that both sides are compared in and returned as,
    /// in the form `arrow_cast` takes it.
    common: &'static [&'static str],
    /// Row counts of `INTERSECT ALL`, then `EXCEPT ALL`, each for the sides as
    /// written and for the sides swapped.
    rows: [usize; 4],
}

const MIXED_TYPE_CASES: &[MixedTypeCase] = &[
    MixedTypeCase {
        name: "integers against padded strings",
        left: "SELECT a FROM (VALUES (1), (1)) AS t(a)",
        right: "SELECT a FROM (VALUES ('1'), ('01')) AS t(a)",
        common: &["Int64"],
        rows: [2, 2, 0, 0],
    },
    MixedTypeCase {
        name: "one integer against padded strings",
        left: "SELECT a FROM (VALUES (1)) AS t(a)",
        right: "SELECT a FROM (VALUES ('1'), ('01')) AS t(a)",
        common: &["Int64"],
        rows: [1, 1, 0, 1],
    },
    MixedTypeCase {
        name: "integers against three spellings",
        left: "SELECT a FROM (VALUES (1), (1)) AS t(a)",
        right: "SELECT a FROM (VALUES ('1'), ('01'), ('001')) AS t(a)",
        common: &["Int64"],
        rows: [2, 2, 0, 1],
    },
    MixedTypeCase {
        name: "floats against strings in float notation",
        left: "SELECT a FROM (VALUES (1.0), (1.0), (2.0)) AS t(a)",
        right: "SELECT a FROM (VALUES ('1'), ('1.0'), ('01'), ('1e0')) AS t(a)",
        common: &["Float64"],
        rows: [2, 2, 1, 2],
    },
    MixedTypeCase {
        name: "integers beyond 2^53 against a float",
        left: "SELECT a FROM (VALUES (9007199254740992), (9007199254740993)) AS t(a)",
        right: "SELECT a FROM (VALUES (9007199254740992.0)) AS t(a)",
        common: &["Float64"],
        rows: [1, 1, 1, 0],
    },
    MixedTypeCase {
        name: "timestamps against strings",
        left: "SELECT arrow_cast(a, 'Timestamp(ns)') AS a FROM (VALUES ('2020-01-01 00:00:00'), ('2020-01-01 00:00:00')) AS t(a)",
        right: "SELECT a FROM (VALUES ('2020-01-01 00:00:00'), ('2020-01-01T00:00:00'), ('2020-01-01 00:00:00.000')) AS t(a)",
        common: &["Timestamp(ns)"],
        rows: [2, 2, 0, 1],
    },
    MixedTypeCase {
        name: "floats that round to the same decimal",
        left: "SELECT a FROM (VALUES (0.1), (0.10000000000000002)) AS t(a)",
        right: "SELECT arrow_cast(a, 'Decimal128(10, 2)') AS a FROM (VALUES (0.1)) AS t(a)",
        common: &["Decimal128(30, 15)"],
        rows: [1, 1, 1, 0],
    },
    MixedTypeCase {
        name: "second column mixed, with NULLs",
        left: "SELECT arrow_cast(a, 'Int64') AS a, b FROM (VALUES (NULL, 1), (NULL, 1), (7, 1)) AS t(a, b)",
        right: "SELECT arrow_cast(a, 'Int64') AS a, b FROM (VALUES (NULL, '1'), (NULL, '01'), (7, '1')) AS t(a, b)",
        common: &["Int64", "Int64"],
        rows: [3, 3, 0, 0],
    },
    MixedTypeCase {
        name: "lists of integers against lists of strings",
        left: "SELECT make_array(1) AS a",
        right: "SELECT make_array('1') AS a UNION ALL SELECT make_array('01') AS a",
        common: &["List(Int64)"],
        rows: [1, 1, 0, 1],
    },
    MixedTypeCase {
        name: "Int32 against Int64",
        left: "SELECT arrow_cast(a, 'Int32') AS a FROM (VALUES (1), (1), (2)) AS t(a)",
        right: "SELECT a FROM (VALUES (1), (3)) AS t(a)",
        common: &["Int64"],
        rows: [1, 1, 2, 1],
    },
    MixedTypeCase {
        name: "Utf8 against LargeUtf8",
        left: "SELECT a FROM (VALUES ('x'), ('x'), ('y')) AS t(a)",
        right: "SELECT arrow_cast(a, 'LargeUtf8') AS a FROM (VALUES ('x'), ('z')) AS t(a)",
        common: &["LargeUtf8"],
        rows: [1, 1, 2, 1],
    },
    MixedTypeCase {
        name: "Utf8 against Utf8View",
        left: "SELECT a FROM (VALUES ('x'), ('x'), ('y')) AS t(a)",
        right: "SELECT arrow_cast(a, 'Utf8View') AS a FROM (VALUES ('x'), ('z')) AS t(a)",
        common: &["Utf8View"],
        rows: [1, 1, 2, 1],
    },
];

fn sorted_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = vec![];
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells = batch
                .columns()
                .iter()
                .map(|column| array_value_to_string(column, row).unwrap())
                .collect::<Vec<_>>();
            rows.push(cells.join(","));
        }
    }
    rows.sort();
    rows
}

/// The sorted rows and output column types of `left OPERATION right`, planned
/// from SQL and through the DataFrame API.
async fn run_set_operation(
    ctx: &SessionContext,
    operation: &str,
    left: &str,
    right: &str,
) -> Result<Vec<(Vec<String>, Vec<String>)>> {
    let from_sql = ctx.sql(&format!("({left}) {operation} ({right})")).await?;
    let (left, right) = (ctx.sql(left).await?, ctx.sql(right).await?);
    let from_api = if operation == "INTERSECT ALL" {
        left.intersect(right)?
    } else {
        left.except(right)?
    };
    let mut results = vec![];
    for df in [from_sql, from_api] {
        let types = df
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().to_string())
            .collect::<Vec<_>>();
        results.push((sorted_rows(&df.collect().await?), types));
    }
    Ok(results)
}

#[tokio::test]
async fn set_operation_all_counts_rows_in_the_type_they_are_compared_in() -> Result<()> {
    let ctx = SessionContext::new();
    let mut failures = vec![];
    for case in MIXED_TYPE_CASES {
        // The same operation on inputs cast to the common type up front never
        // needs the planner to coerce anything, so it is the expected result.
        let cast = |query: &str| {
            let casts = case
                .common
                .iter()
                .zip(["a", "b"])
                .map(|(ty, column)| format!("arrow_cast({column}, '{ty}') AS {column}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("SELECT {casts} FROM ({query}) AS cast_input")
        };
        for (operation_index, operation) in
            ["INTERSECT ALL", "EXCEPT ALL"].into_iter().enumerate()
        {
            for (swapped_index, swapped) in [false, true].into_iter().enumerate() {
                let (left, right) = if swapped {
                    (case.right, case.left)
                } else {
                    (case.left, case.right)
                };
                let expected =
                    run_set_operation(&ctx, operation, &cast(left), &cast(right)).await?;
                let actual = run_set_operation(&ctx, operation, left, right).await?;
                for (api, (actual, expected)) in ["SQL", "DataFrame"]
                    .into_iter()
                    .zip(actual.iter().zip(&expected))
                {
                    let context =
                        format!("{} ({api}): {left} {operation} {right}", case.name);
                    if actual.0 != expected.0 {
                        failures.push(format!(
                            "rows of {context}\n    got {:?}\n    want {:?}",
                            actual.0, expected.0
                        ));
                    }
                    if actual.1 != case.common {
                        failures.push(format!(
                            "types of {context}\n    got {:?}\n    want {:?}",
                            actual.1, case.common
                        ));
                    }
                    let want = case.rows[operation_index * 2 + swapped_index];
                    if actual.0.len() != want {
                        failures.push(format!(
                            "row count of {context}\n    got {}\n    want {want}",
                            actual.0.len()
                        ));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}
