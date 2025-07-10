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
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion_common::test_util::datafusion_test_data;

#[tokio::test]
async fn recursive_cte_alias_instability() -> Result<()> {
    let ctx = SessionContext::new();
    let testdata = datafusion_test_data();
    let csv_path = format!("{testdata}/recursive_cte/prices.csv");
    ctx.register_csv("prices", &csv_path, CsvReadOptions::new().has_header(true))
        .await?;

    let sql = r#"
WITH RECURSIVE "recursive_cte" AS (
  (
    WITH "min_prices_row_num_cte" AS (
      SELECT
        MIN("prices"."prices_row_num") AS "prices_row_num"
      FROM
        "prices"
    ),
    "min_prices_row_num_cte_second" AS (
      SELECT
        MIN("prices"."prices_row_num") AS "prices_row_num_advancement"
      FROM
        "prices"
      WHERE
        "prices"."prices_row_num" > (
          SELECT
            "prices_row_num"
          FROM
            "min_prices_row_num_cte"
        )
    )
    SELECT
      0.0 AS "beg",
      (0.0 + 50) AS "end",
      (
        SELECT
          "prices_row_num"
        FROM
          "min_prices_row_num_cte"
      ) AS "prices_row_num",
      (
        SELECT
          "prices_row_num_advancement"
        FROM
          "min_prices_row_num_cte_second"
      ) AS "prices_row_num_advancement"
    FROM
      "prices"
    WHERE
      "prices"."prices_row_num" = (
        SELECT
          DISTINCT "prices_row_num"
        FROM
          "min_prices_row_num_cte"
      )
  )
  UNION ALL (
    WITH "min_prices_row_num_cte" AS (
      SELECT
        "prices"."prices_row_num" AS "prices_row_num",
        LEAD("prices"."prices_row_num", 1) OVER (
          ORDER BY "prices_row_num"
        ) AS "prices_row_num_advancement"
      FROM
        (
          SELECT
            DISTINCT "prices_row_num"
          FROM
            "prices"
        ) AS "prices"
    )
    SELECT
      "recursive_cte"."end" AS "beg",
      ("recursive_cte"."end" + 50) AS "end",
      "min_prices_row_num_cte"."prices_row_num" AS "prices_row_num",
      "min_prices_row_num_cte"."prices_row_num_advancement" AS "prices_row_num_advancement"
    FROM
      "recursive_cte"
      FULL JOIN "prices" ON "prices"."prices_row_num" = "recursive_cte"."prices_row_num_advancement"
      FULL JOIN "min_prices_row_num_cte" ON "min_prices_row_num_cte"."prices_row_num" = COALESCE(
        "prices"."prices_row_num",
        "recursive_cte"."prices_row_num_advancement"
      )
    WHERE
      "recursive_cte"."prices_row_num_advancement" IS NOT NULL
  )
)
SELECT
  DISTINCT *
FROM
  "recursive_cte"
ORDER BY
  "prices_row_num" ASC;
"#;

    println!("Creating logical plan...");
    let df = ctx.sql(sql).await?;

    let result = df.collect().await;

    // Assert that no error occurs
    assert!(
        result.is_ok(),
        "Expected no error, but got: {:?}",
        result.err()
    );
    Ok(())
}
