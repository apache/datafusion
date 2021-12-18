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

//! This module contains end to end tests of running SQL queries using
//! DataFusion

use std::convert::TryFrom;
use std::sync::Arc;

use chrono::prelude::*;
use chrono::Duration;

extern crate arrow;
extern crate datafusion;

use arrow::{
    array::*, datatypes::*, record_batch::RecordBatch,
    util::display::array_value_to_string,
};

use datafusion::assert_batches_eq;
use datafusion::assert_batches_sorted_eq;
use datafusion::assert_contains;
use datafusion::assert_not_contains;
use datafusion::logical_plan::plan::{Aggregate, Projection};
use datafusion::logical_plan::LogicalPlan;
use datafusion::logical_plan::TableScan;
use datafusion::physical_plan::functions::Volatility;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanVisitor;
use datafusion::prelude::*;
use datafusion::test_util;
use datafusion::{datasource::MemTable, physical_plan::collect};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ColumnarValue,
};
use datafusion::{execution::context::ExecutionContext, physical_plan::displayable};

#[tokio::test]
async fn nyc() -> Result<()> {
    // schema for nyxtaxi csv files
    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Utf8, true),
        Field::new("trip_distance", DataType::Float64, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ]);

    let mut ctx = ExecutionContext::new();
    ctx.register_csv(
        "tripdata",
        "file.csv",
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    let logical_plan = ctx.create_logical_plan(
        "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount) \
         FROM tripdata GROUP BY passenger_count",
    )?;

    let optimized_plan = ctx.optimize(&logical_plan)?;

    match &optimized_plan {
        LogicalPlan::Projection(Projection { input, .. }) => match input.as_ref() {
            LogicalPlan::Aggregate(Aggregate { input, .. }) => match input.as_ref() {
                LogicalPlan::TableScan(TableScan {
                    ref projected_schema,
                    ..
                }) => {
                    assert_eq!(2, projected_schema.fields().len());
                    assert_eq!(projected_schema.field(0).name(), "passenger_count");
                    assert_eq!(projected_schema.field(1).name(), "fare_amount");
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        },
        _ => unreachable!(false),
    }

    Ok(())
}

#[tokio::test]
async fn parquet_query() {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
    // so we need an explicit cast
    let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------------------+",
        "| id | CAST(alltypes_plain.string_col AS Utf8) |",
        "+----+-----------------------------------------+",
        "| 4  | 0                                       |",
        "| 5  | 1                                       |",
        "| 6  | 0                                       |",
        "| 7  | 1                                       |",
        "| 2  | 0                                       |",
        "| 3  | 1                                       |",
        "| 0  | 0                                       |",
        "| 1  | 1                                       |",
        "+----+-----------------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn parquet_single_nan_schema() {
    let mut ctx = ExecutionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet("single_nan", &format!("{}/single_nan.parquet", testdata))
        .await
        .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let results = collect(plan).await.unwrap();
    for batch in results {
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[tokio::test]
#[ignore = "Test ignored, will be enabled as part of the nested Parquet reader"]
async fn parquet_list_columns() {
    let mut ctx = ExecutionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "list_columns",
        &format!("{}/list_columns.parquet", testdata),
    )
    .await
    .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "int64_list",
            DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new(
            "utf8_list",
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    let sql = "SELECT int64_list, utf8_list FROM list_columns";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let results = collect(plan).await.unwrap();

    //   int64_list              utf8_list
    // 0  [1, 2, 3]        [abc, efg, hij]
    // 1  [None, 1]                   None
    // 2        [4]  [efg, None, hij, xyz]

    assert_eq!(1, results.len());
    let batch = &results[0];
    assert_eq!(3, batch.num_rows());
    assert_eq!(2, batch.num_columns());
    assert_eq!(schema, batch.schema());

    let int_list_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let utf8_list_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(
        int_list_array
            .value(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap(),
        &PrimitiveArray::<Int64Type>::from(vec![Some(1), Some(2), Some(3),])
    );

    assert_eq!(
        utf8_list_array
            .value(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::try_from(vec![Some("abc"), Some("efg"), Some("hij"),]).unwrap()
    );

    assert_eq!(
        int_list_array
            .value(1)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap(),
        &PrimitiveArray::<Int64Type>::from(vec![None, Some(1),])
    );

    assert!(utf8_list_array.is_null(1));

    assert_eq!(
        int_list_array
            .value(2)
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .unwrap(),
        &PrimitiveArray::<Int64Type>::from(vec![Some(4),])
    );

    let result = utf8_list_array.value(2);
    let result = result.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(result.value(0), "efg");
    assert!(result.is_null(1));
    assert_eq!(result.value(2), "hij");
    assert_eq!(result.value(3), "xyz");
}

#[tokio::test]
async fn csv_select_nested() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT o1, o2, c3
               FROM (
                 SELECT c1 AS o1, c2 + 1 AS o2, c3
                 FROM (
                   SELECT c1, c2, c3, c4
                   FROM aggregate_test_100
                   WHERE c1 = 'a' AND c2 >= 4
                   ORDER BY c2 ASC, c3 ASC
                 ) AS a
               ) AS b";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+----+------+",
        "| o1 | o2 | c3   |",
        "+----+----+------+",
        "| a  | 5  | -101 |",
        "| a  | 5  | -54  |",
        "| a  | 5  | -38  |",
        "| a  | 5  | 65   |",
        "| a  | 6  | -101 |",
        "| a  | 6  | -31  |",
        "| a  | 6  | 36   |",
        "+----+----+------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_count_star() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT COUNT(*), COUNT(1) AS c, COUNT(c1) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+-----+------------------------------+",
        "| COUNT(UInt8(1)) | c   | COUNT(aggregate_test_100.c1) |",
        "+-----------------+-----+------------------------------+",
        "| 100             | 100 | 100                          |",
        "+-----------------+-----+------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, c12 FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+---------------------+",
        "| c1 | c12                 |",
        "+----+---------------------+",
        "| e  | 0.39144436569161134 |",
        "| d  | 0.38870280983958583 |",
        "+----+---------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_negative_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, c4 FROM aggregate_test_100 WHERE c3 < -55 AND -c4 > 30000";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+--------+",
        "| c1 | c4     |",
        "+----+--------+",
        "| e  | -31500 |",
        "| c  | -30187 |",
        "+----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_negated_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE NOT(c1 != 'a')";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 21              |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_is_not_null_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE c1 IS NOT NULL";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_with_is_null_predicate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100 WHERE c1 IS NULL";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 0               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_int_min_max() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c2, MIN(c12), MAX(c12) FROM aggregate_test_100 GROUP BY c2";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+-----------------------------+",
        "| c2 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) |",
        "+----+-----------------------------+-----------------------------+",
        "| 1  | 0.05636955101974106         | 0.9965400387585364          |",
        "| 2  | 0.16301110515739792         | 0.991517828651004           |",
        "| 3  | 0.047343434291126085        | 0.9293883502480845          |",
        "| 4  | 0.02182578039211991         | 0.9237877978193884          |",
        "| 5  | 0.01479305307777301         | 0.9723580396501548          |",
        "+----+-----------------------------+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_float32() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    let sql =
        "SELECT COUNT(*) as cnt, c1 FROM aggregate_simple GROUP BY c1 ORDER BY cnt DESC";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----+---------+",
        "| cnt | c1      |",
        "+-----+---------+",
        "| 5   | 0.00005 |",
        "| 4   | 0.00004 |",
        "| 3   | 0.00003 |",
        "| 2   | 0.00002 |",
        "| 1   | 0.00001 |",
        "+-----+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn select_values_list() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    {
        let sql = "VALUES (1)";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (-1)";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+",
            "| column1 |",
            "+---------+",
            "| -1      |",
            "+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (2+1,2-1,2>1)";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+---------+---------+",
            "| column1 | column2 | column3 |",
            "+---------+---------+---------+",
            "| 3       | 1       | true    |",
            "+---------+---------+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES";
        let plan = ctx.create_logical_plan(sql);
        assert!(plan.is_err());
    }
    {
        let sql = "VALUES ()";
        let plan = ctx.create_logical_plan(sql);
        assert!(plan.is_err());
    }
    {
        let sql = "VALUES (1),(2)";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 2       |",
            "+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (1),()";
        let plan = ctx.create_logical_plan(sql);
        assert!(plan.is_err());
    }
    {
        let sql = "VALUES (1,'a'),(2,'b')";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+---------+",
            "| column1 | column2 |",
            "+---------+---------+",
            "| 1       | a       |",
            "| 2       | b       |",
            "+---------+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (1),(1,2)";
        let plan = ctx.create_logical_plan(sql);
        assert!(plan.is_err());
    }
    {
        let sql = "VALUES (1),('2')";
        let plan = ctx.create_logical_plan(sql);
        assert!(plan.is_err());
    }
    {
        let sql = "VALUES (1),(2.0)";
        let plan = ctx.create_logical_plan(sql);
        assert!(plan.is_err());
    }
    {
        let sql = "VALUES (1,2), (1,'2')";
        let plan = ctx.create_logical_plan(sql);
        assert!(plan.is_err());
    }
    {
        let sql = "VALUES (1,'a'),(NULL,'b'),(3,'c')";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+---------+",
            "| column1 | column2 |",
            "+---------+---------+",
            "| 1       | a       |",
            "|         | b       |",
            "| 3       | c       |",
            "+---------+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (NULL,'a'),(NULL,'b'),(3,'c')";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+---------+",
            "| column1 | column2 |",
            "+---------+---------+",
            "|         | a       |",
            "|         | b       |",
            "| 3       | c       |",
            "+---------+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (NULL,'a'),(NULL,'b'),(NULL,'c')";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+---------+",
            "| column1 | column2 |",
            "+---------+---------+",
            "|         | a       |",
            "|         | b       |",
            "|         | c       |",
            "+---------+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (1,'a'),(2,NULL),(3,'c')";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+---------+",
            "| column1 | column2 |",
            "+---------+---------+",
            "| 1       | a       |",
            "| 2       |         |",
            "| 3       | c       |",
            "+---------+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (1,NULL),(2,NULL),(3,'c')";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+---------+",
            "| column1 | column2 |",
            "+---------+---------+",
            "| 1       |         |",
            "| 2       |         |",
            "| 3       | c       |",
            "+---------+---------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "VALUES (1,2,3,4,5,6,7,8,9,10,11,12,13,NULL,'F',3.5)";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+",
            "| column1 | column2 | column3 | column4 | column5 | column6 | column7 | column8 | column9 | column10 | column11 | column12 | column13 | column14 | column15 | column16 |",
            "+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+",
            "| 1       | 2       | 3       | 4       | 5       | 6       | 7       | 8       | 9       | 10       | 11       | 12       | 13       |          | F        | 3.5      |",
            "+---------+---------+---------+---------+---------+---------+---------+---------+---------+----------+----------+----------+----------+----------+----------+----------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "SELECT * FROM (VALUES (1,'a'),(2,NULL)) AS t(c1, c2)";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----+----+",
            "| c1 | c2 |",
            "+----+----+",
            "| 1  | a  |",
            "| 2  |    |",
            "+----+----+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "EXPLAIN VALUES (1, 'a', -1, 1.1),(NULL, 'b', -3, 0.5)";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------------+-----------------------------------------------------------------------------------------------------------+",
            "| plan_type     | plan                                                                                                      |",
            "+---------------+-----------------------------------------------------------------------------------------------------------+",
            "| logical_plan  | Values: (Int64(1), Utf8(\"a\"), Int64(-1), Float64(1.1)), (Int64(NULL), Utf8(\"b\"), Int64(-3), Float64(0.5)) |",
            "| physical_plan | ValuesExec                                                                                                |",
            "|               |                                                                                                           |",
            "+---------------+-----------------------------------------------------------------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn select_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    let sql = "SELECT c1 FROM aggregate_simple order by c1";
    let results = execute_to_batches(&mut ctx, sql).await;

    let sql_all = "SELECT ALL c1 FROM aggregate_simple order by c1";
    let results_all = execute_to_batches(&mut ctx, sql_all).await;

    let expected = vec![
        "+---------+",
        "| c1      |",
        "+---------+",
        "| 0.00001 |",
        "| 0.00002 |",
        "| 0.00002 |",
        "| 0.00003 |",
        "| 0.00003 |",
        "| 0.00003 |",
        "| 0.00004 |",
        "| 0.00004 |",
        "| 0.00004 |",
        "| 0.00004 |",
        "| 0.00005 |",
        "| 0.00005 |",
        "| 0.00005 |",
        "| 0.00005 |",
        "| 0.00005 |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);
    assert_batches_eq!(expected, &results_all);

    Ok(())
}

#[tokio::test]
async fn create_table_as() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    let sql = "CREATE TABLE my_table AS SELECT * FROM aggregate_simple";
    ctx.sql(sql).await.unwrap();

    let sql_all = "SELECT * FROM my_table order by c1 LIMIT 1";
    let results_all = execute_to_batches(&mut ctx, sql_all).await;

    let expected = vec![
        "+---------+----------------+------+",
        "| c1      | c2             | c3   |",
        "+---------+----------------+------+",
        "| 0.00001 | 0.000000000001 | true |",
        "+---------+----------------+------+",
    ];

    assert_batches_eq!(expected, &results_all);

    Ok(())
}

#[tokio::test]
async fn drop_table() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    let sql = "CREATE TABLE my_table AS SELECT * FROM aggregate_simple";
    ctx.sql(sql).await.unwrap();

    let sql = "DROP TABLE my_table";
    ctx.sql(sql).await.unwrap();

    let result = ctx.table("my_table");
    assert!(result.is_err(), "drop table should deregister table.");

    let sql = "DROP TABLE IF EXISTS my_table";
    ctx.sql(sql).await.unwrap();

    Ok(())
}

#[tokio::test]
async fn select_distinct() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    let sql = "SELECT DISTINCT * FROM aggregate_simple";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();

    let mut dedup = actual.clone();
    dedup.dedup();

    assert_eq!(actual, dedup);

    Ok(())
}

#[tokio::test]
async fn select_distinct_simple_1() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await.unwrap();

    let sql = "SELECT DISTINCT c1 FROM aggregate_simple order by c1";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c1      |",
        "+---------+",
        "| 0.00001 |",
        "| 0.00002 |",
        "| 0.00003 |",
        "| 0.00004 |",
        "| 0.00005 |",
        "+---------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn select_distinct_simple_2() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await.unwrap();

    let sql = "SELECT DISTINCT c1, c2 FROM aggregate_simple order by c1";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+---------+----------------+",
        "| c1      | c2             |",
        "+---------+----------------+",
        "| 0.00001 | 0.000000000001 |",
        "| 0.00002 | 0.000000000002 |",
        "| 0.00003 | 0.000000000003 |",
        "| 0.00004 | 0.000000000004 |",
        "| 0.00005 | 0.000000000005 |",
        "+---------+----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn select_distinct_simple_3() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await.unwrap();

    let sql = "SELECT distinct c3 FROM aggregate_simple order by c3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------+",
        "| c3    |",
        "+-------+",
        "| false |",
        "| true  |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn select_distinct_simple_4() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await.unwrap();

    let sql = "SELECT distinct c1+c2 as a FROM aggregate_simple";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------------------------+",
        "| a                       |",
        "+-------------------------+",
        "| 0.000030000002242136256 |",
        "| 0.000040000002989515004 |",
        "| 0.000010000000747378751 |",
        "| 0.00005000000373689376  |",
        "| 0.000020000001494757502 |",
        "+-------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
}

#[tokio::test]
async fn select_distinct_from() {
    let mut ctx = ExecutionContext::new();

    let sql = "select
        1 IS DISTINCT FROM CAST(NULL as INT) as a,
        1 IS DISTINCT FROM 1 as b,
        1 IS NOT DISTINCT FROM CAST(NULL as INT) as c,
        1 IS NOT DISTINCT FROM 1 as d,
        NULL IS DISTINCT FROM NULL as e,
        NULL IS NOT DISTINCT FROM NULL as f
    ";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------+-------+-------+------+-------+------+",
        "| a    | b     | c     | d    | e     | f    |",
        "+------+-------+-------+------+-------+------+",
        "| true | false | false | true | false | true |",
        "+------+-------+-------+------+-------+------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn select_distinct_from_utf8() {
    let mut ctx = ExecutionContext::new();

    let sql = "select
        'x' IS DISTINCT FROM NULL as a,
        'x' IS DISTINCT FROM 'x' as b,
        'x' IS NOT DISTINCT FROM NULL as c,
        'x' IS NOT DISTINCT FROM 'x' as d
    ";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------+-------+-------+------+",
        "| a    | b     | c     | d    |",
        "+------+-------+-------+------+",
        "| true | false | false | true |",
        "+------+-------+-------+------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn projection_same_fields() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let sql = "select (1+1) as a from (select 1 as a) as b;";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec!["+---+", "| a |", "+---+", "| 2 |", "+---+"];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn projection_type_alias() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    // Query that aliases one column to the name of a different column
    // that also has a different type (c1 == float32, c3 == boolean)
    let sql = "SELECT c1 as c3 FROM aggregate_simple ORDER BY c3 LIMIT 2";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c3      |",
        "+---------+",
        "| 0.00001 |",
        "| 0.00002 |",
        "+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_float64() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    let sql =
        "SELECT COUNT(*) as cnt, c2 FROM aggregate_simple GROUP BY c2 ORDER BY cnt DESC";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----+----------------+",
        "| cnt | c2             |",
        "+-----+----------------+",
        "| 5   | 0.000000000005 |",
        "| 4   | 0.000000000004 |",
        "| 3   | 0.000000000003 |",
        "| 2   | 0.000000000002 |",
        "| 1   | 0.000000000001 |",
        "+-----+----------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_boolean() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    let sql =
        "SELECT COUNT(*) as cnt, c3 FROM aggregate_simple GROUP BY c3 ORDER BY cnt DESC";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----+-------+",
        "| cnt | c3    |",
        "+-----+-------+",
        "| 9   | true  |",
        "| 6   | false |",
        "+-----+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_two_columns() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, c2, MIN(c3) FROM aggregate_test_100 GROUP BY c1, c2";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+----+----------------------------+",
        "| c1 | c2 | MIN(aggregate_test_100.c3) |",
        "+----+----+----------------------------+",
        "| a  | 1  | -85                        |",
        "| a  | 2  | -48                        |",
        "| a  | 3  | -72                        |",
        "| a  | 4  | -101                       |",
        "| a  | 5  | -101                       |",
        "| b  | 1  | 12                         |",
        "| b  | 2  | -60                        |",
        "| b  | 3  | -101                       |",
        "| b  | 4  | -117                       |",
        "| b  | 5  | -82                        |",
        "| c  | 1  | -24                        |",
        "| c  | 2  | -117                       |",
        "| c  | 3  | -2                         |",
        "| c  | 4  | -90                        |",
        "| c  | 5  | -94                        |",
        "| d  | 1  | -99                        |",
        "| d  | 2  | 93                         |",
        "| d  | 3  | -76                        |",
        "| d  | 4  | 5                          |",
        "| d  | 5  | -59                        |",
        "| e  | 1  | 36                         |",
        "| e  | 2  | -61                        |",
        "| e  | 3  | -95                        |",
        "| e  | 4  | -56                        |",
        "| e  | 5  | -86                        |",
        "+----+----+----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_and_having() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, MIN(c3) AS m FROM aggregate_test_100 GROUP BY c1 HAVING m < -100 AND MAX(c3) > 70";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+------+",
        "| c1 | m    |",
        "+----+------+",
        "| a  | -101 |",
        "| c  | -117 |",
        "+----+------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_and_having_and_where() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, MIN(c3) AS m
               FROM aggregate_test_100
               WHERE c1 IN ('a', 'b')
               GROUP BY c1
               HAVING m < -100 AND MAX(c3) > 70";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+------+",
        "| c1 | m    |",
        "+----+------+",
        "| a  | -101 |",
        "+----+------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn all_where_empty() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT *
               FROM aggregate_test_100
               WHERE 1=2";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_having_without_group_by() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, c2, c3 FROM aggregate_test_100 HAVING c2 >= 4 AND c3 > 90";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+----+-----+",
        "| c1 | c2 | c3  |",
        "+----+----+-----+",
        "| c  | 4  | 123 |",
        "| c  | 5  | 118 |",
        "| d  | 4  | 102 |",
        "| e  | 4  | 96  |",
        "| e  | 4  | 97  |",
        "+----+----+-----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_boolean_eq_neq() {
    let mut ctx = ExecutionContext::new();
    register_boolean(&mut ctx).await.unwrap();
    // verify the plumbing is all hooked up for eq and neq
    let sql = "SELECT a, b, a = b as eq, b = true as eq_scalar, a != b as neq, a != true as neq_scalar FROM t1";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------+-------+-------+-----------+-------+------------+",
        "| a     | b     | eq    | eq_scalar | neq   | neq_scalar |",
        "+-------+-------+-------+-----------+-------+------------+",
        "| true  | true  | true  | true      | false | false      |",
        "| true  |       |       |           |       | false      |",
        "| true  | false | false | false     | true  | false      |",
        "|       | true  |       | true      |       |            |",
        "|       |       |       |           |       |            |",
        "|       | false |       | false     |       |            |",
        "| false | true  | false | true      | true  | true       |",
        "| false |       |       |           |       | true       |",
        "| false | false | true  | false     | false | true       |",
        "+-------+-------+-------+-----------+-------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_boolean_lt_lt_eq() {
    let mut ctx = ExecutionContext::new();
    register_boolean(&mut ctx).await.unwrap();
    // verify the plumbing is all hooked up for < and <=
    let sql = "SELECT a, b, a < b as lt, b = true as lt_scalar, a <= b as lt_eq, a <= true as lt_eq_scalar FROM t1";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------+-------+-------+-----------+-------+--------------+",
        "| a     | b     | lt    | lt_scalar | lt_eq | lt_eq_scalar |",
        "+-------+-------+-------+-----------+-------+--------------+",
        "| true  | true  | false | true      | true  | true         |",
        "| true  |       |       |           |       | true         |",
        "| true  | false | false | false     | false | true         |",
        "|       | true  |       | true      |       |              |",
        "|       |       |       |           |       |              |",
        "|       | false |       | false     |       |              |",
        "| false | true  | true  | true      | true  | true         |",
        "| false |       |       |           |       | true         |",
        "| false | false | false | false     | true  | true         |",
        "+-------+-------+-------+-----------+-------+--------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_boolean_gt_gt_eq() {
    let mut ctx = ExecutionContext::new();
    register_boolean(&mut ctx).await.unwrap();
    // verify the plumbing is all hooked up for > and >=
    let sql = "SELECT a, b, a > b as gt, b = true as gt_scalar, a >= b as gt_eq, a >= true as gt_eq_scalar FROM t1";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------+-------+-------+-----------+-------+--------------+",
        "| a     | b     | gt    | gt_scalar | gt_eq | gt_eq_scalar |",
        "+-------+-------+-------+-----------+-------+--------------+",
        "| true  | true  | false | true      | true  | true         |",
        "| true  |       |       |           |       | true         |",
        "| true  | false | true  | false     | true  | true         |",
        "|       | true  |       | true      |       |              |",
        "|       |       |       |           |       |              |",
        "|       | false |       | false     |       |              |",
        "| false | true  | false | true      | false | false        |",
        "| false |       |       |           |       | false        |",
        "| false | false | false | false     | true  | false        |",
        "+-------+-------+-------+-----------+-------+--------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_boolean_distinct_from() {
    let mut ctx = ExecutionContext::new();
    register_boolean(&mut ctx).await.unwrap();
    // verify the plumbing is all hooked up for is distinct from and is not distinct from
    let sql = "SELECT a, b, \
               a is distinct from b as df, \
               b is distinct from true as df_scalar, \
               a is not distinct from b as ndf, \
               a is not distinct from true as ndf_scalar \
               FROM t1";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------+-------+-------+-----------+-------+------------+",
        "| a     | b     | df    | df_scalar | ndf   | ndf_scalar |",
        "+-------+-------+-------+-----------+-------+------------+",
        "| true  | true  | false | false     | true  | true       |",
        "| true  |       | true  | true      | false | true       |",
        "| true  | false | true  | true      | false | true       |",
        "|       | true  | true  | false     | false | false      |",
        "|       |       | false | true      | true  | false      |",
        "|       | false | true  | true      | false | false      |",
        "| false | true  | true  | false     | false | false      |",
        "| false |       | true  | true      | false | false      |",
        "| false | false | false | true      | true  | false      |",
        "+-------+-------+-------+-----------+-------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_avg_sqrt() -> Result<()> {
    let mut ctx = create_ctx()?;
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(custom_sqrt(c12)) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.6706002946036462"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

/// test that casting happens on udfs.
/// c11 is f32, but `custom_sqrt` requires f64. Casting happens but the logical plan and
/// physical plan have the same schema.
#[tokio::test]
async fn csv_query_custom_udf_with_cast() -> Result<()> {
    let mut ctx = create_ctx()?;
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(custom_sqrt(c11)) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["0.6584408483418833"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

/// sqrt(f32) is slightly different than sqrt(CAST(f32 AS double)))
#[tokio::test]
async fn sqrt_f32_vs_f64() -> Result<()> {
    let mut ctx = create_ctx()?;
    register_aggregate_csv(&mut ctx).await?;
    // sqrt(f32)'s plan passes
    let sql = "SELECT avg(sqrt(c11)) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["0.6584407806396484"]];

    assert_eq!(actual, expected);
    let sql = "SELECT avg(sqrt(CAST(c11 AS double))) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["0.6584408483418833"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

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

// this query used to deadlock due to the call udf(udf())
#[tokio::test]
async fn csv_query_sqrt_sqrt() -> Result<()> {
    let mut ctx = create_ctx()?;
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT sqrt(sqrt(c12)) FROM aggregate_test_100 LIMIT 1";
    let actual = execute(&mut ctx, sql).await;
    // sqrt(sqrt(c12=0.9294097332465232)) = 0.9818650561397431
    let expected = vec![vec!["0.9818650561397431"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn create_ctx() -> Result<ExecutionContext> {
    let mut ctx = ExecutionContext::new();

    // register a custom UDF
    ctx.register_udf(create_udf(
        "custom_sqrt",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(custom_sqrt),
    ));

    Ok(ctx)
}

fn custom_sqrt(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arg = &args[0];
    if let ColumnarValue::Array(v) = arg {
        let input = v
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast failed");

        let array: Float64Array = input.iter().map(|v| v.map(|x| x.sqrt())).collect();
        Ok(ColumnarValue::Array(Arc::new(array)))
    } else {
        unimplemented!()
    }
}

#[tokio::test]
async fn csv_query_avg() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let mut actual = execute(&mut ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.5089725099127211"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_avg() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, avg(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+",
        "| c1 | AVG(aggregate_test_100.c12) |",
        "+----+-----------------------------+",
        "| a  | 0.48754517466109415         |",
        "| b  | 0.41040709263815384         |",
        "| c  | 0.6600456536439784          |",
        "| d  | 0.48855379387549824         |",
        "| e  | 0.48600669271341534         |",
        "+----+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_avg_with_projection() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(c12), c1 FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------------------+----+",
        "| AVG(aggregate_test_100.c12) | c1 |",
        "+-----------------------------+----+",
        "| 0.41040709263815384         | b  |",
        "| 0.48600669271341534         | e  |",
        "| 0.48754517466109415         | a  |",
        "| 0.48855379387549824         | d  |",
        "| 0.6600456536439784          | c  |",
        "+-----------------------------+----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_avg_multi_batch() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT avg(c12) FROM aggregate_test_100";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let results = collect(plan).await.unwrap();
    let batch = &results[0];
    let column = batch.column(0);
    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    let actual = array.value(0);
    let expected = 0.5089725;
    // Due to float number's accuracy, different batch size will lead to different
    // answers.
    assert!((expected - actual).abs() < 0.01);
    Ok(())
}

#[tokio::test]
async fn csv_query_nullif_divide_by_0() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c8/nullif(c7, 0) FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    let actual = &actual[80..90]; // We just want to compare rows 80-89
    let expected = vec![
        vec!["258"],
        vec!["664"],
        vec!["NULL"],
        vec!["22"],
        vec!["164"],
        vec!["448"],
        vec!["365"],
        vec!["1640"],
        vec!["671"],
        vec!["203"],
    ];
    assert_eq!(expected, actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_count() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT count(c12) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| COUNT(aggregate_test_100.c12) |",
        "+-------------------------------+",
        "| 100                           |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_approx_count() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT approx_distinct(c9) count_c9, approx_distinct(cast(c9 as varchar)) count_c9_str FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------+--------------+",
        "| count_c9 | count_c9_str |",
        "+----------+--------------+",
        "| 100      | 99           |",
        "+----------+--------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_count_without_from() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT count(1 + 1)";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| COUNT(Int64(1) + Int64(1)) |",
        "+----------------------------+",
        "| 1                          |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 2) test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------------------------------------------------------------------+",
        "| ARRAYAGG(test.c13)                                               |",
        "+------------------------------------------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm, 0keZ5G8BffGwgF2RwQD59TFzMStxCB] |",
        "+------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_empty() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 LIMIT 0) test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------+",
        "| ARRAYAGG(test.c13) |",
        "+--------------------+",
        "| []                 |",
        "+--------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_array_agg_one() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT array_agg(c13) FROM (SELECT * FROM aggregate_test_100 ORDER BY c13 LIMIT 1) test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------------+",
        "| ARRAYAGG(test.c13)               |",
        "+----------------------------------+",
        "| [0VVIHzxWtNOFLtnhjHEKjXaJOSLJfm] |",
        "+----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

/// for window functions without order by the first, last, and nth function call does not make sense
#[tokio::test]
async fn csv_query_window_with_empty_over() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "select \
               c9, \
               count(c5) over (), \
               max(c5) over (), \
               min(c5) over () \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------+------------------------------+----------------------------+----------------------------+",
        "| c9        | COUNT(aggregate_test_100.c5) | MAX(aggregate_test_100.c5) | MIN(aggregate_test_100.c5) |",
        "+-----------+------------------------------+----------------------------+----------------------------+",
        "| 28774375  | 100                          | 2143473091                 | -2141999138                |",
        "| 63044568  | 100                          | 2143473091                 | -2141999138                |",
        "| 141047417 | 100                          | 2143473091                 | -2141999138                |",
        "| 141680161 | 100                          | 2143473091                 | -2141999138                |",
        "| 145294611 | 100                          | 2143473091                 | -2141999138                |",
        "+-----------+------------------------------+----------------------------+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

/// for window functions without order by the first, last, and nth function call does not make sense
#[tokio::test]
async fn csv_query_window_with_partition_by() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "select \
               c9, \
               sum(cast(c4 as Int)) over (partition by c3), \
               avg(cast(c4 as Int)) over (partition by c3), \
               count(cast(c4 as Int)) over (partition by c3), \
               max(cast(c4 as Int)) over (partition by c3), \
               min(cast(c4 as Int)) over (partition by c3) \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------+-------------------------------------------+-------------------------------------------+---------------------------------------------+-------------------------------------------+-------------------------------------------+",
        "| c9        | SUM(CAST(aggregate_test_100.c4 AS Int32)) | AVG(CAST(aggregate_test_100.c4 AS Int32)) | COUNT(CAST(aggregate_test_100.c4 AS Int32)) | MAX(CAST(aggregate_test_100.c4 AS Int32)) | MIN(CAST(aggregate_test_100.c4 AS Int32)) |",
        "+-----------+-------------------------------------------+-------------------------------------------+---------------------------------------------+-------------------------------------------+-------------------------------------------+",
        "| 28774375  | -16110                                    | -16110                                    | 1                                           | -16110                                    | -16110                                    |",
        "| 63044568  | 3917                                      | 3917                                      | 1                                           | 3917                                      | 3917                                      |",
        "| 141047417 | -38455                                    | -19227.5                                  | 2                                           | -16974                                    | -21481                                    |",
        "| 141680161 | -1114                                     | -1114                                     | 1                                           | -1114                                     | -1114                                     |",
        "| 145294611 | 15673                                     | 15673                                     | 1                                           | 15673                                     | 15673                                     |",
        "+-----------+-------------------------------------------+-------------------------------------------+---------------------------------------------+-------------------------------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_window_with_order_by() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "select \
               c9, \
               sum(c5) over (order by c9), \
               avg(c5) over (order by c9), \
               count(c5) over (order by c9), \
               max(c5) over (order by c9), \
               min(c5) over (order by c9), \
               first_value(c5) over (order by c9), \
               last_value(c5) over (order by c9), \
               nth_value(c5, 2) over (order by c9) \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
        "| c9        | SUM(aggregate_test_100.c5) | AVG(aggregate_test_100.c5) | COUNT(aggregate_test_100.c5) | MAX(aggregate_test_100.c5) | MIN(aggregate_test_100.c5) | FIRST_VALUE(aggregate_test_100.c5) | LAST_VALUE(aggregate_test_100.c5) | NTH_VALUE(aggregate_test_100.c5,Int64(2)) |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
        "| 28774375  | 61035129                   | 61035129                   | 1                            | 61035129                   | 61035129                   | 61035129                           | 61035129                          |                                           |",
        "| 63044568  | -47938237                  | -23969118.5                | 2                            | 61035129                   | -108973366                 | 61035129                           | -108973366                        | -108973366                                |",
        "| 141047417 | 575165281                  | 191721760.33333334         | 3                            | 623103518                  | -108973366                 | 61035129                           | 623103518                         | -108973366                                |",
        "| 141680161 | -1352462829                | -338115707.25              | 4                            | 623103518                  | -1927628110                | 61035129                           | -1927628110                       | -108973366                                |",
        "| 145294611 | -3251637940                | -650327588                 | 5                            | 623103518                  | -1927628110                | 61035129                           | -1899175111                       | -108973366                                |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_window_with_partition_by_order_by() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "select \
               c9, \
               sum(c5) over (partition by c4 order by c9), \
               avg(c5) over (partition by c4 order by c9), \
               count(c5) over (partition by c4 order by c9), \
               max(c5) over (partition by c4 order by c9), \
               min(c5) over (partition by c4 order by c9), \
               first_value(c5) over (partition by c4 order by c9), \
               last_value(c5) over (partition by c4 order by c9), \
               nth_value(c5, 2) over (partition by c4 order by c9) \
               from aggregate_test_100 \
               order by c9 \
               limit 5";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
        "| c9        | SUM(aggregate_test_100.c5) | AVG(aggregate_test_100.c5) | COUNT(aggregate_test_100.c5) | MAX(aggregate_test_100.c5) | MIN(aggregate_test_100.c5) | FIRST_VALUE(aggregate_test_100.c5) | LAST_VALUE(aggregate_test_100.c5) | NTH_VALUE(aggregate_test_100.c5,Int64(2)) |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+",
        "| 28774375  | 61035129                   | 61035129                   | 1                            | 61035129                   | 61035129                   | 61035129                           | 61035129                          |                                           |",
        "| 63044568  | -108973366                 | -108973366                 | 1                            | -108973366                 | -108973366                 | -108973366                         | -108973366                        |                                           |",
        "| 141047417 | 623103518                  | 623103518                  | 1                            | 623103518                  | 623103518                  | 623103518                          | 623103518                         |                                           |",
        "| 141680161 | -1927628110                | -1927628110                | 1                            | -1927628110                | -1927628110                | -1927628110                        | -1927628110                       |                                           |",
        "| 145294611 | -1899175111                | -1899175111                | 1                            | -1899175111                | -1899175111                | -1899175111                        | -1899175111                       |                                           |",
        "+-----------+----------------------------+----------------------------+------------------------------+----------------------------+----------------------------+------------------------------------+-----------------------------------+-------------------------------------------+"
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_int_count() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, count(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-------------------------------+",
        "| c1 | COUNT(aggregate_test_100.c12) |",
        "+----+-------------------------------+",
        "| a  | 21                            |",
        "| b  | 19                            |",
        "| c  | 21                            |",
        "| d  | 18                            |",
        "| e  | 21                            |",
        "+----+-------------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_with_aliased_aggregate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, count(c12) AS count FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-------+",
        "| c1 | count |",
        "+----+-------+",
        "| a  | 21    |",
        "| b  | 19    |",
        "| c  | 21    |",
        "| d  | 18    |",
        "| e  | 21    |",
        "+----+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_string_min_max() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1, MIN(c12), MAX(c12) FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------+-----------------------------+",
        "| c1 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) |",
        "+----+-----------------------------+-----------------------------+",
        "| a  | 0.02182578039211991         | 0.9800193410444061          |",
        "| b  | 0.04893135681998029         | 0.9185813970744787          |",
        "| c  | 0.0494924465469434          | 0.991517828651004           |",
        "| d  | 0.061029375346466685        | 0.9748360509016578          |",
        "| e  | 0.01479305307777301         | 0.9965400387585364          |",
        "+----+-----------------------------+-----------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_cast() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT CAST(c12 AS float) FROM aggregate_test_100 WHERE c12 > 0.376 AND c12 < 0.4";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----------------------------------------+",
        "| CAST(aggregate_test_100.c12 AS Float32) |",
        "+-----------------------------------------+",
        "| 0.39144436                              |",
        "| 0.3887028                               |",
        "+-----------------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_cast_literal() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT c12, CAST(1 AS float) FROM aggregate_test_100 WHERE c12 > CAST(0 AS float) LIMIT 2";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+--------------------+---------------------------+",
        "| c12                | CAST(Int64(1) AS Float32) |",
        "+--------------------+---------------------------+",
        "| 0.9294097332465232 | 1                         |",
        "| 0.3114712539863804 | 1                         |",
        "+--------------------+---------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_millis() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            1235865600000,
            1235865660000,
            1238544000000,
        ]))],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let sql = "SELECT to_timestamp_millis(ts) FROM t1 LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+--------------------------+",
        "| totimestampmillis(t1.ts) |",
        "+--------------------------+",
        "| 2009-03-01 00:00:00      |",
        "| 2009-03-01 00:01:00      |",
        "| 2009-04-01 00:00:00      |",
        "+--------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_micros() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            1235865600000000,
            1235865660000000,
            1238544000000000,
        ]))],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let sql = "SELECT to_timestamp_micros(ts) FROM t1 LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+--------------------------+",
        "| totimestampmicros(t1.ts) |",
        "+--------------------------+",
        "| 2009-03-01 00:00:00      |",
        "| 2009-03-01 00:01:00      |",
        "| 2009-04-01 00:00:00      |",
        "+--------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_seconds() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![
            1235865600, 1235865660, 1238544000,
        ]))],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let sql = "SELECT to_timestamp_seconds(ts) FROM t1 LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+---------------------------+",
        "| totimestampseconds(t1.ts) |",
        "+---------------------------+",
        "| 2009-03-01 00:00:00       |",
        "| 2009-03-01 00:01:00       |",
        "| 2009-04-01 00:00:00       |",
        "+---------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_nanos_to_others() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    // Original column is nanos, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_data LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------------------------------+",
        "| totimestampmillis(ts_data.ts) |",
        "+-------------------------------+",
        "| 2020-09-08 13:42:29.190       |",
        "| 2020-09-08 12:42:29.190       |",
        "| 2020-09-08 11:42:29.190       |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT to_timestamp_micros(ts) FROM ts_data LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------------------------------+",
        "| totimestampmicros(ts_data.ts) |",
        "+-------------------------------+",
        "| 2020-09-08 13:42:29.190855    |",
        "| 2020-09-08 12:42:29.190855    |",
        "| 2020-09-08 11:42:29.190855    |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT to_timestamp_seconds(ts) FROM ts_data LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------------------+",
        "| totimestampseconds(ts_data.ts) |",
        "+--------------------------------+",
        "| 2020-09-08 13:42:29            |",
        "| 2020-09-08 12:42:29            |",
        "| 2020-09-08 11:42:29            |",
        "+--------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_seconds_to_others() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_secs", make_timestamp_table::<TimestampSecondType>()?)?;

    // Original column is seconds, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_secs LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| totimestampmillis(ts_secs.ts) |",
        "+-------------------------------+",
        "| 2020-09-08 13:42:29           |",
        "| 2020-09-08 12:42:29           |",
        "| 2020-09-08 11:42:29           |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);

    // Original column is seconds, convert to micros and check timestamp
    let sql = "SELECT to_timestamp_micros(ts) FROM ts_secs LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| totimestampmicros(ts_secs.ts) |",
        "+-------------------------------+",
        "| 2020-09-08 13:42:29           |",
        "| 2020-09-08 12:42:29           |",
        "| 2020-09-08 11:42:29           |",
        "+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // to nanos
    let sql = "SELECT to_timestamp(ts) FROM ts_secs LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------+",
        "| totimestamp(ts_secs.ts) |",
        "+-------------------------+",
        "| 2020-09-08 13:42:29     |",
        "| 2020-09-08 12:42:29     |",
        "| 2020-09-08 11:42:29     |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cast_timestamp_micros_to_others() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "ts_micros",
        make_timestamp_table::<TimestampMicrosecondType>()?,
    )?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT to_timestamp_millis(ts) FROM ts_micros LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------------------------------+",
        "| totimestampmillis(ts_micros.ts) |",
        "+---------------------------------+",
        "| 2020-09-08 13:42:29.190         |",
        "| 2020-09-08 12:42:29.190         |",
        "| 2020-09-08 11:42:29.190         |",
        "+---------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Original column is micros, convert to seconds and check timestamp
    let sql = "SELECT to_timestamp_seconds(ts) FROM ts_micros LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------------+",
        "| totimestampseconds(ts_micros.ts) |",
        "+----------------------------------+",
        "| 2020-09-08 13:42:29              |",
        "| 2020-09-08 12:42:29              |",
        "| 2020-09-08 11:42:29              |",
        "+----------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Original column is micros, convert to nanos and check timestamp
    let sql = "SELECT to_timestamp(ts) FROM ts_micros LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------+",
        "| totimestamp(ts_micros.ts)  |",
        "+----------------------------+",
        "| 2020-09-08 13:42:29.190855 |",
        "| 2020-09-08 12:42:29.190855 |",
        "| 2020-09-08 11:42:29.190855 |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn union_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT 1 as x UNION ALL SELECT 2 as x";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+---+", "| x |", "+---+", "| 1 |", "| 2 |", "+---+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_union_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql =
        "SELECT c1 FROM aggregate_test_100 UNION ALL SELECT c1 FROM aggregate_test_100";
    let actual = execute(&mut ctx, sql).await;
    assert_eq!(actual.len(), 200);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 2";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+----+", "| c1 |", "+----+", "| c  |", "| d  |", "+----+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_bigger_than_nbr_of_rows() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 200";
    let actual = execute_to_batches(&mut ctx, sql).await;
    // println!("{}", pretty_format_batches(&a).unwrap());
    let expected = vec![
        "+----+", "| c2 |", "+----+", "| 2  |", "| 5  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 3  |", "| 3  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 3  |",
        "| 2  |", "| 1  |", "| 1  |", "| 2  |", "| 1  |", "| 3  |", "| 2  |", "| 4  |",
        "| 1  |", "| 5  |", "| 4  |", "| 2  |", "| 1  |", "| 4  |", "| 5  |", "| 2  |",
        "| 3  |", "| 4  |", "| 2  |", "| 1  |", "| 5  |", "| 3  |", "| 1  |", "| 2  |",
        "| 3  |", "| 3  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |", "| 2  |",
        "| 5  |", "| 2  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 2  |", "| 5  |",
        "| 4  |", "| 2  |", "| 3  |", "| 4  |", "| 4  |", "| 4  |", "| 5  |", "| 4  |",
        "| 2  |", "| 1  |", "| 2  |", "| 4  |", "| 2  |", "| 3  |", "| 5  |", "| 1  |",
        "| 1  |", "| 4  |", "| 2  |", "| 1  |", "| 2  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 5  |", "| 2  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |",
        "| 4  |", "| 3  |", "| 2  |", "| 5  |", "| 3  |", "| 3  |", "| 2  |", "| 5  |",
        "| 5  |", "| 4  |", "| 1  |", "| 3  |", "| 3  |", "| 4  |", "| 4  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_with_same_nbr_of_rows() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+", "| c2 |", "+----+", "| 2  |", "| 5  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 3  |", "| 3  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 3  |",
        "| 2  |", "| 1  |", "| 1  |", "| 2  |", "| 1  |", "| 3  |", "| 2  |", "| 4  |",
        "| 1  |", "| 5  |", "| 4  |", "| 2  |", "| 1  |", "| 4  |", "| 5  |", "| 2  |",
        "| 3  |", "| 4  |", "| 2  |", "| 1  |", "| 5  |", "| 3  |", "| 1  |", "| 2  |",
        "| 3  |", "| 3  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |", "| 2  |",
        "| 5  |", "| 2  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 2  |", "| 5  |",
        "| 4  |", "| 2  |", "| 3  |", "| 4  |", "| 4  |", "| 4  |", "| 5  |", "| 4  |",
        "| 2  |", "| 1  |", "| 2  |", "| 4  |", "| 2  |", "| 3  |", "| 5  |", "| 1  |",
        "| 1  |", "| 4  |", "| 2  |", "| 1  |", "| 2  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 5  |", "| 2  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |",
        "| 4  |", "| 3  |", "| 2  |", "| 5  |", "| 3  |", "| 3  |", "| 2  |", "| 5  |",
        "| 5  |", "| 4  |", "| 1  |", "| 3  |", "| 3  |", "| 4  |", "| 4  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_zero() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 0";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_create_external_table() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, 10, c11, c12, c13 FROM aggregate_test_100 LIMIT 1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+----+----+-------+------------+----------------------+----+-------+------------+-----------+-------------+--------------------+--------------------------------+",
        "| c1 | c2 | c3 | c4    | c5         | c6                   | c7 | c8    | c9         | Int64(10) | c11         | c12                | c13                            |",
        "+----+----+----+-------+------------+----------------------+----+-------+------------+-----------+-------------+--------------------+--------------------------------+",
        "| c  | 2  | 1  | 18109 | 2033001162 | -6513304855495910254 | 25 | 43062 | 1491205016 | 10        | 0.110830784 | 0.9294097332465232 | 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW |",
        "+----+----+----+-------+------------+----------------------+----+-------+------------+-----------+-------------+--------------------+--------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_external_table_count() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(c12) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------+",
        "| COUNT(aggregate_test_100.c12) |",
        "+-------------------------------+",
        "| 100                           |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_external_table_sum() {
    let mut ctx = ExecutionContext::new();
    // cast smallint and int to bigint to avoid overflow during calculation
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql =
        "SELECT SUM(CAST(c7 AS BIGINT)), SUM(CAST(c8 AS BIGINT)) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------------------+-------------------------------------------+",
        "| SUM(CAST(aggregate_test_100.c7 AS Int64)) | SUM(CAST(aggregate_test_100.c8 AS Int64)) |",
        "+-------------------------------------------+-------------------------------------------+",
        "| 13060                                     | 3017641                                   |",
        "+-------------------------------------------+-------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_count_star() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(*) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn csv_query_count_one() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(1) FROM aggregate_test_100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 100             |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn case_when() -> Result<()> {
    let mut ctx = create_case_context()?;
    let sql = "SELECT \
        CASE WHEN c1 = 'a' THEN 1 \
             WHEN c1 = 'b' THEN 2 \
             END \
        FROM t1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------------------------------------------------+",
        "| CASE WHEN #t1.c1 = Utf8(\"a\") THEN Int64(1) WHEN #t1.c1 = Utf8(\"b\") THEN Int64(2) END |",
        "+--------------------------------------------------------------------------------------+",
        "| 1                                                                                    |",
        "| 2                                                                                    |",
        "|                                                                                      |",
        "|                                                                                      |",
        "+--------------------------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn case_when_else() -> Result<()> {
    let mut ctx = create_case_context()?;
    let sql = "SELECT \
        CASE WHEN c1 = 'a' THEN 1 \
             WHEN c1 = 'b' THEN 2 \
             ELSE 999 END \
        FROM t1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------------------------------------------------------------------------------------------------------+",
        "| CASE WHEN #t1.c1 = Utf8(\"a\") THEN Int64(1) WHEN #t1.c1 = Utf8(\"b\") THEN Int64(2) ELSE Int64(999) END |",
        "+------------------------------------------------------------------------------------------------------+",
        "| 1                                                                                                    |",
        "| 2                                                                                                    |",
        "| 999                                                                                                  |",
        "| 999                                                                                                  |",
        "+------------------------------------------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn case_when_with_base_expr() -> Result<()> {
    let mut ctx = create_case_context()?;
    let sql = "SELECT \
        CASE c1 WHEN 'a' THEN 1 \
             WHEN 'b' THEN 2 \
             END \
        FROM t1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------------------------------------------------------------------------+",
        "| CASE #t1.c1 WHEN Utf8(\"a\") THEN Int64(1) WHEN Utf8(\"b\") THEN Int64(2) END |",
        "+---------------------------------------------------------------------------+",
        "| 1                                                                         |",
        "| 2                                                                         |",
        "|                                                                           |",
        "|                                                                           |",
        "+---------------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn case_when_else_with_base_expr() -> Result<()> {
    let mut ctx = create_case_context()?;
    let sql = "SELECT \
        CASE c1 WHEN 'a' THEN 1 \
             WHEN 'b' THEN 2 \
             ELSE 999 END \
        FROM t1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------------------------------------------------------------------------+",
        "| CASE #t1.c1 WHEN Utf8(\"a\") THEN Int64(1) WHEN Utf8(\"b\") THEN Int64(2) ELSE Int64(999) END |",
        "+-------------------------------------------------------------------------------------------+",
        "| 1                                                                                         |",
        "| 2                                                                                         |",
        "| 999                                                                                       |",
        "| 999                                                                                       |",
        "+-------------------------------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

fn create_case_context() -> Result<ExecutionContext> {
    let mut ctx = ExecutionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Utf8, true)]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            None,
        ]))],
    )?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    ctx.register_table("t1", Arc::new(table))?;
    Ok(ctx)
}

#[tokio::test]
async fn equijoin() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    let mut ctx = create_join_context_qualified()?;
    let equivalent_sql = [
        "SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t1.a = t2.a ORDER BY t1.a",
        "SELECT t1.a, t2.b FROM t1 INNER JOIN t2 ON t2.a = t1.a ORDER BY t1.a",
    ];
    let expected = vec![
        "+---+-----+",
        "| a | b   |",
        "+---+-----+",
        "| 1 | 100 |",
        "| 2 | 200 |",
        "| 4 | 400 |",
        "+---+-----+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_multiple_condition_ordering() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t1_name <> t2_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t2_name <> t1_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id AND t1_name <> t2_name ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t2_id = t1_id AND t2_name <> t1_name ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_and_other_condition() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 JOIN t2 ON t1_id = t2_id AND t2_name >= 'y' ORDER BY t1_id";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_left_and_condition_from_right() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id AND t2_name >= 'y' ORDER BY t1_id";
    let res = ctx.create_logical_plan(sql);
    assert!(res.is_ok());
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       |         |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn equijoin_right_and_condition_from_left() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t1_id = t2_id AND t1_id >= 22 ORDER BY t2_name";
    let res = ctx.create_logical_plan(sql);
    assert!(res.is_ok());
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "|       |         | w       |",
        "| 44    | d       | x       |",
        "| 22    | b       | y       |",
        "|       |         | z       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_and_unsupported_condition() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id AND t1_id >= '44' ORDER BY t1_id";
    let res = ctx.create_logical_plan(sql);

    assert!(res.is_err());
    assert_eq!(format!("{}", res.unwrap_err()), "This feature is not implemented: Unsupported expressions in Left JOIN: [#t1_id >= Utf8(\"44\")]");

    Ok(())
}

#[tokio::test]
async fn left_join() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn left_join_unbalanced() -> Result<()> {
    // the t1_id is larger than t2_id so the hash_build_probe_order optimizer should kick in
    let mut ctx = create_join_context_unbalanced("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 LEFT JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "| 77    | e       |         |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn right_join() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 RIGHT JOIN t2 ON t2_id = t1_id ORDER BY t1_id"
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "|       |         | w       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn full_join() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 33    | c       |         |",
        "| 44    | d       | x       |",
        "|       |         | w       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL OUTER JOIN t2 ON t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1 FULL OUTER JOIN t2 ON t2_id = t1_id ORDER BY t1_id",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}

#[tokio::test]
async fn left_join_using() -> Result<()> {
    let mut ctx = create_join_context("id", "id")?;
    let sql = "SELECT id, t1_name, t2_name FROM t1 LEFT JOIN t2 USING (id) ORDER BY id";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+---------+---------+",
        "| id | t1_name | t2_name |",
        "+----+---------+---------+",
        "| 11 | a       | z       |",
        "| 22 | b       | y       |",
        "| 33 | c       |         |",
        "| 44 | d       | x       |",
        "+----+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let equivalent_sql = [
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t1_id = t2_id ORDER BY t1_id",
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t2_id = t1_id ORDER BY t1_id",
    ];
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    for sql in equivalent_sql.iter() {
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax_with_filter() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let sql = "SELECT t1_id, t1_name, t2_name \
        FROM t1, t2 \
        WHERE t1_id > 0 \
        AND t1_id = t2_id \
        AND t2_id < 99 \
        ORDER BY t1_id";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn equijoin_implicit_syntax_reversed() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE t2_id = t1_id ORDER BY t1_id";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 22    | b       | y       |",
        "| 44    | d       | x       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn cross_join() {
    let mut ctx = create_join_context("t1_id", "t2_id").unwrap();

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1, t2 ORDER BY t1_id";
    let actual = execute(&mut ctx, sql).await;

    assert_eq!(4 * 4, actual.len());

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1, t2 WHERE 1=1 ORDER BY t1_id";
    let actual = execute(&mut ctx, sql).await;

    assert_eq!(4 * 4, actual.len());

    let sql = "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN t2";

    let actual = execute(&mut ctx, sql).await;
    assert_eq!(4 * 4, actual.len());

    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 11    | a       | y       |",
        "| 11    | a       | x       |",
        "| 11    | a       | w       |",
        "| 22    | b       | z       |",
        "| 22    | b       | y       |",
        "| 22    | b       | x       |",
        "| 22    | b       | w       |",
        "| 33    | c       | z       |",
        "| 33    | c       | y       |",
        "| 33    | c       | x       |",
        "| 33    | c       | w       |",
        "| 44    | d       | z       |",
        "| 44    | d       | y       |",
        "| 44    | d       | x       |",
        "| 44    | d       | w       |",
        "+-------+---------+---------+",
    ];

    assert_batches_eq!(expected, &actual);

    // Two partitions (from UNION) on the left
    let sql = "SELECT * FROM (SELECT t1_id, t1_name FROM t1 UNION ALL SELECT t1_id, t1_name FROM t1) AS t1 CROSS JOIN t2";
    let actual = execute(&mut ctx, sql).await;

    assert_eq!(4 * 4 * 2, actual.len());

    // Two partitions (from UNION) on the right
    let sql = "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN (SELECT t2_name FROM t2 UNION ALL SELECT t2_name FROM t2) AS t2";
    let actual = execute(&mut ctx, sql).await;

    assert_eq!(4 * 4 * 2, actual.len());
}

#[tokio::test]
async fn cross_join_unbalanced() {
    // the t1_id is larger than t2_id so the hash_build_probe_order optimizer should kick in
    let mut ctx = create_join_context_unbalanced("t1_id", "t2_id").unwrap();

    // the order of the values is not determinisitic, so we need to sort to check the values
    let sql =
        "SELECT t1_id, t1_name, t2_name FROM t1 CROSS JOIN t2 ORDER BY t1_id, t1_name";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+---------+---------+",
        "| t1_id | t1_name | t2_name |",
        "+-------+---------+---------+",
        "| 11    | a       | z       |",
        "| 11    | a       | y       |",
        "| 11    | a       | x       |",
        "| 11    | a       | w       |",
        "| 22    | b       | z       |",
        "| 22    | b       | y       |",
        "| 22    | b       | x       |",
        "| 22    | b       | w       |",
        "| 33    | c       | z       |",
        "| 33    | c       | y       |",
        "| 33    | c       | x       |",
        "| 33    | c       | w       |",
        "| 44    | d       | z       |",
        "| 44    | d       | y       |",
        "| 44    | d       | x       |",
        "| 44    | d       | w       |",
        "| 77    | e       | z       |",
        "| 77    | e       | y       |",
        "| 77    | e       | x       |",
        "| 77    | e       | w       |",
        "+-------+---------+---------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_join_timestamp() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    // register time table
    let timestamp_schema = Arc::new(Schema::new(vec![Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let timestamp_data = RecordBatch::try_new(
        timestamp_schema.clone(),
        vec![Arc::new(TimestampNanosecondArray::from(vec![
            131964190213133,
            131964190213134,
            131964190213135,
        ]))],
    )?;
    let timestamp_table =
        MemTable::try_new(timestamp_schema, vec![vec![timestamp_data]])?;
    ctx.register_table("timestamp", Arc::new(timestamp_table))?;

    let sql = "SELECT * \
                     FROM timestamp as a \
                     JOIN (SELECT * FROM timestamp) as b \
                     ON a.time = b.time \
                     ORDER BY a.time";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-------------------------------+-------------------------------+",
        "| time                          | time                          |",
        "+-------------------------------+-------------------------------+",
        "| 1970-01-02 12:39:24.190213133 | 1970-01-02 12:39:24.190213133 |",
        "| 1970-01-02 12:39:24.190213134 | 1970-01-02 12:39:24.190213134 |",
        "| 1970-01-02 12:39:24.190213135 | 1970-01-02 12:39:24.190213135 |",
        "+-------------------------------+-------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_join_float32() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    // register population table
    let population_schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("population", DataType::Float32, true),
    ]));
    let population_data = RecordBatch::try_new(
        population_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            Arc::new(Float32Array::from(vec![838.698, 1778.934, 626.443])),
        ],
    )?;
    let population_table =
        MemTable::try_new(population_schema, vec![vec![population_data]])?;
    ctx.register_table("population", Arc::new(population_table))?;

    let sql = "SELECT * \
                     FROM population as a \
                     JOIN (SELECT * FROM population) as b \
                     ON a.population = b.population \
                     ORDER BY a.population";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+------+------------+------+------------+",
        "| city | population | city | population |",
        "+------+------------+------+------------+",
        "| c    | 626.443    | c    | 626.443    |",
        "| a    | 838.698    | a    | 838.698    |",
        "| b    | 1778.934   | b    | 1778.934   |",
        "+------+------------+------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn test_join_float64() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    // register population table
    let population_schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("population", DataType::Float64, true),
    ]));
    let population_data = RecordBatch::try_new(
        population_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            Arc::new(Float64Array::from(vec![838.698, 1778.934, 626.443])),
        ],
    )?;
    let population_table =
        MemTable::try_new(population_schema, vec![vec![population_data]])?;
    ctx.register_table("population", Arc::new(population_table))?;

    let sql = "SELECT * \
                     FROM population as a \
                     JOIN (SELECT * FROM population) as b \
                     ON a.population = b.population \
                     ORDER BY a.population";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+------+------------+------+------------+",
        "| city | population | city | population |",
        "+------+------------+------+------------+",
        "| c    | 626.443    | c    | 626.443    |",
        "| a    | 838.698    | a    | 838.698    |",
        "| b    | 1778.934   | b    | 1778.934   |",
        "+------+------------+------+------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

fn create_join_context(
    column_left: &str,
    column_right: &str,
) -> Result<ExecutionContext> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![11, 22, 33, 44])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![11, 22, 44, 55])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
            ])),
        ],
    )?;
    let t2_table = MemTable::try_new(t2_schema, vec![vec![t2_data]])?;
    ctx.register_table("t2", Arc::new(t2_table))?;

    Ok(ctx)
}

fn create_join_context_qualified() -> Result<ExecutionContext> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::UInt32, true),
        Field::new("c", DataType::UInt32, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
            Arc::new(UInt32Array::from(vec![10, 20, 30, 40])),
            Arc::new(UInt32Array::from(vec![50, 60, 70, 80])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::UInt32, true),
        Field::new("c", DataType::UInt32, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![1, 2, 9, 4])),
            Arc::new(UInt32Array::from(vec![100, 200, 300, 400])),
            Arc::new(UInt32Array::from(vec![500, 600, 700, 800])),
        ],
    )?;
    let t2_table = MemTable::try_new(t2_schema, vec![vec![t2_data]])?;
    ctx.register_table("t2", Arc::new(t2_table))?;

    Ok(ctx)
}

/// the table column_left has more rows than the table column_right
fn create_join_context_unbalanced(
    column_left: &str,
    column_right: &str,
) -> Result<ExecutionContext> {
    let mut ctx = ExecutionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new(column_left, DataType::UInt32, true),
        Field::new("t1_name", DataType::Utf8, true),
    ]));
    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![11, 22, 33, 44, 77])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
            ])),
        ],
    )?;
    let t1_table = MemTable::try_new(t1_schema, vec![vec![t1_data]])?;
    ctx.register_table("t1", Arc::new(t1_table))?;

    let t2_schema = Arc::new(Schema::new(vec![
        Field::new(column_right, DataType::UInt32, true),
        Field::new("t2_name", DataType::Utf8, true),
    ]));
    let t2_data = RecordBatch::try_new(
        t2_schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![11, 22, 44, 55])),
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some("y"),
                Some("x"),
                Some("w"),
            ])),
        ],
    )?;
    let t2_table = MemTable::try_new(t2_schema, vec![vec![t2_data]])?;
    ctx.register_table("t2", Arc::new(t2_table))?;

    Ok(ctx)
}

#[tokio::test]
async fn csv_explain() {
    // This test uses the execute function that create full plan cycle: logical, optimized logical, and physical,
    // then execute the physical plan and return the final explain results
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "EXPLAIN SELECT c1 FROM aggregate_test_100 where c2 > 10";
    let actual = execute(&mut ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);

    // Note can't use `assert_batches_eq` as the plan needs to be
    // normalized for filenames and number of cores
    let expected = vec![
        vec![
            "logical_plan",
            "Projection: #aggregate_test_100.c1\
             \n  Filter: #aggregate_test_100.c2 > Int64(10)\
             \n    TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)]"
        ],
        vec!["physical_plan",
             "ProjectionExec: expr=[c1@0 as c1]\
              \n  CoalesceBatchesExec: target_batch_size=4096\
              \n    FilterExec: CAST(c2@1 AS Int64) > 10\
              \n      RepartitionExec: partitioning=RoundRobinBatch(NUM_CORES)\
              \n        CsvExec: files=[ARROW_TEST_DATA/csv/aggregate_test_100.csv], has_header=true, batch_size=8192, limit=None\
              \n"
    ]];
    assert_eq!(expected, actual);

    // Also, expect same result with lowercase explain
    let sql = "explain SELECT c1 FROM aggregate_test_100 where c2 > 10";
    let actual = execute(&mut ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    assert_eq!(expected, actual);
}

#[tokio::test]
async fn csv_explain_analyze() {
    // This test uses the execute function to run an actual plan under EXPLAIN ANALYZE
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "EXPLAIN ANALYZE SELECT count(*), c1 FROM aggregate_test_100 group by c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual).unwrap();

    // Only test basic plumbing and try to avoid having to change too
    // many things. explain_analyze_baseline_metrics covers the values
    // in greater depth
    let needle = "CoalescePartitionsExec, metrics=[output_rows=5, elapsed_compute=";
    assert_contains!(&formatted, needle);

    let verbose_needle = "Output Rows";
    assert_not_contains!(formatted, verbose_needle);
}

#[tokio::test]
async fn csv_explain_analyze_verbose() {
    // This test uses the execute function to run an actual plan under EXPLAIN VERBOSE ANALYZE
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql =
        "EXPLAIN ANALYZE VERBOSE SELECT count(*), c1 FROM aggregate_test_100 group by c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual).unwrap();

    let verbose_needle = "Output Rows";
    assert_contains!(formatted, verbose_needle);
}

/// A macro to assert that some particular line contains two substrings
///
/// Usage: `assert_metrics!(actual, operator_name, metrics)`
///
macro_rules! assert_metrics {
    ($ACTUAL: expr, $OPERATOR_NAME: expr, $METRICS: expr) => {
        let found = $ACTUAL
            .lines()
            .any(|line| line.contains($OPERATOR_NAME) && line.contains($METRICS));
        assert!(
            found,
            "Can not find a line with both '{}' and '{}' in\n\n{}",
            $OPERATOR_NAME, $METRICS, $ACTUAL
        );
    };
}

#[tokio::test]
async fn explain_analyze_baseline_metrics() {
    // This test uses the execute function to run an actual plan under EXPLAIN ANALYZE
    // and then validate the presence of baseline metrics for supported operators
    let config = ExecutionConfig::new().with_target_partitions(3);
    let mut ctx = ExecutionContext::with_config(config);
    register_aggregate_csv_by_sql(&mut ctx).await;
    // a query with as many operators as we have metrics for
    let sql = "EXPLAIN ANALYZE \
               SELECT count(*) as cnt FROM \
                 (SELECT count(*), c1 \
                  FROM aggregate_test_100 \
                  WHERE c13 != 'C2GT5KVyOPZpgKVl110TyZO0NcJ434' \
                  GROUP BY c1 \
                  ORDER BY c1 ) AS a \
                 UNION ALL \
               SELECT 1 as cnt \
                 UNION ALL \
               SELECT lead(c1, 1) OVER () as cnt FROM (select 1 as c1) AS b \
               LIMIT 3";
    println!("running query: {}", sql);
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let physical_plan = ctx.create_physical_plan(&plan).await.unwrap();
    let results = collect(physical_plan.clone()).await.unwrap();
    let formatted = arrow::util::pretty::pretty_format_batches(&results).unwrap();
    println!("Query Output:\n\n{}", formatted);

    assert_metrics!(
        &formatted,
        "HashAggregateExec: mode=Partial, gby=[]",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "HashAggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1]",
        "metrics=[output_rows=5, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "SortExec: [c1@0 ASC NULLS LAST]",
        "metrics=[output_rows=5, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "FilterExec: c13@1 != C2GT5KVyOPZpgKVl110TyZO0NcJ434",
        "metrics=[output_rows=99, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "GlobalLimitExec: limit=3, ",
        "metrics=[output_rows=1, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "LocalLimitExec: limit=3",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "ProjectionExec: expr=[COUNT(UInt8(1))",
        "metrics=[output_rows=1, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "CoalesceBatchesExec: target_batch_size=4096",
        "metrics=[output_rows=5, elapsed_compute"
    );
    assert_metrics!(
        &formatted,
        "CoalescePartitionsExec",
        "metrics=[output_rows=5, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "UnionExec",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "WindowAggExec",
        "metrics=[output_rows=1, elapsed_compute="
    );

    fn expected_to_have_metrics(plan: &dyn ExecutionPlan) -> bool {
        use datafusion::physical_plan;

        plan.as_any().downcast_ref::<physical_plan::sort::SortExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::hash_aggregate::HashAggregateExec>().is_some()
            // CoalescePartitionsExec doesn't do any work so is not included
            || plan.as_any().downcast_ref::<physical_plan::filter::FilterExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::limit::GlobalLimitExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::limit::LocalLimitExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::projection::ProjectionExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::coalesce_batches::CoalesceBatchesExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::coalesce_partitions::CoalescePartitionsExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::union::UnionExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::windows::WindowAggExec>().is_some()
    }

    // Validate that the recorded elapsed compute time was more than
    // zero for all operators as well as the start/end timestamp are set
    struct TimeValidator {}
    impl ExecutionPlanVisitor for TimeValidator {
        type Error = std::convert::Infallible;

        fn pre_visit(
            &mut self,
            plan: &dyn ExecutionPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if !expected_to_have_metrics(plan) {
                return Ok(true);
            }
            let metrics = plan.metrics().unwrap().aggregate_by_partition();

            assert!(metrics.output_rows().unwrap() > 0);
            assert!(metrics.elapsed_compute().unwrap() > 0);

            let mut saw_start = false;
            let mut saw_end = false;
            metrics.iter().for_each(|m| match m.value() {
                MetricValue::StartTimestamp(ts) => {
                    saw_start = true;
                    assert!(ts.value().unwrap().timestamp_nanos() > 0);
                }
                MetricValue::EndTimestamp(ts) => {
                    saw_end = true;
                    assert!(ts.value().unwrap().timestamp_nanos() > 0);
                }
                _ => {}
            });

            assert!(saw_start);
            assert!(saw_end);

            Ok(true)
        }
    }

    datafusion::physical_plan::accept(physical_plan.as_ref(), &mut TimeValidator {})
        .unwrap();
}

#[tokio::test]
async fn csv_explain_plans() {
    // This test verify the look of each plan in its full cycle plan creation

    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "EXPLAIN SELECT c1 FROM aggregate_test_100 where c2 > 10";

    // Logical plan
    // Create plan
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx.create_logical_plan(sql).expect(&msg);
    let logical_schema = plan.schema();
    //
    println!("SQL: {}", sql);
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: #aggregate_test_100.c2 > Int64(10) [c1:Utf8, c2:Int32, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:Int64, c10:Utf8, c11:Float32, c12:Float64, c13:Utf8]",
        "      TableScan: aggregate_test_100 projection=None [c1:Utf8, c2:Int32, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:Int64, c10:Utf8, c11:Float32, c12:Float64, c13:Utf8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: #aggregate_test_100.c1",
        "    Filter: #aggregate_test_100.c2 > Int64(10)",
        "      TableScan: aggregate_test_100 projection=None",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan (see https://graphviz.org)",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: #aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100 projection=None\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: #aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\\nSchema: [c1:Utf8, c2:Int32, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:Int64, c10:Utf8, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100 projection=None\\nSchema: [c1:Utf8, c2:Int32, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:Int64, c10:Utf8, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    // Optimized logical plan
    //
    let msg = format!("Optimizing logical plan for '{}': {:?}", sql, plan);
    let plan = ctx.optimize(&plan).expect(&msg);
    let optimized_logical_schema = plan.schema();
    // Both schema has to be the same
    assert_eq!(logical_schema.as_ref(), optimized_logical_schema.as_ref());
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: #aggregate_test_100.c2 > Int64(10) [c1:Utf8, c2:Int32]",
        "      TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)] [c1:Utf8, c2:Int32]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: #aggregate_test_100.c1",
        "    Filter: #aggregate_test_100.c2 > Int64(10)",
        "      TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)]",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan (see https://graphviz.org)",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: #aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)]\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: #aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\\nSchema: [c1:Utf8, c2:Int32]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)]\\nSchema: [c1:Utf8, c2:Int32]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    // Physical plan
    // Create plan
    let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
    let plan = ctx.create_physical_plan(&plan).await.expect(&msg);
    //
    // Execute plan
    let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
    let results = collect(plan).await.expect(&msg);
    let actual = result_vec(&results);
    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();
    // Since the plan contains path that are environmentally dependant (e.g. full path of the test file), only verify important content
    assert_contains!(&actual, "logical_plan");
    assert_contains!(&actual, "Projection: #aggregate_test_100.c1");
    assert_contains!(actual, "Filter: #aggregate_test_100.c2 > Int64(10)");
}

#[tokio::test]
async fn csv_explain_verbose() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 > 10";
    let actual = execute(&mut ctx, sql).await;

    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();

    // Don't actually test the contents of the debuging output (as
    // that may change and keeping this test updated will be a
    // pain). Instead just check for a few key pieces.
    assert_contains!(&actual, "logical_plan");
    assert_contains!(&actual, "physical_plan");
    assert_contains!(&actual, "#aggregate_test_100.c2 > Int64(10)");

    // ensure the "same text as above" optimization is working
    assert_contains!(actual, "SAME TEXT AS ABOVE");
}

#[tokio::test]
async fn csv_explain_verbose_plans() {
    // This test verify the look of each plan in its full cycle plan creation

    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 > 10";

    // Logical plan
    // Create plan
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx.create_logical_plan(sql).expect(&msg);
    let logical_schema = plan.schema();
    //
    println!("SQL: {}", sql);

    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: #aggregate_test_100.c2 > Int64(10) [c1:Utf8, c2:Int32, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:Int64, c10:Utf8, c11:Float32, c12:Float64, c13:Utf8]",
        "      TableScan: aggregate_test_100 projection=None [c1:Utf8, c2:Int32, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:Int64, c10:Utf8, c11:Float32, c12:Float64, c13:Utf8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: #aggregate_test_100.c1",
        "    Filter: #aggregate_test_100.c2 > Int64(10)",
        "      TableScan: aggregate_test_100 projection=None",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan (see https://graphviz.org)",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: #aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100 projection=None\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: #aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\\nSchema: [c1:Utf8, c2:Int32, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:Int64, c10:Utf8, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100 projection=None\\nSchema: [c1:Utf8, c2:Int32, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:Int64, c10:Utf8, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    // Optimized logical plan
    //
    let msg = format!("Optimizing logical plan for '{}': {:?}", sql, plan);
    let plan = ctx.optimize(&plan).expect(&msg);
    let optimized_logical_schema = plan.schema();
    // Both schema has to be the same
    assert_eq!(logical_schema.as_ref(), optimized_logical_schema.as_ref());
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: #aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: #aggregate_test_100.c2 > Int64(10) [c1:Utf8, c2:Int32]",
        "      TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)] [c1:Utf8, c2:Int32]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: #aggregate_test_100.c1",
        "    Filter: #aggregate_test_100.c2 > Int64(10)",
        "      TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)]",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan (see https://graphviz.org)",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: #aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)]\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: #aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: #aggregate_test_100.c2 > Int64(10)\\nSchema: [c1:Utf8, c2:Int32]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100 projection=Some([0, 1]), filters=[#aggregate_test_100.c2 > Int64(10)]\\nSchema: [c1:Utf8, c2:Int32]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected, actual
    );

    // Physical plan
    // Create plan
    let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
    let plan = ctx.create_physical_plan(&plan).await.expect(&msg);
    //
    // Execute plan
    let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
    let results = collect(plan).await.expect(&msg);
    let actual = result_vec(&results);
    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();
    // Since the plan contains path that are environmentally
    // dependant(e.g. full path of the test file), only verify
    // important content
    assert_contains!(&actual, "logical_plan after projection_push_down");
    assert_contains!(&actual, "physical_plan");
    assert_contains!(&actual, "FilterExec: CAST(c2@1 AS Int64) > 10");
    assert_contains!(actual, "ProjectionExec: expr=[c1@0 as c1]");
}

#[tokio::test]
async fn explain_analyze_runs_optimizers() {
    // repro for https://github.com/apache/arrow-datafusion/issues/917
    // where EXPLAIN ANALYZE was not correctly running optiimizer
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;

    // This happens as an optimization pass where count(*) can be
    // answered using statistics only.
    let expected = "EmptyExec: produce_one_row=true";

    let sql = "EXPLAIN SELECT count(*) from alltypes_plain";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let actual = arrow::util::pretty::pretty_format_batches(&actual).unwrap();
    assert_contains!(actual, expected);

    // EXPLAIN ANALYZE should work the same
    let sql = "EXPLAIN  ANALYZE SELECT count(*) from alltypes_plain";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let actual = arrow::util::pretty::pretty_format_batches(&actual).unwrap();
    assert_contains!(actual, expected);
}

#[tokio::test]
async fn tpch_explain_q10() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    register_tpch_csv(&mut ctx, "customer").await?;
    register_tpch_csv(&mut ctx, "orders").await?;
    register_tpch_csv(&mut ctx, "lineitem").await?;
    register_tpch_csv(&mut ctx, "nation").await?;

    let sql = "select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1993-10-01'
  and o_orderdate < date '1994-01-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc;";

    let mut plan = ctx.create_logical_plan(sql);
    plan = ctx.optimize(&plan.unwrap());

    let expected = "\
    Sort: #revenue DESC NULLS FIRST\
    \n  Projection: #customer.c_custkey, #customer.c_name, #SUM(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount) AS revenue, #customer.c_acctbal, #nation.n_name, #customer.c_address, #customer.c_phone, #customer.c_comment\
    \n    Aggregate: groupBy=[[#customer.c_custkey, #customer.c_name, #customer.c_acctbal, #customer.c_phone, #nation.n_name, #customer.c_address, #customer.c_comment]], aggr=[[SUM(#lineitem.l_extendedprice * Int64(1) - #lineitem.l_discount)]]\
    \n      Join: #customer.c_nationkey = #nation.n_nationkey\
    \n        Join: #orders.o_orderkey = #lineitem.l_orderkey\
    \n          Join: #customer.c_custkey = #orders.o_custkey\
    \n            TableScan: customer projection=Some([0, 1, 2, 3, 4, 5, 7])\
    \n            Filter: #orders.o_orderdate >= Date32(\"8674\") AND #orders.o_orderdate < Date32(\"8766\")\
    \n              TableScan: orders projection=Some([0, 1, 4]), filters=[#orders.o_orderdate >= Date32(\"8674\"), #orders.o_orderdate < Date32(\"8766\")]\
    \n          Filter: #lineitem.l_returnflag = Utf8(\"R\")\
    \n            TableScan: lineitem projection=Some([0, 5, 6, 8]), filters=[#lineitem.l_returnflag = Utf8(\"R\")]\
    \n        TableScan: nation projection=Some([0, 1])";
    assert_eq!(format!("{:?}", plan.unwrap()), expected);

    Ok(())
}

fn get_tpch_table_schema(table: &str) -> Schema {
    match table {
        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

async fn register_tpch_csv(ctx: &mut ExecutionContext, table: &str) -> Result<()> {
    let schema = get_tpch_table_schema(table);

    ctx.register_csv(
        table,
        format!("tests/tpch-csv/{}.csv", table).as_str(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

async fn register_aggregate_csv_by_sql(ctx: &mut ExecutionContext) {
    let testdata = datafusion::test_util::arrow_test_data();

    // TODO: The following c9 should be migrated to UInt32 and c10 should be UInt64 once
    // unsigned is supported.
    let df = ctx
        .sql(&format!(
            "
    CREATE EXTERNAL TABLE aggregate_test_100 (
        c1  VARCHAR NOT NULL,
        c2  INT NOT NULL,
        c3  SMALLINT NOT NULL,
        c4  SMALLINT NOT NULL,
        c5  INT NOT NULL,
        c6  BIGINT NOT NULL,
        c7  SMALLINT NOT NULL,
        c8  INT NOT NULL,
        c9  BIGINT NOT NULL,
        c10 VARCHAR NOT NULL,
        c11 FLOAT NOT NULL,
        c12 DOUBLE NOT NULL,
        c13 VARCHAR NOT NULL
    )
    STORED AS CSV
    WITH HEADER ROW
    LOCATION '{}/csv/aggregate_test_100.csv'
    ",
            testdata
        ))
        .await
        .expect("Creating dataframe for CREATE EXTERNAL TABLE");

    // Mimic the CLI and execute the resulting plan -- even though it
    // is effectively a no-op (returns zero rows)
    let results = df.collect().await.expect("Executing CREATE EXTERNAL TABLE");
    assert!(
        results.is_empty(),
        "Expected no rows from executing CREATE EXTERNAL TABLE"
    );
}

/// Create table "t1" with two boolean columns "a" and "b"
async fn register_boolean(ctx: &mut ExecutionContext) -> Result<()> {
    let a: BooleanArray = [
        Some(true),
        Some(true),
        Some(true),
        None,
        None,
        None,
        Some(false),
        Some(false),
        Some(false),
    ]
    .iter()
    .collect();
    let b: BooleanArray = [
        Some(true),
        None,
        Some(false),
        Some(true),
        None,
        Some(false),
        Some(true),
        None,
        Some(false),
    ]
    .iter()
    .collect();

    let data =
        RecordBatch::try_from_iter([("a", Arc::new(a) as _), ("b", Arc::new(b) as _)])?;
    let table = MemTable::try_new(data.schema(), vec![vec![data]])?;
    ctx.register_table("t1", Arc::new(table))?;
    Ok(())
}

async fn register_aggregate_csv(ctx: &mut ExecutionContext) -> Result<()> {
    let testdata = datafusion::test_util::arrow_test_data();
    let schema = test_util::aggr_test_schema();
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{}/csv/aggregate_test_100.csv", testdata),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

async fn register_simple_aggregate_csv_with_decimal_by_sql(ctx: &mut ExecutionContext) {
    let df = ctx
        .sql(
            "CREATE EXTERNAL TABLE aggregate_simple (
            c1  DECIMAL(10,6) NOT NULL,
            c2  DOUBLE NOT NULL,
            c3  BOOLEAN NOT NULL
            )
            STORED AS CSV
            WITH HEADER ROW
            LOCATION 'tests/aggregate_simple.csv'",
        )
        .await
        .expect("Creating dataframe for CREATE EXTERNAL TABLE with decimal data type");

    let results = df.collect().await.expect("Executing CREATE EXTERNAL TABLE");
    assert!(
        results.is_empty(),
        "Expected no rows from executing CREATE EXTERNAL TABLE"
    );
}

async fn register_aggregate_simple_csv(ctx: &mut ExecutionContext) -> Result<()> {
    // It's not possible to use aggregate_test_100, not enought similar values to test grouping on floats
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Float32, false),
        Field::new("c2", DataType::Float64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));

    ctx.register_csv(
        "aggregate_simple",
        "tests/aggregate_simple.csv",
        CsvReadOptions::new().schema(&schema),
    )
    .await?;
    Ok(())
}

async fn register_alltypes_parquet(ctx: &mut ExecutionContext) {
    let testdata = datafusion::test_util::parquet_test_data();
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{}/alltypes_plain.parquet", testdata),
    )
    .await
    .unwrap();
}

#[cfg(feature = "avro")]
async fn register_alltypes_avro(ctx: &mut ExecutionContext) {
    let testdata = datafusion::test_util::arrow_test_data();
    ctx.register_avro(
        "alltypes_plain",
        &format!("{}/avro/alltypes_plain.avro", testdata),
        AvroReadOptions::default(),
    )
    .await
    .unwrap();
}

/// Execute query and return result set as 2-d table of Vecs
/// `result[row][column]`
async fn execute_to_batches(ctx: &mut ExecutionContext, sql: &str) -> Vec<RecordBatch> {
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx.create_logical_plan(sql).expect(&msg);
    let logical_schema = plan.schema();

    let msg = format!("Optimizing logical plan for '{}': {:?}", sql, plan);
    let plan = ctx.optimize(&plan).expect(&msg);
    let optimized_logical_schema = plan.schema();

    let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
    let plan = ctx.create_physical_plan(&plan).await.expect(&msg);

    let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
    let results = collect(plan).await.expect(&msg);

    assert_eq!(logical_schema.as_ref(), optimized_logical_schema.as_ref());
    results
}

/// Execute query and return result set as 2-d table of Vecs
/// `result[row][column]`
async fn execute(ctx: &mut ExecutionContext, sql: &str) -> Vec<Vec<String>> {
    result_vec(&execute_to_batches(ctx, sql).await)
}

/// Specialised String representation
fn col_str(column: &ArrayRef, row_index: usize) -> String {
    if column.is_null(row_index) {
        return "NULL".to_string();
    }

    // Special case ListArray as there is no pretty print support for it yet
    if let DataType::FixedSizeList(_, n) = column.data_type() {
        let array = column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .value(row_index);

        let mut r = Vec::with_capacity(*n as usize);
        for i in 0..*n {
            r.push(col_str(&array, i as usize));
        }
        return format!("[{}]", r.join(","));
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

async fn generic_query_length<T: 'static + Array + From<Vec<&'static str>>>(
    datatype: DataType,
) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", datatype, false)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(T::from(vec!["", "a", "aa", "aaa"]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT length(c1) FROM test";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![vec!["0"], vec!["1"], vec!["2"], vec!["3"]];
    assert_eq!(expected, actual);
    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "unicode_expressions"), ignore)]
async fn query_length() -> Result<()> {
    generic_query_length::<StringArray>(DataType::Utf8).await
}

#[tokio::test]
#[cfg_attr(not(feature = "unicode_expressions"), ignore)]
async fn query_large_length() -> Result<()> {
    generic_query_length::<LargeStringArray>(DataType::LargeUtf8).await
}

#[tokio::test]
async fn query_not() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Boolean, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(BooleanArray::from(vec![
            Some(false),
            None,
            Some(true),
        ]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT NOT c1 FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------+",
        "| NOT test.c1 |",
        "+-------------+",
        "| true        |",
        "|             |",
        "| false       |",
        "+-------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_concat() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["", "a", "aa", "aaa"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT concat(c1, '-hi-', cast(c2 as varchar)) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------------------------------------------+",
        "| concat(test.c1,Utf8(\"-hi-\"),CAST(test.c2 AS Utf8)) |",
        "+----------------------------------------------------+",
        "| -hi-0                                              |",
        "| a-hi-1                                             |",
        "| aa-hi-                                             |",
        "| aaa-hi-3                                           |",
        "+----------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

// Revisit after implementing https://github.com/apache/arrow-rs/issues/925
#[tokio::test]
async fn query_array() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::Int32, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["", "a", "aa", "aaa"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT array(c1, cast(c2 as varchar)) FROM test";
    let actual = execute(&mut ctx, sql).await;
    let expected = vec![
        vec!["[,0]"],
        vec!["[a,1]"],
        vec!["[aa,NULL]"],
        vec!["[aaa,3]"],
    ];
    assert_eq!(expected, actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_sum_cast() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    // c8 = i32; c9 = i64
    let sql = "SELECT c8 + c9 FROM aggregate_test_100";
    // check that the physical and logical schemas are equal
    execute(&mut ctx, sql).await;
}

#[tokio::test]
async fn query_where_neg_num() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;

    // Negative numbers do not parse correctly as of Arrow 2.0.0
    let sql = "select c7, c8 from aggregate_test_100 where c7 >= -2 and c7 < 10";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-------+",
        "| c7 | c8    |",
        "+----+-------+",
        "| 7  | 45465 |",
        "| 5  | 40622 |",
        "| 0  | 61069 |",
        "| 2  | 20120 |",
        "| 4  | 39363 |",
        "+----+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Also check floating point neg numbers
    let sql = "select c7, c8 from aggregate_test_100 where c7 >= -2.9 and c7 < 10";
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT COUNT(c1) FROM aggregate_test_100 WHERE c13 LIKE '%FB%'";
    // check that the physical and logical schemas are equal
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------------------------------+",
        "| COUNT(aggregate_test_100.c1) |",
        "+------------------------------+",
        "| 1                            |",
        "+------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

fn make_timestamp_table<A>() -> Result<Arc<MemTable>>
where
    A: ArrowTimestampType,
{
    make_timestamp_tz_table::<A>(None)
}

fn make_timestamp_tz_table<A>(tz: Option<String>) -> Result<Arc<MemTable>>
where
    A: ArrowTimestampType,
{
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(A::get_time_unit(), tz.clone()),
            false,
        ),
        Field::new("value", DataType::Int32, true),
    ]));

    let divisor = match A::get_time_unit() {
        TimeUnit::Nanosecond => 1,
        TimeUnit::Microsecond => 1000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Second => 1_000_000_000,
    };

    let timestamps = vec![
        1599572549190855000i64 / divisor, // 2020-09-08T13:42:29.190855+00:00
        1599568949190855000 / divisor,    // 2020-09-08T12:42:29.190855+00:00
        1599565349190855000 / divisor,    //2020-09-08T11:42:29.190855+00:00
    ]; // 2020-09-08T11:42:29.190855+00:00

    let array = PrimitiveArray::<A>::from_vec(timestamps, tz);

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(array),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    Ok(Arc::new(table))
}

fn make_timestamp_nano_table() -> Result<Arc<MemTable>> {
    make_timestamp_table::<TimestampNanosecondType>()
}

#[tokio::test]
async fn to_timestamp() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn to_timestamp_millis() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "ts_data",
        make_timestamp_table::<TimestampMillisecondType>()?,
    )?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_millis('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn to_timestamp_micros() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "ts_data",
        make_timestamp_table::<TimestampMicrosecondType>()?,
    )?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_micros('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn to_timestamp_seconds() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_data", make_timestamp_table::<TimestampSecondType>()?)?;

    let sql = "SELECT COUNT(*) FROM ts_data where ts > to_timestamp_seconds('2020-09-08T12:00:00+00:00')";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+-----------------+",
        "| COUNT(UInt8(1)) |",
        "+-----------------+",
        "| 2               |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn count_distinct_timestamps() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ts_data", make_timestamp_nano_table()?)?;

    let sql = "SELECT COUNT(DISTINCT(ts)) FROM ts_data";
    let actual = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+----------------------------+",
        "| COUNT(DISTINCT ts_data.ts) |",
        "+----------------------------+",
        "| 3                          |",
        "+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_is_null() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Float64, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(f64::NAN),
        ]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT c1 IS NULL FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----------------+",
        "| test.c1 IS NULL |",
        "+-----------------+",
        "| false           |",
        "| true            |",
        "| false           |",
        "+-----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_is_not_null() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Float64, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(f64::NAN),
        ]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT c1 IS NOT NULL FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------------------+",
        "| test.c1 IS NOT NULL |",
        "+---------------------+",
        "| true                |",
        "| false               |",
        "| true                |",
        "+---------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_count_distinct() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
            Some(3),
        ]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COUNT(DISTINCT c1) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------+",
        "| COUNT(DISTINCT test.c1) |",
        "+-------------------------+",
        "| 3                       |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_group_on_null() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![
            Some(0),
            Some(3),
            None,
            Some(1),
            Some(3),
        ]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COUNT(*), c1 FROM test GROUP BY c1";

    let actual = execute_to_batches(&mut ctx, sql).await;

    // Note that the results also
    // include a row for NULL (c1=NULL, count = 1)
    let expected = vec![
        "+-----------------+----+",
        "| COUNT(UInt8(1)) | c1 |",
        "+-----------------+----+",
        "| 1               |    |",
        "| 1               | 0  |",
        "| 1               | 1  |",
        "| 2               | 3  |",
        "+-----------------+----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_group_on_null_multi_col() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Utf8, true),
    ]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(0),
                Some(0),
                Some(3),
                None,
                None,
                Some(3),
                Some(0),
                None,
                Some(3),
            ])),
            Arc::new(StringArray::from(vec![
                None,
                None,
                Some("foo"),
                None,
                Some("bar"),
                Some("foo"),
                None,
                Some("bar"),
                Some("foo"),
            ])),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT COUNT(*), c1, c2 FROM test GROUP BY c1, c2";

    let actual = execute_to_batches(&mut ctx, sql).await;

    // Note that the results also include values for null
    // include a row for NULL (c1=NULL, count = 1)
    let expected = vec![
        "+-----------------+----+-----+",
        "| COUNT(UInt8(1)) | c1 | c2  |",
        "+-----------------+----+-----+",
        "| 1               |    |     |",
        "| 2               |    | bar |",
        "| 3               | 0  |     |",
        "| 3               | 3  | foo |",
        "+-----------------+----+-----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);

    // Also run query with group columns reversed (results should be the same)
    let sql = "SELECT COUNT(*), c1, c2 FROM test GROUP BY c2, c1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_on_string_dictionary() -> Result<()> {
    // Test to ensure DataFusion can operate on dictionary types
    // Use StringDictionary (32 bit indexes = keys)
    let array = vec![Some("one"), None, Some("three")]
        .into_iter()
        .collect::<DictionaryArray<Int32Type>>();

    let batch =
        RecordBatch::try_from_iter(vec![("d1", Arc::new(array) as ArrayRef)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    // Basic SELECT
    let sql = "SELECT * FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| one   |",
        "|       |",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // basic filtering
    let sql = "SELECT * FROM test WHERE d1 IS NOT NULL";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| one   |",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // filtering with constant
    let sql = "SELECT * FROM test WHERE d1 = 'three'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // Expression evaluation
    let sql = "SELECT concat(d1, '-foo') FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------------------------------+",
        "| concat(test.d1,Utf8(\"-foo\")) |",
        "+------------------------------+",
        "| one-foo                      |",
        "| -foo                         |",
        "| three-foo                    |",
        "+------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // aggregation
    let sql = "SELECT COUNT(d1) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| COUNT(test.d1) |",
        "+----------------+",
        "| 2              |",
        "+----------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // aggregation min
    let sql = "SELECT MIN(d1) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| MIN(test.d1) |",
        "+--------------+",
        "| one          |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // aggregation max
    let sql = "SELECT MAX(d1) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| MAX(test.d1) |",
        "+--------------+",
        "| three        |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // grouping
    let sql = "SELECT d1, COUNT(*) FROM test group by d1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+-----------------+",
        "| d1    | COUNT(UInt8(1)) |",
        "+-------+-----------------+",
        "| one   | 1               |",
        "|       | 1               |",
        "| three | 1               |",
        "+-------+-----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);

    // window functions
    let sql = "SELECT d1, row_number() OVER (partition by d1) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+--------------+",
        "| d1    | ROW_NUMBER() |",
        "+-------+--------------+",
        "|       | 1            |",
        "| one   | 1            |",
        "| three | 1            |",
        "+-------+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn query_without_from() -> Result<()> {
    // Test for SELECT <expression> without FROM.
    // Should evaluate expressions in project position.
    let mut ctx = ExecutionContext::new();

    let sql = "SELECT 1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| Int64(1) |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT 1+2, 3/4, cos(0)";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------------------+---------------------+---------------+",
        "| Int64(1) + Int64(2) | Int64(3) / Int64(4) | cos(Int64(0)) |",
        "+---------------------+---------------------+---------------+",
        "| 3                   | 0                   | 1             |",
        "+---------------------+---------------------+---------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn query_cte() -> Result<()> {
    // Test for SELECT <expression> without FROM.
    // Should evaluate expressions in project position.
    let mut ctx = ExecutionContext::new();

    // simple with
    let sql = "WITH t AS (SELECT 1) SELECT * FROM t";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| Int64(1) |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);

    // with + union
    let sql =
        "WITH t AS (SELECT 1 AS a), u AS (SELECT 2 AS a) SELECT * FROM t UNION ALL SELECT * FROM u";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+---+", "| a |", "+---+", "| 1 |", "| 2 |", "+---+"];
    assert_batches_eq!(expected, &actual);

    // with + join
    let sql = "WITH t AS (SELECT 1 AS id1), u AS (SELECT 1 AS id2, 5 as x) SELECT x FROM t JOIN u ON (id1 = id2)";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+---+", "| x |", "+---+", "| 5 |", "+---+"];
    assert_batches_eq!(expected, &actual);

    // backward reference
    let sql = "WITH t AS (SELECT 1 AS id1), u AS (SELECT * FROM t) SELECT * from u";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+-----+", "| id1 |", "+-----+", "| 1   |", "+-----+"];
    assert_batches_eq!(expected, &actual);

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
async fn query_scalar_minus_array() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(3),
        ]))],
    )?;

    let table = MemTable::try_new(schema, vec![vec![data]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;
    let sql = "SELECT 4 - c1 FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------------------------+",
        "| Int64(4) Minus test.c1 |",
        "+------------------------+",
        "| 4                      |",
        "| 3                      |",
        "|                        |",
        "| 1                      |",
        "+------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

fn assert_float_eq<T>(expected: &[Vec<T>], received: &[Vec<String>])
where
    T: AsRef<str>,
{
    expected
        .iter()
        .flatten()
        .zip(received.iter().flatten())
        .for_each(|(l, r)| {
            let (l, r) = (
                l.as_ref().parse::<f64>().unwrap(),
                r.as_str().parse::<f64>().unwrap(),
            );
            assert!((l - r).abs() <= 2.0 * f64::EPSILON);
        });
}

#[tokio::test]
async fn csv_between_expr() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c4 FROM aggregate_test_100 WHERE c12 BETWEEN 0.995 AND 1.0";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c4    |",
        "+-------+",
        "| 10837 |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_between_expr_negated() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c4 FROM aggregate_test_100 WHERE c12 NOT BETWEEN 0 AND 0.995";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c4    |",
        "+-------+",
        "| 10837 |",
        "+-------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_group_by_date() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
        Field::new("cnt", DataType::Int32, false),
    ]));
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![
                Some(100),
                Some(100),
                Some(100),
                Some(101),
                Some(101),
                Some(101),
            ])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                Some(3),
                Some(3),
                Some(3),
            ])),
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;

    ctx.register_table("dates", Arc::new(table))?;
    let sql = "SELECT SUM(cnt) FROM dates GROUP BY date";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| SUM(dates.cnt) |",
        "+----------------+",
        "| 6              |",
        "| 9              |",
        "+----------------+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn group_by_timestamp_millis() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("count", DataType::Int32, false),
    ]));
    let base_dt = Utc.ymd(2018, 7, 1).and_hms(6, 0, 0); // 2018-Jul-01 06:00
    let hour1 = Duration::hours(1);
    let timestamps = vec![
        base_dt.timestamp_millis(),
        (base_dt + hour1).timestamp_millis(),
        base_dt.timestamp_millis(),
        base_dt.timestamp_millis(),
        (base_dt + hour1).timestamp_millis(),
        (base_dt + hour1).timestamp_millis(),
    ];
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
        ],
    )?;
    let t1_table = MemTable::try_new(schema, vec![vec![data]])?;
    ctx.register_table("t1", Arc::new(t1_table)).unwrap();

    let sql =
        "SELECT timestamp, SUM(count) FROM t1 GROUP BY timestamp ORDER BY timestamp ASC";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------------------+---------------+",
        "| timestamp           | SUM(t1.count) |",
        "+---------------------+---------------+",
        "| 2018-07-01 06:00:00 | 80            |",
        "| 2018-07-01 07:00:00 | 130           |",
        "+---------------------+---------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

macro_rules! test_expression {
    ($SQL:expr, $EXPECTED:expr) => {
        let mut ctx = ExecutionContext::new();
        let sql = format!("SELECT {}", $SQL);
        let actual = execute(&mut ctx, sql.as_str()).await;
        assert_eq!(actual[0][0], $EXPECTED);
    };
}

#[tokio::test]
async fn test_boolean_expressions() -> Result<()> {
    test_expression!("true", "true");
    test_expression!("false", "false");
    test_expression!("false = false", "true");
    test_expression!("true = false", "false");
    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "crypto_expressions"), ignore)]
async fn test_crypto_expressions() -> Result<()> {
    test_expression!("md5('tom')", "34b7da764b21d298ef307d04d8152dc5");
    test_expression!("digest('tom','md5')", "34b7da764b21d298ef307d04d8152dc5");
    test_expression!("md5('')", "d41d8cd98f00b204e9800998ecf8427e");
    test_expression!("digest('','md5')", "d41d8cd98f00b204e9800998ecf8427e");
    test_expression!("md5(NULL)", "NULL");
    test_expression!("digest(NULL,'md5')", "NULL");
    test_expression!(
        "sha224('tom')",
        "0bf6cb62649c42a9ae3876ab6f6d92ad36cb5414e495f8873292be4d"
    );
    test_expression!(
        "digest('tom','sha224')",
        "0bf6cb62649c42a9ae3876ab6f6d92ad36cb5414e495f8873292be4d"
    );
    test_expression!(
        "sha224('')",
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
    );
    test_expression!(
        "digest('','sha224')",
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
    );
    test_expression!("sha224(NULL)", "NULL");
    test_expression!("digest(NULL,'sha224')", "NULL");
    test_expression!(
        "sha256('tom')",
        "e1608f75c5d7813f3d4031cb30bfb786507d98137538ff8e128a6ff74e84e643"
    );
    test_expression!(
        "digest('tom','sha256')",
        "e1608f75c5d7813f3d4031cb30bfb786507d98137538ff8e128a6ff74e84e643"
    );
    test_expression!(
        "sha256('')",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    test_expression!(
        "digest('','sha256')",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    test_expression!("sha256(NULL)", "NULL");
    test_expression!("digest(NULL,'sha256')", "NULL");
    test_expression!("sha384('tom')", "096f5b68aa77848e4fdf5c1c0b350de2dbfad60ffd7c25d9ea07c6c19b8a4d55a9187eb117c557883f58c16dfac3e343");
    test_expression!("digest('tom','sha384')", "096f5b68aa77848e4fdf5c1c0b350de2dbfad60ffd7c25d9ea07c6c19b8a4d55a9187eb117c557883f58c16dfac3e343");
    test_expression!("sha384('')", "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b");
    test_expression!("digest('','sha384')", "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b");
    test_expression!("sha384(NULL)", "NULL");
    test_expression!("digest(NULL,'sha384')", "NULL");
    test_expression!("sha512('tom')", "6e1b9b3fe840680e37051f7ad5e959d6f39ad0f8885d855166f55c659469d3c8b78118c44a2a49c72ddb481cd6d8731034e11cc030070ba843a90b3495cb8d3e");
    test_expression!("digest('tom','sha512')", "6e1b9b3fe840680e37051f7ad5e959d6f39ad0f8885d855166f55c659469d3c8b78118c44a2a49c72ddb481cd6d8731034e11cc030070ba843a90b3495cb8d3e");
    test_expression!("sha512('')", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
    test_expression!("digest('','sha512')", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
    test_expression!("sha512(NULL)", "NULL");
    test_expression!("digest(NULL,'sha512')", "NULL");
    test_expression!("digest(NULL,'blake2s')", "NULL");
    test_expression!("digest(NULL,'blake2b')", "NULL");
    test_expression!("digest('','blake2b')", "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce");
    test_expression!("digest('tom','blake2b')", "482499a18da10a18d8d35ab5eb4c635551ec5b8d3ff37c3e87a632caf6680fe31566417834b4732e26e0203d1cad4f5366cb7ab57d89694e4c1fda3e26af2c23");
    test_expression!(
        "digest('','blake2s')",
        "69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9"
    );
    test_expression!(
        "digest('tom','blake2s')",
        "5fc3f2b3a07cade5023c3df566e4d697d3823ba1b72bfb3e84cf7e768b2e7529"
    );
    test_expression!(
        "digest('','blake3')",
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    );
    Ok(())
}

#[tokio::test]
async fn test_interval_expressions() -> Result<()> {
    test_expression!(
        "interval '1'",
        "0 years 0 mons 0 days 0 hours 0 mins 1.00 secs"
    );
    test_expression!(
        "interval '1 second'",
        "0 years 0 mons 0 days 0 hours 0 mins 1.00 secs"
    );
    test_expression!(
        "interval '500 milliseconds'",
        "0 years 0 mons 0 days 0 hours 0 mins 0.500 secs"
    );
    test_expression!(
        "interval '5 second'",
        "0 years 0 mons 0 days 0 hours 0 mins 5.00 secs"
    );
    test_expression!(
        "interval '0.5 minute'",
        "0 years 0 mons 0 days 0 hours 0 mins 30.00 secs"
    );
    test_expression!(
        "interval '.5 minute'",
        "0 years 0 mons 0 days 0 hours 0 mins 30.00 secs"
    );
    test_expression!(
        "interval '5 minute'",
        "0 years 0 mons 0 days 0 hours 5 mins 0.00 secs"
    );
    test_expression!(
        "interval '5 minute 1 second'",
        "0 years 0 mons 0 days 0 hours 5 mins 1.00 secs"
    );
    test_expression!(
        "interval '1 hour'",
        "0 years 0 mons 0 days 1 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '5 hour'",
        "0 years 0 mons 0 days 5 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '1 day'",
        "0 years 0 mons 1 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '1 day 1'",
        "0 years 0 mons 1 days 0 hours 0 mins 1.00 secs"
    );
    test_expression!(
        "interval '0.5'",
        "0 years 0 mons 0 days 0 hours 0 mins 0.500 secs"
    );
    test_expression!(
        "interval '0.5 day 1'",
        "0 years 0 mons 0 days 12 hours 0 mins 1.00 secs"
    );
    test_expression!(
        "interval '0.49 day'",
        "0 years 0 mons 0 days 11 hours 45 mins 36.00 secs"
    );
    test_expression!(
        "interval '0.499 day'",
        "0 years 0 mons 0 days 11 hours 58 mins 33.596 secs"
    );
    test_expression!(
        "interval '0.4999 day'",
        "0 years 0 mons 0 days 11 hours 59 mins 51.364 secs"
    );
    test_expression!(
        "interval '0.49999 day'",
        "0 years 0 mons 0 days 11 hours 59 mins 59.136 secs"
    );
    test_expression!(
        "interval '0.49999999999 day'",
        "0 years 0 mons 0 days 12 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '5 day'",
        "0 years 0 mons 5 days 0 hours 0 mins 0.00 secs"
    );
    // Hour is ignored, this matches PostgreSQL
    test_expression!(
        "interval '5 day' hour",
        "0 years 0 mons 5 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '5 day 4 hours 3 minutes 2 seconds 100 milliseconds'",
        "0 years 0 mons 5 days 4 hours 3 mins 2.100 secs"
    );
    test_expression!(
        "interval '0.5 month'",
        "0 years 0 mons 15 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '0.5' month",
        "0 years 0 mons 15 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '1 month'",
        "0 years 1 mons 0 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '1' MONTH",
        "0 years 1 mons 0 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '5 month'",
        "0 years 5 mons 0 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '13 month'",
        "1 years 1 mons 0 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '0.5 year'",
        "0 years 6 mons 0 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '1 year'",
        "1 years 0 mons 0 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '2 year'",
        "2 years 0 mons 0 days 0 hours 0 mins 0.00 secs"
    );
    test_expression!(
        "interval '2' year",
        "2 years 0 mons 0 days 0 hours 0 mins 0.00 secs"
    );
    Ok(())
}

#[tokio::test]
async fn test_string_expressions() -> Result<()> {
    test_expression!("ascii('')", "0");
    test_expression!("ascii('x')", "120");
    test_expression!("ascii(NULL)", "NULL");
    test_expression!("bit_length('')", "0");
    test_expression!("bit_length('chars')", "40");
    test_expression!("bit_length('josé')", "40");
    test_expression!("bit_length(NULL)", "NULL");
    test_expression!("btrim(' xyxtrimyyx ', NULL)", "NULL");
    test_expression!("btrim(' xyxtrimyyx ')", "xyxtrimyyx");
    test_expression!("btrim('\n xyxtrimyyx \n')", "\n xyxtrimyyx \n");
    test_expression!("btrim('xyxtrimyyx', 'xyz')", "trim");
    test_expression!("btrim('\nxyxtrimyyx\n', 'xyz\n')", "trim");
    test_expression!("btrim(NULL, 'xyz')", "NULL");
    test_expression!("chr(CAST(120 AS int))", "x");
    test_expression!("chr(CAST(128175 AS int))", "💯");
    test_expression!("chr(CAST(NULL AS int))", "NULL");
    test_expression!("concat('a','b','c')", "abc");
    test_expression!("concat('abcde', 2, NULL, 22)", "abcde222");
    test_expression!("concat(NULL)", "");
    test_expression!("concat_ws(',', 'abcde', 2, NULL, 22)", "abcde,2,22");
    test_expression!("concat_ws('|','a','b','c')", "a|b|c");
    test_expression!("concat_ws('|',NULL)", "");
    test_expression!("concat_ws(NULL,'a',NULL,'b','c')", "NULL");
    test_expression!("initcap('')", "");
    test_expression!("initcap('hi THOMAS')", "Hi Thomas");
    test_expression!("initcap(NULL)", "NULL");
    test_expression!("lower('')", "");
    test_expression!("lower('TOM')", "tom");
    test_expression!("lower(NULL)", "NULL");
    test_expression!("ltrim(' zzzytest ', NULL)", "NULL");
    test_expression!("ltrim(' zzzytest ')", "zzzytest ");
    test_expression!("ltrim('zzzytest', 'xyz')", "test");
    test_expression!("ltrim(NULL, 'xyz')", "NULL");
    test_expression!("octet_length('')", "0");
    test_expression!("octet_length('chars')", "5");
    test_expression!("octet_length('josé')", "5");
    test_expression!("octet_length(NULL)", "NULL");
    test_expression!("repeat('Pg', 4)", "PgPgPgPg");
    test_expression!("repeat('Pg', CAST(NULL AS INT))", "NULL");
    test_expression!("repeat(NULL, 4)", "NULL");
    test_expression!("replace('abcdefabcdef', 'cd', 'XX')", "abXXefabXXef");
    test_expression!("replace('abcdefabcdef', 'cd', NULL)", "NULL");
    test_expression!("replace('abcdefabcdef', 'notmatch', 'XX')", "abcdefabcdef");
    test_expression!("replace('abcdefabcdef', NULL, 'XX')", "NULL");
    test_expression!("replace(NULL, 'cd', 'XX')", "NULL");
    test_expression!("rtrim(' testxxzx ')", " testxxzx");
    test_expression!("rtrim(' zzzytest ', NULL)", "NULL");
    test_expression!("rtrim('testxxzx', 'xyz')", "test");
    test_expression!("rtrim(NULL, 'xyz')", "NULL");
    test_expression!("split_part('abc~@~def~@~ghi', '~@~', 2)", "def");
    test_expression!("split_part('abc~@~def~@~ghi', '~@~', 20)", "");
    test_expression!("split_part(NULL, '~@~', 20)", "NULL");
    test_expression!("split_part('abc~@~def~@~ghi', NULL, 20)", "NULL");
    test_expression!(
        "split_part('abc~@~def~@~ghi', '~@~', CAST(NULL AS INT))",
        "NULL"
    );
    test_expression!("starts_with('alphabet', 'alph')", "true");
    test_expression!("starts_with('alphabet', 'blph')", "false");
    test_expression!("starts_with(NULL, 'blph')", "NULL");
    test_expression!("starts_with('alphabet', NULL)", "NULL");
    test_expression!("to_hex(2147483647)", "7fffffff");
    test_expression!("to_hex(9223372036854775807)", "7fffffffffffffff");
    test_expression!("to_hex(CAST(NULL AS int))", "NULL");
    test_expression!("trim(' tom ')", "tom");
    test_expression!("trim(LEADING ' ' FROM ' tom ')", "tom ");
    test_expression!("trim(TRAILING ' ' FROM ' tom ')", " tom");
    test_expression!("trim(BOTH ' ' FROM ' tom ')", "tom");
    test_expression!("trim(LEADING 'x' FROM 'xxxtomxxx')", "tomxxx");
    test_expression!("trim(TRAILING 'x' FROM 'xxxtomxxx')", "xxxtom");
    test_expression!("trim(BOTH 'x' FROM 'xxxtomxx')", "tom");
    test_expression!("trim(LEADING 'xy' FROM 'xyxabcxyzdefxyx')", "abcxyzdefxyx");
    test_expression!("trim(TRAILING 'xy' FROM 'xyxabcxyzdefxyx')", "xyxabcxyzdef");
    test_expression!("trim(BOTH 'xy' FROM 'xyxabcxyzdefxyx')", "abcxyzdef");
    test_expression!("trim(' tom')", "tom");
    test_expression!("trim('')", "");
    test_expression!("trim('tom ')", "tom");
    test_expression!("upper('')", "");
    test_expression!("upper('tom')", "TOM");
    test_expression!("upper(NULL)", "NULL");
    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "unicode_expressions"), ignore)]
async fn test_unicode_expressions() -> Result<()> {
    test_expression!("char_length('')", "0");
    test_expression!("char_length('chars')", "5");
    test_expression!("char_length('josé')", "4");
    test_expression!("char_length(NULL)", "NULL");
    test_expression!("character_length('')", "0");
    test_expression!("character_length('chars')", "5");
    test_expression!("character_length('josé')", "4");
    test_expression!("character_length(NULL)", "NULL");
    test_expression!("left('abcde', -2)", "abc");
    test_expression!("left('abcde', -200)", "");
    test_expression!("left('abcde', 0)", "");
    test_expression!("left('abcde', 2)", "ab");
    test_expression!("left('abcde', 200)", "abcde");
    test_expression!("left('abcde', CAST(NULL AS INT))", "NULL");
    test_expression!("left(NULL, 2)", "NULL");
    test_expression!("left(NULL, CAST(NULL AS INT))", "NULL");
    test_expression!("length('')", "0");
    test_expression!("length('chars')", "5");
    test_expression!("length('josé')", "4");
    test_expression!("length(NULL)", "NULL");
    test_expression!("lpad('hi', 5, 'xy')", "xyxhi");
    test_expression!("lpad('hi', 0)", "");
    test_expression!("lpad('hi', 21, 'abcdef')", "abcdefabcdefabcdefahi");
    test_expression!("lpad('hi', 5, 'xy')", "xyxhi");
    test_expression!("lpad('hi', 5, NULL)", "NULL");
    test_expression!("lpad('hi', 5)", "   hi");
    test_expression!("lpad('hi', CAST(NULL AS INT), 'xy')", "NULL");
    test_expression!("lpad('hi', CAST(NULL AS INT))", "NULL");
    test_expression!("lpad('xyxhi', 3)", "xyx");
    test_expression!("lpad(NULL, 0)", "NULL");
    test_expression!("lpad(NULL, 5, 'xy')", "NULL");
    test_expression!("reverse('abcde')", "edcba");
    test_expression!("reverse('loẅks')", "skẅol");
    test_expression!("reverse(NULL)", "NULL");
    test_expression!("right('abcde', -2)", "cde");
    test_expression!("right('abcde', -200)", "");
    test_expression!("right('abcde', 0)", "");
    test_expression!("right('abcde', 2)", "de");
    test_expression!("right('abcde', 200)", "abcde");
    test_expression!("right('abcde', CAST(NULL AS INT))", "NULL");
    test_expression!("right(NULL, 2)", "NULL");
    test_expression!("right(NULL, CAST(NULL AS INT))", "NULL");
    test_expression!("rpad('hi', 5, 'xy')", "hixyx");
    test_expression!("rpad('hi', 0)", "");
    test_expression!("rpad('hi', 21, 'abcdef')", "hiabcdefabcdefabcdefa");
    test_expression!("rpad('hi', 5, 'xy')", "hixyx");
    test_expression!("rpad('hi', 5, NULL)", "NULL");
    test_expression!("rpad('hi', 5)", "hi   ");
    test_expression!("rpad('hi', CAST(NULL AS INT), 'xy')", "NULL");
    test_expression!("rpad('hi', CAST(NULL AS INT))", "NULL");
    test_expression!("rpad('xyxhi', 3)", "xyx");
    test_expression!("strpos('abc', 'c')", "3");
    test_expression!("strpos('josé', 'é')", "4");
    test_expression!("strpos('joséésoj', 'so')", "6");
    test_expression!("strpos('joséésoj', 'abc')", "0");
    test_expression!("strpos(NULL, 'abc')", "NULL");
    test_expression!("strpos('joséésoj', NULL)", "NULL");
    test_expression!("substr('alphabet', -3)", "alphabet");
    test_expression!("substr('alphabet', 0)", "alphabet");
    test_expression!("substr('alphabet', 1)", "alphabet");
    test_expression!("substr('alphabet', 2)", "lphabet");
    test_expression!("substr('alphabet', 3)", "phabet");
    test_expression!("substr('alphabet', 30)", "");
    test_expression!("substr('alphabet', CAST(NULL AS int))", "NULL");
    test_expression!("substr('alphabet', 3, 2)", "ph");
    test_expression!("substr('alphabet', 3, 20)", "phabet");
    test_expression!("substr('alphabet', CAST(NULL AS int), 20)", "NULL");
    test_expression!("substr('alphabet', 3, CAST(NULL AS int))", "NULL");
    test_expression!("translate('12345', '143', 'ax')", "a2x5");
    test_expression!("translate(NULL, '143', 'ax')", "NULL");
    test_expression!("translate('12345', NULL, 'ax')", "NULL");
    test_expression!("translate('12345', '143', NULL)", "NULL");
    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "regex_expressions"), ignore)]
async fn test_regex_expressions() -> Result<()> {
    test_expression!("regexp_replace('ABCabcABC', '(abc)', 'X', 'gi')", "XXX");
    test_expression!("regexp_replace('ABCabcABC', '(abc)', 'X', 'i')", "XabcABC");
    test_expression!("regexp_replace('foobarbaz', 'b..', 'X', 'g')", "fooXX");
    test_expression!("regexp_replace('foobarbaz', 'b..', 'X')", "fooXbaz");
    test_expression!(
        "regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', 'g')",
        "fooXarYXazY"
    );
    test_expression!(
        "regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', NULL)",
        "NULL"
    );
    test_expression!("regexp_replace('foobarbaz', 'b(..)', NULL, 'g')", "NULL");
    test_expression!("regexp_replace('foobarbaz', NULL, 'X\\1Y', 'g')", "NULL");
    test_expression!("regexp_replace('Thomas', '.[mN]a.', 'M')", "ThM");
    test_expression!("regexp_replace(NULL, 'b(..)', 'X\\1Y', 'g')", "NULL");
    test_expression!("regexp_match('foobarbequebaz', '')", "[]");
    test_expression!(
        "regexp_match('foobarbequebaz', '(bar)(beque)')",
        "[bar, beque]"
    );
    test_expression!("regexp_match('foobarbequebaz', '(ba3r)(bequ34e)')", "NULL");
    test_expression!("regexp_match('aaa-0', '.*-(\\d)')", "[0]");
    test_expression!("regexp_match('bb-1', '.*-(\\d)')", "[1]");
    test_expression!("regexp_match('aa', '.*-(\\d)')", "NULL");
    test_expression!("regexp_match(NULL, '.*-(\\d)')", "NULL");
    test_expression!("regexp_match('aaa-0', NULL)", "NULL");
    Ok(())
}

#[tokio::test]
async fn test_extract_date_part() -> Result<()> {
    test_expression!("date_part('hour', CAST('2020-01-01' AS DATE))", "0");
    test_expression!("EXTRACT(HOUR FROM CAST('2020-01-01' AS DATE))", "0");
    test_expression!(
        "EXTRACT(HOUR FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "12"
    );
    test_expression!("date_part('YEAR', CAST('2000-01-01' AS DATE))", "2000");
    test_expression!(
        "EXTRACT(year FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "2020"
    );
    Ok(())
}

#[tokio::test]
async fn test_in_list_scalar() -> Result<()> {
    test_expression!("'a' IN ('a','b')", "true");
    test_expression!("'c' IN ('a','b')", "false");
    test_expression!("'c' NOT IN ('a','b')", "true");
    test_expression!("'a' NOT IN ('a','b')", "false");
    test_expression!("NULL IN ('a','b')", "NULL");
    test_expression!("NULL NOT IN ('a','b')", "NULL");
    test_expression!("'a' IN ('a','b',NULL)", "true");
    test_expression!("'c' IN ('a','b',NULL)", "NULL");
    test_expression!("'a' NOT IN ('a','b',NULL)", "false");
    test_expression!("'c' NOT IN ('a','b',NULL)", "NULL");
    test_expression!("0 IN (0,1,2)", "true");
    test_expression!("3 IN (0,1,2)", "false");
    test_expression!("3 NOT IN (0,1,2)", "true");
    test_expression!("0 NOT IN (0,1,2)", "false");
    test_expression!("NULL IN (0,1,2)", "NULL");
    test_expression!("NULL NOT IN (0,1,2)", "NULL");
    test_expression!("0 IN (0,1,2,NULL)", "true");
    test_expression!("3 IN (0,1,2,NULL)", "NULL");
    test_expression!("0 NOT IN (0,1,2,NULL)", "false");
    test_expression!("3 NOT IN (0,1,2,NULL)", "NULL");
    test_expression!("0.0 IN (0.0,0.1,0.2)", "true");
    test_expression!("0.3 IN (0.0,0.1,0.2)", "false");
    test_expression!("0.3 NOT IN (0.0,0.1,0.2)", "true");
    test_expression!("0.0 NOT IN (0.0,0.1,0.2)", "false");
    test_expression!("NULL IN (0.0,0.1,0.2)", "NULL");
    test_expression!("NULL NOT IN (0.0,0.1,0.2)", "NULL");
    test_expression!("0.0 IN (0.0,0.1,0.2,NULL)", "true");
    test_expression!("0.3 IN (0.0,0.1,0.2,NULL)", "NULL");
    test_expression!("0.0 NOT IN (0.0,0.1,0.2,NULL)", "false");
    test_expression!("0.3 NOT IN (0.0,0.1,0.2,NULL)", "NULL");
    test_expression!("'1' IN ('a','b',1)", "true");
    test_expression!("'2' IN ('a','b',1)", "false");
    test_expression!("'2' NOT IN ('a','b',1)", "true");
    test_expression!("'1' NOT IN ('a','b',1)", "false");
    test_expression!("NULL IN ('a','b',1)", "NULL");
    test_expression!("NULL NOT IN ('a','b',1)", "NULL");
    test_expression!("'1' IN ('a','b',NULL,1)", "true");
    test_expression!("'2' IN ('a','b',NULL,1)", "NULL");
    test_expression!("'1' NOT IN ('a','b',NULL,1)", "false");
    test_expression!("'2' NOT IN ('a','b',NULL,1)", "NULL");
    Ok(())
}

#[tokio::test]
async fn in_list_array() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    let sql = "SELECT
            c1 IN ('a', 'c') AS utf8_in_true
            ,c1 IN ('x', 'y') AS utf8_in_false
            ,c1 NOT IN ('x', 'y') AS utf8_not_in_true
            ,c1 NOT IN ('a', 'c') AS utf8_not_in_false
            ,NULL IN ('a', 'c') AS utf8_in_null
        FROM aggregate_test_100 WHERE c12 < 0.05";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
            "+--------------+---------------+------------------+-------------------+--------------+",
            "| utf8_in_true | utf8_in_false | utf8_not_in_true | utf8_not_in_false | utf8_in_null |",
            "+--------------+---------------+------------------+-------------------+--------------+",
            "| true         | false         | true             | false             |              |",
            "| true         | false         | true             | false             |              |",
            "| true         | false         | true             | false             |              |",
            "| false        | false         | true             | true              |              |",
            "| false        | false         | true             | true              |              |",
            "| false        | false         | true             | true              |              |",
            "| false        | false         | true             | true              |              |",
            "+--------------+---------------+------------------+-------------------+--------------+",
        ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

// TODO Tests to prove correct implementation of INNER JOIN's with qualified names.
//  https://issues.apache.org/jira/projects/ARROW/issues/ARROW-11432.
#[tokio::test]
#[ignore]
async fn inner_join_qualified_names() -> Result<()> {
    // Setup the statements that test qualified names function correctly.
    let equivalent_sql = [
        "SELECT t1.a, t1.b, t1.c, t2.a, t2.b, t2.c
            FROM t1
            INNER JOIN t2 ON t1.a = t2.a
            ORDER BY t1.a",
        "SELECT t1.a, t1.b, t1.c, t2.a, t2.b, t2.c
            FROM t1
            INNER JOIN t2 ON t2.a = t1.a
            ORDER BY t1.a",
    ];

    let expected = vec![
        "+---+----+----+---+-----+-----+",
        "| a | b  | c  | a | b   | c   |",
        "+---+----+----+---+-----+-----+",
        "| 1 | 10 | 50 | 1 | 100 | 500 |",
        "| 2 | 20 | 60 | 2 | 200 | 600 |",
        "| 4 | 40 | 80 | 4 | 400 | 800 |",
        "+---+----+----+---+-----+-----+",
    ];

    for sql in equivalent_sql.iter() {
        let mut ctx = create_join_context_qualified()?;
        let actual = execute_to_batches(&mut ctx, sql).await;
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn inner_join_nulls() {
    let sql = "SELECT * FROM (SELECT null AS id1) t1
            INNER JOIN (SELECT null AS id2) t2 ON id1 = id2";

    let expected = vec!["++", "++"];

    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;

    // left and right shouldn't match anything
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn qualified_table_references() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;

    for table_ref in &[
        "aggregate_test_100",
        "public.aggregate_test_100",
        "datafusion.public.aggregate_test_100",
    ] {
        let sql = format!("SELECT COUNT(*) FROM {}", table_ref);
        let actual = execute_to_batches(&mut ctx, &sql).await;
        let expected = vec![
            "+-----------------+",
            "| COUNT(UInt8(1)) |",
            "+-----------------+",
            "| 100             |",
            "+-----------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn qualified_table_references_and_fields() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let c1: StringArray = vec!["foofoo", "foobar", "foobaz"]
        .into_iter()
        .map(Some)
        .collect();
    let c2: Int64Array = vec![1, 2, 3].into_iter().map(Some).collect();
    let c3: Int64Array = vec![10, 20, 30].into_iter().map(Some).collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("f.c1", Arc::new(c1) as ArrayRef),
        //  evil -- use the same name as the table
        ("test.c2", Arc::new(c2) as ArrayRef),
        //  more evil still
        ("....", Arc::new(c3) as ArrayRef),
    ])?;

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("test", Arc::new(table))?;

    // referring to the unquoted column is an error
    let sql = r#"SELECT f1.c1 from test"#;
    let error = ctx.create_logical_plan(sql).unwrap_err();
    assert_contains!(
        error.to_string(),
        "No field named 'f1.c1'. Valid fields are 'test.f.c1', 'test.test.c2'"
    );

    // however, enclosing it in double quotes is ok
    let sql = r#"SELECT "f.c1" from test"#;
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------+",
        "| f.c1   |",
        "+--------+",
        "| foofoo |",
        "| foobar |",
        "| foobaz |",
        "+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    // Works fully qualified too
    let sql = r#"SELECT test."f.c1" from test"#;
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    // check that duplicated table name and column name are ok
    let sql = r#"SELECT "test.c2" as expr1, test."test.c2" as expr2 from test"#;
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+-------+",
        "| expr1 | expr2 |",
        "+-------+-------+",
        "| 1     | 1     |",
        "| 2     | 2     |",
        "| 3     | 3     |",
        "+-------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // check that '....' is also an ok column name (in the sense that
    // datafusion should run the query, not that someone should write
    // this
    let sql = r#"SELECT "....", "...." as c3 from test order by "....""#;
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+------+----+",
        "| .... | c3 |",
        "+------+----+",
        "| 10   | 10 |",
        "| 20   | 20 |",
        "| 30   | 30 |",
        "+------+----+",
    ];
    assert_batches_eq!(expected, &actual);
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

#[tokio::test]
async fn test_cast_expressions() -> Result<()> {
    test_expression!("CAST('0' AS INT)", "0");
    test_expression!("CAST(NULL AS INT)", "NULL");
    test_expression!("TRY_CAST('0' AS INT)", "0");
    test_expression!("TRY_CAST('x' AS INT)", "NULL");
    Ok(())
}

#[tokio::test]
async fn test_current_timestamp_expressions() -> Result<()> {
    let t1 = chrono::Utc::now().timestamp();
    let mut ctx = ExecutionContext::new();
    let actual = execute(&mut ctx, "SELECT NOW(), NOW() as t2").await;
    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();
    let t3 = chrono::Utc::now().timestamp();
    let t2_naive =
        chrono::NaiveDateTime::parse_from_str(res1, "%Y-%m-%d %H:%M:%S%.6f").unwrap();

    let t2 = t2_naive.timestamp();
    assert!(t1 <= t2 && t2 <= t3);
    assert_eq!(res2, res1);

    Ok(())
}

#[tokio::test]
async fn test_current_timestamp_expressions_non_optimized() -> Result<()> {
    let t1 = chrono::Utc::now().timestamp();
    let ctx = ExecutionContext::new();
    let sql = "SELECT NOW(), NOW() as t2";

    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx.create_logical_plan(sql).expect(&msg);

    let msg = format!("Creating physical plan for '{}': {:?}", sql, plan);
    let plan = ctx.create_physical_plan(&plan).await.expect(&msg);

    let msg = format!("Executing physical plan for '{}': {:?}", sql, plan);
    let res = collect(plan).await.expect(&msg);
    let actual = result_vec(&res);

    let res1 = actual[0][0].as_str();
    let res2 = actual[0][1].as_str();
    let t3 = chrono::Utc::now().timestamp();
    let t2_naive =
        chrono::NaiveDateTime::parse_from_str(res1, "%Y-%m-%d %H:%M:%S%.6f").unwrap();

    let t2 = t2_naive.timestamp();
    assert!(t1 <= t2 && t2 <= t3);
    assert_eq!(res2, res1);

    Ok(())
}

#[tokio::test]
async fn test_random_expression() -> Result<()> {
    let mut ctx = create_ctx()?;
    let sql = "SELECT random() r1";
    let actual = execute(&mut ctx, sql).await;
    let r1 = actual[0][0].parse::<f64>().unwrap();
    assert!(0.0 <= r1);
    assert!(r1 < 1.0);
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
async fn test_physical_plan_display_indent() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = ExecutionConfig::new().with_target_partitions(3);
    let mut ctx = ExecutionContext::with_config(config);
    register_aggregate_csv(&mut ctx).await.unwrap();
    let sql = "SELECT c1, MAX(c12), MIN(c12) as the_min \
               FROM aggregate_test_100 \
               WHERE c12 < 10 \
               GROUP BY c1 \
               ORDER BY the_min DESC \
               LIMIT 10";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();

    let physical_plan = ctx.create_physical_plan(&plan).await.unwrap();
    let expected = vec![
        "GlobalLimitExec: limit=10",
        "  SortExec: [the_min@2 DESC]",
        "    CoalescePartitionsExec",
        "      ProjectionExec: expr=[c1@0 as c1, MAX(aggregate_test_100.c12)@1 as MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)@2 as the_min]",
        "        HashAggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)]",
        "          CoalesceBatchesExec: target_batch_size=4096",
        "            RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 3)",
        "              HashAggregateExec: mode=Partial, gby=[c1@0 as c1], aggr=[MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)]",
        "                CoalesceBatchesExec: target_batch_size=4096",
        "                  FilterExec: c12@1 < CAST(10 AS Float64)",
        "                    RepartitionExec: partitioning=RoundRobinBatch(3)",
        "                      CsvExec: files=[ARROW_TEST_DATA/csv/aggregate_test_100.csv], has_header=true, batch_size=8192, limit=None",
    ];

    let data_path = datafusion::test_util::arrow_test_data();
    let actual = format!("{}", displayable(physical_plan.as_ref()).indent())
        .trim()
        .lines()
        // normalize paths
        .map(|s| s.replace(&data_path, "ARROW_TEST_DATA"))
        .collect::<Vec<_>>();

    assert_eq!(
        expected, actual,
        "expected:\n{:#?}\nactual:\n\n{:#?}\n",
        expected, actual
    );
}

#[tokio::test]
async fn test_physical_plan_display_indent_multi_children() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = ExecutionConfig::new().with_target_partitions(3);
    let mut ctx = ExecutionContext::with_config(config);
    // ensure indenting works for nodes with multiple children
    register_aggregate_csv(&mut ctx).await.unwrap();
    let sql = "SELECT c1 \
               FROM (select c1 from aggregate_test_100) AS a \
               JOIN\
               (select c1 as c2 from aggregate_test_100) AS b \
               ON c1=c2\
               ";

    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();

    let physical_plan = ctx.create_physical_plan(&plan).await.unwrap();
    let expected = vec![
        "ProjectionExec: expr=[c1@0 as c1]",
        "  CoalesceBatchesExec: target_batch_size=4096",
        "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(Column { name: \"c1\", index: 0 }, Column { name: \"c2\", index: 0 })]",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 3)",
        "          ProjectionExec: expr=[c1@0 as c1]",
        "            ProjectionExec: expr=[c1@0 as c1]",
        "              RepartitionExec: partitioning=RoundRobinBatch(3)",
        "                CsvExec: files=[ARROW_TEST_DATA/csv/aggregate_test_100.csv], has_header=true, batch_size=8192, limit=None",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        RepartitionExec: partitioning=Hash([Column { name: \"c2\", index: 0 }], 3)",
        "          ProjectionExec: expr=[c2@0 as c2]",
        "            ProjectionExec: expr=[c1@0 as c2]",
        "              RepartitionExec: partitioning=RoundRobinBatch(3)",
        "                CsvExec: files=[ARROW_TEST_DATA/csv/aggregate_test_100.csv], has_header=true, batch_size=8192, limit=None",
    ];

    let data_path = datafusion::test_util::arrow_test_data();
    let actual = format!("{}", displayable(physical_plan.as_ref()).indent())
        .trim()
        .lines()
        // normalize paths
        .map(|s| s.replace(&data_path, "ARROW_TEST_DATA"))
        .collect::<Vec<_>>();

    assert_eq!(
        expected, actual,
        "expected:\n{:#?}\nactual:\n\n{:#?}\n",
        expected, actual
    );
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

// Normalizes parts of an explain plan that vary from run to run (such as path)
fn normalize_for_explain(s: &str) -> String {
    // Convert things like /Users/alamb/Software/arrow/testing/data/csv/aggregate_test_100.csv
    // to ARROW_TEST_DATA/csv/aggregate_test_100.csv
    let data_path = datafusion::test_util::arrow_test_data();
    let s = s.replace(&data_path, "ARROW_TEST_DATA");

    // convert things like partitioning=RoundRobinBatch(16)
    // to partitioning=RoundRobinBatch(NUM_CORES)
    let needle = format!("RoundRobinBatch({})", num_cpus::get());
    s.replace(&needle, "RoundRobinBatch(NUM_CORES)")
}

/// Applies normalize_for_explain to every line
fn normalize_vec_for_explain(v: Vec<Vec<String>>) -> Vec<Vec<String>> {
    v.into_iter()
        .map(|l| {
            l.into_iter()
                .map(|s| normalize_for_explain(&s))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

#[tokio::test]
async fn test_partial_qualified_name() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let sql = "SELECT t1.t1_id, t1_name FROM public.t1";
    let expected = vec![
        "+-------+---------+",
        "| t1_id | t1_name |",
        "+-------+---------+",
        "| 11    | a       |",
        "| 22    | b       |",
        "| 33    | c       |",
        "| 44    | d       |",
        "+-------+---------+",
    ];
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like_on_strings() -> Result<()> {
    let input = vec![Some("foo"), Some("bar"), None, Some("fazzz")]
        .into_iter()
        .collect::<StringArray>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 LIKE '%a%'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| bar   |",
        "| fazzz |",
        "+-------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn like_on_string_dictionaries() -> Result<()> {
    let input = vec![Some("foo"), Some("bar"), None, Some("fazzz")]
        .into_iter()
        .collect::<DictionaryArray<Int32Type>>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 LIKE '%a%'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| bar   |",
        "| fazzz |",
        "+-------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_regexp_is_match() -> Result<()> {
    let input = vec![Some("foo"), Some("Barrr"), Some("Bazzz"), Some("ZZZZZ")]
        .into_iter()
        .collect::<StringArray>();

    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let mut ctx = ExecutionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT * FROM test WHERE c1 ~ 'z'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| Bazzz |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 ~* 'z'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| Bazzz |",
        "| ZZZZZ |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 !~ 'z'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| foo   |",
        "| Barrr |",
        "| ZZZZZ |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "SELECT * FROM test WHERE c1 !~* 'z'";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| c1    |",
        "+-------+",
        "| foo   |",
        "| Barrr |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn join_tables_with_duplicated_column_name_not_in_on_constraint() -> Result<()> {
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as _),
        (
            "country",
            Arc::new(StringArray::from(vec!["Germany", "Sweden", "Japan"])) as _,
        ),
    ])
    .unwrap();
    let countries = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7])) as _,
        ),
        (
            "city",
            Arc::new(StringArray::from(vec![
                "Hamburg",
                "Stockholm",
                "Osaka",
                "Berlin",
                "Göteborg",
                "Tokyo",
                "Kyoto",
            ])) as _,
        ),
        (
            "country_id",
            Arc::new(Int32Array::from(vec![1, 2, 3, 1, 2, 3, 3])) as _,
        ),
    ])
    .unwrap();
    let cities = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let mut ctx = ExecutionContext::new();
    ctx.register_table("countries", Arc::new(countries))?;
    ctx.register_table("cities", Arc::new(cities))?;

    // city.id is not in the on constraint, but the output result will contain both city.id and
    // country.id
    let sql = "SELECT t1.id, t2.id, t1.city, t2.country FROM cities AS t1 JOIN countries AS t2 ON t1.country_id = t2.id ORDER BY t1.id";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+----+-----------+---------+",
        "| id | id | city      | country |",
        "+----+----+-----------+---------+",
        "| 1  | 1  | Hamburg   | Germany |",
        "| 2  | 2  | Stockholm | Sweden  |",
        "| 3  | 3  | Osaka     | Japan   |",
        "| 4  | 1  | Berlin    | Germany |",
        "| 5  | 2  | Göteborg  | Sweden  |",
        "| 6  | 3  | Tokyo     | Japan   |",
        "| 7  | 3  | Kyoto     | Japan   |",
        "+----+----+-----------+---------+",
    ];

    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn avro_query() {
    let mut ctx = ExecutionContext::new();
    register_alltypes_avro(&mut ctx).await;
    // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
    // so we need an explicit cast
    let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------------------+",
        "| id | CAST(alltypes_plain.string_col AS Utf8) |",
        "+----+-----------------------------------------+",
        "| 4  | 0                                       |",
        "| 5  | 1                                       |",
        "| 6  | 0                                       |",
        "| 7  | 1                                       |",
        "| 2  | 0                                       |",
        "| 3  | 1                                       |",
        "| 0  | 0                                       |",
        "| 1  | 1                                       |",
        "+----+-----------------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn avro_query_multiple_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let table_path = tempdir.path();
    let testdata = datafusion::test_util::arrow_test_data();
    let alltypes_plain_file = format!("{}/avro/alltypes_plain.avro", testdata);
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain1.avro", table_path.display()),
    )
    .unwrap();
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain2.avro", table_path.display()),
    )
    .unwrap();

    let mut ctx = ExecutionContext::new();
    ctx.register_avro(
        "alltypes_plain",
        table_path.display().to_string().as_str(),
        AvroReadOptions::default(),
    )
    .await
    .unwrap();
    // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
    // so we need an explicit cast
    let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-----------------------------------------+",
        "| id | CAST(alltypes_plain.string_col AS Utf8) |",
        "+----+-----------------------------------------+",
        "| 4  | 0                                       |",
        "| 5  | 1                                       |",
        "| 6  | 0                                       |",
        "| 7  | 1                                       |",
        "| 2  | 0                                       |",
        "| 3  | 1                                       |",
        "| 0  | 0                                       |",
        "| 1  | 1                                       |",
        "| 4  | 0                                       |",
        "| 5  | 1                                       |",
        "| 6  | 0                                       |",
        "| 7  | 1                                       |",
        "| 2  | 0                                       |",
        "| 3  | 1                                       |",
        "| 0  | 0                                       |",
        "| 1  | 1                                       |",
        "+----+-----------------------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn avro_single_nan_schema() {
    let mut ctx = ExecutionContext::new();
    let testdata = datafusion::test_util::arrow_test_data();
    ctx.register_avro(
        "single_nan",
        &format!("{}/avro/single_nan.avro", testdata),
        AvroReadOptions::default(),
    )
    .await
    .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let results = collect(plan).await.unwrap();
    for batch in results {
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn avro_explain() {
    let mut ctx = ExecutionContext::new();
    register_alltypes_avro(&mut ctx).await;

    let sql = "EXPLAIN SELECT count(*) from alltypes_plain";
    let actual = execute(&mut ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    let expected = vec![
        vec![
            "logical_plan",
            "Projection: #COUNT(UInt8(1))\
            \n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
            \n    TableScan: alltypes_plain projection=Some([0])",
        ],
        vec![
            "physical_plan",
            "ProjectionExec: expr=[COUNT(UInt8(1))@0 as COUNT(UInt8(1))]\
            \n  HashAggregateExec: mode=Final, gby=[], aggr=[COUNT(UInt8(1))]\
            \n    CoalescePartitionsExec\
            \n      HashAggregateExec: mode=Partial, gby=[], aggr=[COUNT(UInt8(1))]\
            \n        RepartitionExec: partitioning=RoundRobinBatch(NUM_CORES)\
            \n          AvroExec: files=[ARROW_TEST_DATA/avro/alltypes_plain.avro], batch_size=8192, limit=None\
            \n",
        ],
    ];
    assert_eq!(expected, actual);
}

#[tokio::test]
async fn union_distinct() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT 1 as x UNION SELECT 1 as x";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+---+", "| x |", "+---+", "| 1 |", "+---+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn union_all_with_aggregate() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql =
        "SELECT SUM(d) FROM (SELECT 1 as c, 2 as d UNION ALL SELECT 1 as c, 3 AS d) as a";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| SUM(a.d) |",
        "+----------+",
        "| 5        |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn case_with_bool_type_result() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "select case when 'cpu' != 'cpu' then true else false end";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------------------------------------------------------------------------------+",
        "| CASE WHEN Utf8(\"cpu\") != Utf8(\"cpu\") THEN Boolean(true) ELSE Boolean(false) END |",
        "+---------------------------------------------------------------------------------+",
        "| false                                                                           |",
        "+---------------------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn use_between_expression_in_select_query() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    let sql = "SELECT 1 NOT BETWEEN 3 AND 5";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------+",
        "| Int64(1) NOT BETWEEN Int64(3) AND Int64(5) |",
        "+--------------------------------------------+",
        "| true                                       |",
        "+--------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let input = Int64Array::from(vec![1, 2, 3, 4]);
    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT abs(c1) BETWEEN 0 AND LoG(c1 * 100 ) FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    // Expect field name to be correctly converted for expr, low and high.
    let expected = vec![
        "+--------------------------------------------------------------------+",
        "| abs(test.c1) BETWEEN Int64(0) AND log(test.c1 Multiply Int64(100)) |",
        "+--------------------------------------------------------------------+",
        "| true                                                               |",
        "| true                                                               |",
        "| false                                                              |",
        "| false                                                              |",
        "+--------------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "EXPLAIN SELECT c1 BETWEEN 2 AND 3 FROM test";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual).unwrap();

    // Only test that the projection exprs arecorrect, rather than entire output
    let needle = "ProjectionExec: expr=[c1@0 >= 2 AND c1@0 <= 3 as test.c1 BETWEEN Int64(2) AND Int64(3)]";
    assert_contains!(&formatted, needle);
    let needle = "Projection: #test.c1 BETWEEN Int64(2) AND Int64(3)";
    assert_contains!(&formatted, needle);

    Ok(())
}

// --- End Test Porting ---

#[tokio::test]
async fn query_get_indexed_field() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_list",
        DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
        false,
    )]));
    let builder = PrimitiveBuilder::<Int64Type>::new(3);
    let mut lb = ListBuilder::new(builder);
    for int_vec in vec![vec![0, 1, 2], vec![4, 5, 6], vec![7, 8, 9]] {
        let builder = lb.values();
        for int in int_vec {
            builder.append_value(int).unwrap();
        }
        lb.append(true).unwrap();
    }

    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    let table_a = Arc::new(table);

    ctx.register_table("ints", table_a)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_list[0] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+", "| i0 |", "+----+", "| 0  |", "| 4  |", "| 7  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_nested_get_indexed_field() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let nested_dt = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
    // Nested schema of { "some_list": [[i64]] }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_list",
        DataType::List(Box::new(Field::new("item", nested_dt.clone(), true))),
        false,
    )]));

    let builder = PrimitiveBuilder::<Int64Type>::new(3);
    let nested_lb = ListBuilder::new(builder);
    let mut lb = ListBuilder::new(nested_lb);
    for int_vec_vec in vec![
        vec![vec![0, 1], vec![2, 3], vec![3, 4]],
        vec![vec![5, 6], vec![7, 8], vec![9, 10]],
        vec![vec![11, 12], vec![13, 14], vec![15, 16]],
    ] {
        let nested_builder = lb.values();
        for int_vec in int_vec_vec {
            let builder = nested_builder.values();
            for int in int_vec {
                builder.append_value(int).unwrap();
            }
            nested_builder.append(true).unwrap();
        }
        lb.append(true).unwrap();
    }

    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    let table_a = Arc::new(table);

    ctx.register_table("ints", table_a)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_list[0] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| i0       |",
        "+----------+",
        "| [0, 1]   |",
        "| [5, 6]   |",
        "| [11, 12] |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);
    let sql = "SELECT some_list[0][0] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+", "| i0 |", "+----+", "| 0  |", "| 5  |", "| 11 |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_nested_get_indexed_field_on_struct() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let nested_dt = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
    // Nested schema of { "some_struct": { "bar": [i64] } }
    let struct_fields = vec![Field::new("bar", nested_dt.clone(), true)];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_struct",
        DataType::Struct(struct_fields.clone()),
        false,
    )]));

    let builder = PrimitiveBuilder::<Int64Type>::new(3);
    let nested_lb = ListBuilder::new(builder);
    let mut sb = StructBuilder::new(struct_fields, vec![Box::new(nested_lb)]);
    for int_vec in vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]] {
        let lb = sb.field_builder::<ListBuilder<Int64Builder>>(0).unwrap();
        for int in int_vec {
            lb.values().append_value(int).unwrap();
        }
        lb.append(true).unwrap();
    }
    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(sb.finish())])?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    let table_a = Arc::new(table);

    ctx.register_table("structs", table_a)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_struct[\"bar\"] as l0 FROM structs LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------------+",
        "| l0             |",
        "+----------------+",
        "| [0, 1, 2, 3]   |",
        "| [4, 5, 6, 7]   |",
        "| [8, 9, 10, 11] |",
        "+----------------+",
    ];
    assert_batches_eq!(expected, &actual);
    let sql = "SELECT some_struct[\"bar\"][0] as i0 FROM structs LIMIT 3";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+", "| i0 |", "+----+", "| 0  |", "| 4  |", "| 8  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn intersect_with_null_not_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            INTERSECT SELECT * FROM (SELECT null AS id1, 2 AS id2) t2";

    let expected = vec!["++", "++"];
    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn intersect_with_null_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            INTERSECT SELECT * FROM (SELECT null AS id1, 1 AS id2) t2";

    let expected = vec![
        "+-----+-----+",
        "| id1 | id2 |",
        "+-----+-----+",
        "|     | 1   |",
        "+-----+-----+",
    ];

    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_intersect_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 INTERSECT ALL SELECT int_col, double_col FROM alltypes_plain LIMIT 4";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_intersect_distinct() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 INTERSECT SELECT int_col, double_col FROM alltypes_plain";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn except_with_null_not_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            EXCEPT SELECT * FROM (SELECT null AS id1, 2 AS id2) t2";

    let expected = vec![
        "+-----+-----+",
        "| id1 | id2 |",
        "+-----+-----+",
        "|     | 1   |",
        "+-----+-----+",
    ];

    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn except_with_null_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            EXCEPT SELECT * FROM (SELECT null AS id1, 1 AS id2) t2";

    let expected = vec!["++", "++"];
    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_expect_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 EXCEPT ALL SELECT int_col, double_col FROM alltypes_plain where int_col < 1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_expect_distinct() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 EXCEPT SELECT int_col, double_col FROM alltypes_plain where int_col < 1";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_sort_unprojected_col() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT id FROM alltypes_plain ORDER BY int_col, double_col";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+", "| id |", "+----+", "| 4  |", "| 6  |", "| 2  |", "| 0  |", "| 5  |",
        "| 7  |", "| 3  |", "| 1  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_nulls_first_asc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "| 1   | one    |",
        "| 2   | two    |",
        "|     | three  |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_nulls_first_desc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num DESC";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "|     | three  |",
        "| 2   | two    |",
        "| 1   | one    |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_specific_nulls_last_desc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num DESC NULLS LAST";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "| 2   | two    |",
        "| 1   | one    |",
        "|     | three  |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_specific_nulls_first_asc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num ASC NULLS FIRST";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "|     | three  |",
        "| 1   | one    |",
        "| 2   | two    |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
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
async fn csv_query_with_decimal_by_sql() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_simple_aggregate_csv_with_decimal_by_sql(&mut ctx).await;
    let sql = "SELECT c1 from aggregate_simple";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----------+",
        "| c1       |",
        "+----------+",
        "| 0.000010 |",
        "| 0.000020 |",
        "| 0.000020 |",
        "| 0.000030 |",
        "| 0.000030 |",
        "| 0.000030 |",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn timestamp_minmax() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let table_a = make_timestamp_tz_table::<TimestampMillisecondType>(None)?;
    let table_b =
        make_timestamp_tz_table::<TimestampNanosecondType>(Some("UTC".to_owned()))?;
    ctx.register_table("table_a", table_a)?;
    ctx.register_table("table_b", table_b)?;

    let sql = "SELECT MIN(table_a.ts), MAX(table_b.ts) FROM table_a, table_b";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-------------------------+----------------------------+",
        "| MIN(table_a.ts)         | MAX(table_b.ts)            |",
        "+-------------------------+----------------------------+",
        "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29.190855 |",
        "+-------------------------+----------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn timestamp_coercion() -> Result<()> {
    {
        let mut ctx = ExecutionContext::new();
        let table_a =
            make_timestamp_tz_table::<TimestampSecondType>(Some("UTC".to_owned()))?;
        let table_b =
            make_timestamp_tz_table::<TimestampMillisecondType>(Some("UTC".to_owned()))?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------------------+-------------------------+--------------------------+",
            "| ts                  | ts                      | table_a.ts Eq table_b.ts |",
            "+---------------------+-------------------------+--------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190 | true                     |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190 | true                     |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190 | true                     |",
            "+---------------------+-------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampSecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------------------+----------------------------+--------------------------+",
            "| ts                  | ts                         | table_a.ts Eq table_b.ts |",
            "+---------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190855 | true                     |",
            "+---------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampSecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+---------------------+----------------------------+--------------------------+",
            "| ts                  | ts                         | table_a.ts Eq table_b.ts |",
            "+---------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29 | 2020-09-08 11:42:29.190855 | true                     |",
            "+---------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+-------------------------+---------------------+--------------------------+",
            "| ts                      | ts                  | table_a.ts Eq table_b.ts |",
            "+-------------------------+---------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29 | true                     |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29 | true                     |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29 | true                     |",
            "+-------------------------+---------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+-------------------------+----------------------------+--------------------------+",
            "| ts                      | ts                         | table_a.ts Eq table_b.ts |",
            "+-------------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29.190855 | true                     |",
            "+-------------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMillisecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+-------------------------+----------------------------+--------------------------+",
            "| ts                      | ts                         | table_a.ts Eq table_b.ts |",
            "+-------------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29.190 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29.190 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190 | 2020-09-08 11:42:29.190855 | true                     |",
            "+-------------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+---------------------+--------------------------+",
            "| ts                         | ts                  | table_a.ts Eq table_b.ts |",
            "+----------------------------+---------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29 | true                     |",
            "+----------------------------+---------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMillisecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+-------------------------+--------------------------+",
            "| ts                         | ts                      | table_a.ts Eq table_b.ts |",
            "+----------------------------+-------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190 | true                     |",
            "+----------------------------+-------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampMicrosecondType>()?;
        let table_b = make_timestamp_table::<TimestampNanosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+----------------------------+--------------------------+",
            "| ts                         | ts                         | table_a.ts Eq table_b.ts |",
            "+----------------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190855 | true                     |",
            "+----------------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampSecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+---------------------+--------------------------+",
            "| ts                         | ts                  | table_a.ts Eq table_b.ts |",
            "+----------------------------+---------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29 | true                     |",
            "+----------------------------+---------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMillisecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+-------------------------+--------------------------+",
            "| ts                         | ts                      | table_a.ts Eq table_b.ts |",
            "+----------------------------+-------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190 | true                     |",
            "+----------------------------+-------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    {
        let mut ctx = ExecutionContext::new();
        let table_a = make_timestamp_table::<TimestampNanosecondType>()?;
        let table_b = make_timestamp_table::<TimestampMicrosecondType>()?;
        ctx.register_table("table_a", table_a)?;
        ctx.register_table("table_b", table_b)?;

        let sql = "SELECT table_a.ts, table_b.ts, table_a.ts = table_b.ts FROM table_a, table_b";
        let actual = execute_to_batches(&mut ctx, sql).await;
        let expected = vec![
            "+----------------------------+----------------------------+--------------------------+",
            "| ts                         | ts                         | table_a.ts Eq table_b.ts |",
            "+----------------------------+----------------------------+--------------------------+",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 13:42:29.190855 | true                     |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 13:42:29.190855 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 12:42:29.190855 | true                     |",
            "| 2020-09-08 12:42:29.190855 | 2020-09-08 11:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 13:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 12:42:29.190855 | false                    |",
            "| 2020-09-08 11:42:29.190855 | 2020-09-08 11:42:29.190855 | true                     |",
            "+----------------------------+----------------------------+--------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    Ok(())
}
