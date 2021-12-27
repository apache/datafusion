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
