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
