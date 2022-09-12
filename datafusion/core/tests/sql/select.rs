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
use datafusion::{
    datasource::empty::EmptyTable, from_slice::FromSlice,
    physical_plan::collect_partitioned,
};
use tempfile::TempDir;

#[tokio::test]
async fn all_where_empty() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT *
               FROM aggregate_test_100
               WHERE 1=2";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn select_values_list() -> Result<()> {
    let ctx = SessionContext::new();
    {
        let sql = "VALUES (1)";
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
        let actual = execute_to_batches(&ctx, sql).await;
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
    {
        let sql = "EXPLAIN VALUES ('1'::float)";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------+-----------------------------------+",
            "| plan_type     | plan                              |",
            "+---------------+-----------------------------------+",
            "| logical_plan  | Values: (Float32(1) AS Utf8(\"1\")) |",
            "| physical_plan | ValuesExec                        |",
            "|               |                                   |",
            "+---------------+-----------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    {
        let sql = "EXPLAIN VALUES (('1'||'2')::int unsigned)";
        let actual = execute_to_batches(&ctx, sql).await;
        let expected = vec![
            "+---------------+------------------------------------------------+",
            "| plan_type     | plan                                           |",
            "+---------------+------------------------------------------------+",
            "| logical_plan  | Values: (UInt32(12) AS Utf8(\"1\") || Utf8(\"2\")) |",
            "| physical_plan | ValuesExec                                     |",
            "|               |                                                |",
            "+---------------+------------------------------------------------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}

#[tokio::test]
async fn select_all() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql = "SELECT c1 FROM aggregate_simple order by c1";
    let results = execute_to_batches(&ctx, sql).await;

    let sql_all = "SELECT ALL c1 FROM aggregate_simple order by c1";
    let results_all = execute_to_batches(&ctx, sql_all).await;

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
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql = "SELECT DISTINCT * FROM aggregate_simple";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();

    let mut dedup = actual.clone();
    dedup.dedup();

    assert_eq!(actual, dedup);

    Ok(())
}

#[tokio::test]
async fn select_distinct_simple_1() {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await.unwrap();

    let sql = "SELECT DISTINCT c1 FROM aggregate_simple order by c1";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await.unwrap();

    let sql = "SELECT DISTINCT c1, c2 FROM aggregate_simple order by c1";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await.unwrap();

    let sql = "SELECT distinct c3 FROM aggregate_simple order by c3";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await.unwrap();

    let sql = "SELECT distinct c1+c2 as a FROM aggregate_simple";
    let actual = execute_to_batches(&ctx, sql).await;

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
    let ctx = SessionContext::new();

    let sql = "select
        1 IS DISTINCT FROM CAST(NULL as INT) as a,
        1 IS DISTINCT FROM 1 as b,
        1 IS NOT DISTINCT FROM CAST(NULL as INT) as c,
        1 IS NOT DISTINCT FROM 1 as d,
        NULL IS DISTINCT FROM NULL as e,
        NULL IS NOT DISTINCT FROM NULL as f,
        NULL is DISTINCT FROM 1 as g,
        NULL is NOT DISTINCT FROM 1 as h
    ";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------+-------+-------+------+-------+------+------+-------+",
        "| a    | b     | c     | d    | e     | f    | g    | h     |",
        "+------+-------+-------+------+-------+------+------+-------+",
        "| true | false | false | true | false | true | true | false |",
        "+------+-------+-------+------+-------+------+------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select
        NULL IS DISTINCT FROM NULL as a,
        NULL IS NOT DISTINCT FROM NULL as b,
        NULL is DISTINCT FROM 1 as c,
        NULL is NOT DISTINCT FROM 1 as d,
        1 IS DISTINCT FROM CAST(NULL as INT) as e,
        1 IS DISTINCT FROM 1 as f,
        1 IS NOT DISTINCT FROM CAST(NULL as INT) as g,
        1 IS NOT DISTINCT FROM 1 as h
    ";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+------+------+-------+------+-------+-------+------+",
        "| a     | b    | c    | d     | e    | f     | g     | h    |",
        "+-------+------+------+-------+------+-------+-------+------+",
        "| false | true | true | false | true | false | false | true |",
        "+-------+------+------+-------+------+-------+-------+------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn select_distinct_from_utf8() {
    let ctx = SessionContext::new();

    let sql = "select
        'x' IS DISTINCT FROM NULL as a,
        'x' IS DISTINCT FROM 'x' as b,
        'x' IS NOT DISTINCT FROM NULL as c,
        'x' IS NOT DISTINCT FROM 'x' as d
    ";
    let actual = execute_to_batches(&ctx, sql).await;
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
async fn use_between_expression_in_select_query() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "SELECT 1 NOT BETWEEN 3 AND 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------------------------------+",
        "| Int64(1) NOT BETWEEN Int64(3) AND Int64(5) |",
        "+--------------------------------------------+",
        "| true                                       |",
        "+--------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let input = Int64Array::from_slice(&[1, 2, 3, 4]);
    let batch = RecordBatch::try_from_iter(vec![("c1", Arc::new(input) as _)]).unwrap();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("test", Arc::new(table))?;

    let sql = "SELECT abs(c1) BETWEEN 0 AND LoG(c1 * 100 ) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    // Expect field name to be correctly converted for expr, low and high.
    let expected = vec![
        "+-------------------------------------------------------------+",
        "| abs(test.c1) BETWEEN Int64(0) AND log(test.c1 * Int64(100)) |",
        "+-------------------------------------------------------------+",
        "| true                                                        |",
        "| true                                                        |",
        "| false                                                       |",
        "| false                                                       |",
        "+-------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "EXPLAIN SELECT c1 BETWEEN 2 AND 3 FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // Only test that the projection exprs are correct, rather than entire output
    let needle = "ProjectionExec: expr=[c1@0 >= 2 AND c1@0 <= 3 as test.c1 BETWEEN Int64(2) AND Int64(3)]";
    assert_contains!(&formatted, needle);
    let needle = "Projection: #test.c1 >= Int64(2) AND #test.c1 <= Int64(3)";
    assert_contains!(&formatted, needle);

    Ok(())
}

#[tokio::test]
async fn query_get_indexed_field() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_list",
        DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
        false,
    )]));
    let builder = PrimitiveBuilder::<Int64Type>::with_capacity(3);
    let mut lb = ListBuilder::new(builder);
    for int_vec in vec![vec![0, 1, 2], vec![4, 5, 6], vec![7, 8, 9]] {
        let builder = lb.values();
        for int in int_vec {
            builder.append_value(int);
        }
        lb.append(true);
    }

    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    let table_a = Arc::new(table);

    ctx.register_table("ints", table_a)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_list[1] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| i0 |",
        "+----+",
        "| 0  |",
        "| 4  |",
        "| 7  |",
        "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_nested_get_indexed_field() -> Result<()> {
    let ctx = SessionContext::new();
    let nested_dt = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
    // Nested schema of { "some_list": [[i64]] }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_list",
        DataType::List(Box::new(Field::new("item", nested_dt.clone(), true))),
        false,
    )]));

    let builder = PrimitiveBuilder::<Int64Type>::with_capacity(3);
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
                builder.append_value(int);
            }
            nested_builder.append(true);
        }
        lb.append(true);
    }

    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb.finish())])?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    let table_a = Arc::new(table);

    ctx.register_table("ints", table_a)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_list[1] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
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
    let sql = "SELECT some_list[1][1] as i0 FROM ints LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| i0 |",
        "+----+",
        "| 0  |",
        "| 5  |",
        "| 11 |",
        "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_nested_get_indexed_field_on_struct() -> Result<()> {
    let ctx = SessionContext::new();
    let nested_dt = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
    // Nested schema of { "some_struct": { "bar": [i64] } }
    let struct_fields = vec![Field::new("bar", nested_dt.clone(), true)];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "some_struct",
        DataType::Struct(struct_fields.clone()),
        false,
    )]));

    let builder = PrimitiveBuilder::<Int64Type>::with_capacity(3);
    let nested_lb = ListBuilder::new(builder);
    let mut sb = StructBuilder::new(struct_fields, vec![Box::new(nested_lb)]);
    for int_vec in vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10, 11]] {
        let lb = sb.field_builder::<ListBuilder<Int64Builder>>(0).unwrap();
        for int in int_vec {
            lb.values().append_value(int);
        }
        lb.append(true);
        sb.append(true);
    }
    let s = sb.finish();
    let data = RecordBatch::try_new(schema.clone(), vec![Arc::new(s)])?;
    let table = MemTable::try_new(schema, vec![vec![data]])?;
    let table_a = Arc::new(table);

    ctx.register_table("structs", table_a)?;

    // Original column is micros, convert to millis and check timestamp
    let sql = "SELECT some_struct['bar'] as l0 FROM structs LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
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

    // Access to field of struct by CompoundIdentifier
    let sql = "SELECT some_struct.bar as l0 FROM structs LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
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

    let sql = "SELECT some_struct['bar'][1] as i0 FROM structs LIMIT 3";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+----+",
        "| i0 |",
        "+----+",
        "| 0  |",
        "| 4  |",
        "| 8  |",
        "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_on_string_dictionary() -> Result<()> {
    // Test to ensure DataFusion can operate on dictionary types
    // Use StringDictionary (32 bit indexes = keys)
    let d1: DictionaryArray<Int32Type> =
        vec![Some("one"), None, Some("three")].into_iter().collect();

    let d2: DictionaryArray<Int32Type> = vec![Some("blarg"), None, Some("three")]
        .into_iter()
        .collect();

    let d3: StringArray = vec![Some("XYZ"), None, Some("three")].into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("d1", Arc::new(d1) as ArrayRef),
        ("d2", Arc::new(d2) as ArrayRef),
        ("d3", Arc::new(d3) as ArrayRef),
    ])
    .unwrap();

    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(table))?;

    // Basic SELECT
    let sql = "SELECT d1 FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
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
    let sql = "SELECT d1 FROM test WHERE d1 IS NOT NULL";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| one   |",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // comparison with constant
    let sql = "SELECT d1 FROM test WHERE d1 = 'three'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // comparison with another dictionary column
    let sql = "SELECT d1 FROM test WHERE d1 = d2";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // order comparison with another dictionary column
    let sql = "SELECT d1 FROM test WHERE d1 <= d2";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // comparison with a non dictionary column
    let sql = "SELECT d1 FROM test WHERE d1 = d3";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------+",
        "| d1    |",
        "+-------+",
        "| three |",
        "+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // filtering with constant
    let sql = "SELECT d1 FROM test WHERE d1 = 'three'";
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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

    // Expression evaluation with two dictionaries
    let sql = "SELECT concat(d1, d2) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-------------------------+",
        "| concat(test.d1,test.d2) |",
        "+-------------------------+",
        "| oneblarg                |",
        "|                         |",
        "| threethree              |",
        "+-------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // aggregation
    let sql = "SELECT COUNT(d1) FROM test";
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
async fn query_cte_with_alias() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("a", DataType::Int16, false),
    ]);
    let empty_table = Arc::new(EmptyTable::new(Arc::new(schema)));
    ctx.register_table("t1", empty_table)?;
    let sql = "WITH \
        v1 AS (SELECT * FROM t1), \
        v2 AS (SELECT v1.id AS id, v1a.id AS id_a, v1b.id AS id_b \
        FROM v1, v1 v1a, v1 v1b \
        WHERE v1a.id = v1.id - 1 \
        AND v1b.id = v1.id + 1) \
        SELECT * FROM v2";
    let actual = execute_to_batches(&ctx, sql).await;
    // the purpose of this test is just to make sure the query produces a valid plan
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn query_cte() -> Result<()> {
    // Test for SELECT <expression> without FROM.
    // Should evaluate expressions in project position.
    let ctx = SessionContext::new();

    // simple with
    let sql = "WITH t AS (SELECT 1) SELECT * FROM t";
    let actual = execute_to_batches(&ctx, sql).await;
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
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| a |",
        "+---+",
        "| 1 |",
        "| 2 |",
        "+---+"
    ];
    assert_batches_eq!(expected, &actual);

    // with + join
    let sql = "WITH t AS (SELECT 1 AS id1), u AS (SELECT 1 AS id2, 5 as x) SELECT x FROM t JOIN u ON (id1 = id2)";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| x |",
        "+---+",
        "| 5 |",
        "+---+"
    ];
    assert_batches_eq!(expected, &actual);

    // backward reference
    let sql = "WITH t AS (SELECT 1 AS id1), u AS (SELECT * FROM t) SELECT * from u";
    let actual = execute_to_batches(&ctx, sql).await;
    #[rustfmt::skip]
    let expected = vec![
        "+-----+",
        "| id1 |",
        "+-----+",
        "| 1   |",
        "+-----+"
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_select_nested() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
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
    let actual = execute_to_batches(&ctx, sql).await;
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
async fn csv_select_nested_without_aliases() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT o1, o2, c3
               FROM (
                 SELECT c1 AS o1, c2 + 1 AS o2, c3
                 FROM (
                   SELECT c1, c2, c3, c4
                   FROM aggregate_test_100
                   WHERE c1 = 'a' AND c2 >= 4
                   ORDER BY c2 ASC, c3 ASC
                 )
               )";
    let actual = execute_to_batches(&ctx, sql).await;
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
async fn csv_join_unaliased_subqueries() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT o1, o2, c3, p1, p2, p3 FROM \
        (SELECT c1 AS o1, c2 + 1 AS o2, c3 FROM aggregate_test_100), \
        (SELECT c1 AS p1, c2 - 1 AS p2, c3 AS p3 FROM aggregate_test_100) LIMIT 5";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+----+----+----+-----+",
        "| o1 | o2 | c3 | p1 | p2 | p3  |",
        "+----+----+----+----+----+-----+",
        "| c  | 3  | 1  | c  | 1  | 1   |",
        "| c  | 3  | 1  | d  | 4  | -40 |",
        "| c  | 3  | 1  | b  | 0  | 29  |",
        "| c  | 3  | 1  | a  | 0  | -85 |",
        "| c  | 3  | 1  | b  | 4  | -82 |",
        "+----+----+----+----+----+-----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn parallel_query_with_filter() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = partitioned_csv::create_ctx(&tmp_dir, partition_count).await?;

    let logical_plan =
        ctx.create_logical_plan("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")?;
    let logical_plan = ctx.optimize(&logical_plan)?;

    let physical_plan = ctx.create_physical_plan(&logical_plan).await?;

    let task_ctx = ctx.task_ctx();
    let results = collect_partitioned(physical_plan, task_ctx).await?;

    // note that the order of partitions is not deterministic
    let mut num_rows = 0;
    for partition in &results {
        for batch in partition {
            num_rows += batch.num_rows();
        }
    }
    assert_eq!(20, num_rows);

    let results: Vec<RecordBatch> = results.into_iter().flatten().collect();
    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 1  | 1  |",
        "| 1  | 10 |",
        "| 1  | 2  |",
        "| 1  | 3  |",
        "| 1  | 4  |",
        "| 1  | 5  |",
        "| 1  | 6  |",
        "| 1  | 7  |",
        "| 1  | 8  |",
        "| 1  | 9  |",
        "| 2  | 1  |",
        "| 2  | 10 |",
        "| 2  | 2  |",
        "| 2  | 3  |",
        "| 2  | 4  |",
        "| 2  | 5  |",
        "| 2  | 6  |",
        "| 2  | 7  |",
        "| 2  | 8  |",
        "| 2  | 9  |",
        "+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn query_with_filter_string_type_coercion() {
    let large_string_array = LargeStringArray::from(vec!["1", "2", "3", "4", "5"]);
    let schema =
        Schema::new(vec![Field::new("large_string", DataType::LargeUtf8, false)]);
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(large_string_array)])
            .unwrap();

    let ctx = SessionContext::new();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    let sql = "select * from t where large_string = '1'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| large_string |",
        "+--------------+",
        "| 1            |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select * from t where large_string != '1'";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------+",
        "| large_string |",
        "+--------------+",
        "| 2            |",
        "| 3            |",
        "| 4            |",
        "| 5            |",
        "+--------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn query_empty_table() {
    let ctx = SessionContext::new();
    let empty_table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
    ctx.register_table("test_tbl", empty_table).unwrap();
    let sql = "SELECT * FROM test_tbl";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("Query empty table");
    let expected = vec!["++", "++"];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn boolean_literal() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, c3 FROM test WHERE c1 > 2 AND c3 = true", 4)
            .await?;

    let expected = vec![
        "+----+------+",
        "| c1 | c3   |",
        "+----+------+",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "| 3  | true |",
        "+----+------+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn unprojected_filter() {
    let ctx = SessionContext::new();
    let df = ctx.read_table(table_with_sequence(1, 3).unwrap()).unwrap();

    let df = df
        .select(vec![col("i") + col("i")])
        .unwrap()
        .filter(col("i").gt(lit(2)))
        .unwrap();
    let results = df.collect().await.unwrap();

    let expected = vec![
        "+-----------------------+",
        "| ?table?.i + ?table?.i |",
        "+-----------------------+",
        "| 6                     |",
        "+-----------------------+",
    ];
    assert_batches_sorted_eq!(expected, &results);
}

#[tokio::test]
async fn case_sensitive_in_default_dialect() {
    let int32_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![Field::new("INT32", DataType::Int32, false)]);
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(int32_array)]).unwrap();

    let ctx = SessionContext::new();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    {
        let sql = "select \"int32\" from t";
        let plan = ctx.create_logical_plan(sql);
        assert!(plan.is_err());
    }

    {
        let sql = "select \"INT32\" from t";
        let actual = execute_to_batches(&ctx, sql).await;

        let expected = vec![
            "+-------+",
            "| INT32 |",
            "+-------+",
            "| 1     |",
            "| 2     |",
            "| 3     |",
            "| 4     |",
            "| 5     |",
            "+-------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
}
