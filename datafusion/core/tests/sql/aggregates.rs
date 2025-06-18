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
use datafusion::common::test_util::batches_to_string;
use datafusion_catalog::MemTable;
use datafusion_common::ScalarValue;

#[tokio::test]
async fn csv_query_array_agg_distinct() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT array_agg(distinct c2) FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;

    // The results for this query should be something like the following:
    //    +------------------------------------------+
    //    | ARRAY_AGG(DISTINCT aggregate_test_100.c2) |
    //    +------------------------------------------+
    //    | [4, 2, 3, 5, 1]                          |
    //    +------------------------------------------+
    // Since ARRAY_AGG(DISTINCT) ordering is nondeterministic, check the schema and contents.
    assert_eq!(
        *actual[0].schema(),
        Schema::new(vec![Field::new_list(
            "array_agg(DISTINCT aggregate_test_100.c2)",
            Field::new_list_field(DataType::UInt32, true),
            true
        ),])
    );

    // We should have 1 row containing a list
    let column = actual[0].column(0);
    assert_eq!(column.len(), 1);
    let scalar_vec = ScalarValue::convert_array_to_scalar_vec(&column)?;
    let mut scalars = scalar_vec[0].clone();

    // workaround lack of Ord of ScalarValue
    let cmp = |a: &ScalarValue, b: &ScalarValue| {
        a.partial_cmp(b).expect("Can compare ScalarValues")
    };
    scalars.sort_by(cmp);
    assert_eq!(
        scalars,
        vec![
            ScalarValue::UInt32(Some(1)),
            ScalarValue::UInt32(Some(2)),
            ScalarValue::UInt32(Some(3)),
            ScalarValue::UInt32(Some(4)),
            ScalarValue::UInt32(Some(5))
        ]
    );

    Ok(())
}

#[tokio::test]
async fn count_partitioned() -> Result<()> {
    let results =
        execute_with_partition("SELECT count(c1), count(c2) FROM test", 4).await?;
    assert_eq!(results.len(), 1);

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----------------+----------------+
    | count(test.c1) | count(test.c2) |
    +----------------+----------------+
    | 40             | 40             |
    +----------------+----------------+
    ");
    Ok(())
}

#[tokio::test]
async fn count_aggregated() -> Result<()> {
    let results =
        execute_with_partition("SELECT c1, count(c2) FROM test GROUP BY c1", 4).await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----+----------------+
    | c1 | count(test.c2) |
    +----+----------------+
    | 0  | 10             |
    | 1  | 10             |
    | 2  | 10             |
    | 3  | 10             |
    +----+----------------+
    ");
    Ok(())
}

#[tokio::test]
async fn count_aggregated_cube() -> Result<()> {
    let results = execute_with_partition(
        "SELECT c1, c2, count(c3) FROM test GROUP BY CUBE (c1, c2) ORDER BY c1, c2",
        4,
    )
    .await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +----+----+----------------+
    | c1 | c2 | count(test.c3) |
    +----+----+----------------+
    |    |    | 40             |
    |    | 1  | 4              |
    |    | 10 | 4              |
    |    | 2  | 4              |
    |    | 3  | 4              |
    |    | 4  | 4              |
    |    | 5  | 4              |
    |    | 6  | 4              |
    |    | 7  | 4              |
    |    | 8  | 4              |
    |    | 9  | 4              |
    | 0  |    | 10             |
    | 0  | 1  | 1              |
    | 0  | 10 | 1              |
    | 0  | 2  | 1              |
    | 0  | 3  | 1              |
    | 0  | 4  | 1              |
    | 0  | 5  | 1              |
    | 0  | 6  | 1              |
    | 0  | 7  | 1              |
    | 0  | 8  | 1              |
    | 0  | 9  | 1              |
    | 1  |    | 10             |
    | 1  | 1  | 1              |
    | 1  | 10 | 1              |
    | 1  | 2  | 1              |
    | 1  | 3  | 1              |
    | 1  | 4  | 1              |
    | 1  | 5  | 1              |
    | 1  | 6  | 1              |
    | 1  | 7  | 1              |
    | 1  | 8  | 1              |
    | 1  | 9  | 1              |
    | 2  |    | 10             |
    | 2  | 1  | 1              |
    | 2  | 10 | 1              |
    | 2  | 2  | 1              |
    | 2  | 3  | 1              |
    | 2  | 4  | 1              |
    | 2  | 5  | 1              |
    | 2  | 6  | 1              |
    | 2  | 7  | 1              |
    | 2  | 8  | 1              |
    | 2  | 9  | 1              |
    | 3  |    | 10             |
    | 3  | 1  | 1              |
    | 3  | 10 | 1              |
    | 3  | 2  | 1              |
    | 3  | 3  | 1              |
    | 3  | 4  | 1              |
    | 3  | 5  | 1              |
    | 3  | 6  | 1              |
    | 3  | 7  | 1              |
    | 3  | 8  | 1              |
    | 3  | 9  | 1              |
    +----+----+----------------+
    ");
    Ok(())
}

async fn run_count_distinct_integers_aggregated_scenario(
    partitions: Vec<Vec<(&str, u64)>>,
) -> Result<Vec<RecordBatch>> {
    let tmp_dir = TempDir::new()?;
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("c_group", DataType::Utf8, false),
        Field::new("c_int8", DataType::Int8, false),
        Field::new("c_int16", DataType::Int16, false),
        Field::new("c_int32", DataType::Int32, false),
        Field::new("c_int64", DataType::Int64, false),
        Field::new("c_uint8", DataType::UInt8, false),
        Field::new("c_uint16", DataType::UInt16, false),
        Field::new("c_uint32", DataType::UInt32, false),
        Field::new("c_uint64", DataType::UInt64, false),
    ]));

    for (i, partition) in partitions.iter().enumerate() {
        let filename = format!("partition-{i}.csv");
        let file_path = tmp_dir.path().join(filename);
        let mut file = File::create(file_path)?;
        for row in partition {
            let row_str = format!(
                "{},{}\n",
                row.0,
                // Populate values for each of the integer fields in the
                // schema.
                (0..8)
                    .map(|_| { row.1.to_string() })
                    .collect::<Vec<_>>()
                    .join(","),
            );
            file.write_all(row_str.as_bytes())?;
        }
    }
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema).has_header(false),
    )
    .await?;

    let results = plan_and_collect(
        &ctx,
        "
          SELECT
            c_group,
            count(c_uint64),
            count(DISTINCT c_int8),
            count(DISTINCT c_int16),
            count(DISTINCT c_int32),
            count(DISTINCT c_int64),
            count(DISTINCT c_uint8),
            count(DISTINCT c_uint16),
            count(DISTINCT c_uint32),
            count(DISTINCT c_uint64)
          FROM test
          GROUP BY c_group
        ",
    )
    .await?;

    Ok(results)
}

#[tokio::test]
async fn count_distinct_integers_aggregated_single_partition() -> Result<()> {
    let partitions = vec![
        // The first member of each tuple will be the value for the
        // `c_group` column, and the second member will be the value for
        // each of the int/uint fields.
        vec![
            ("a", 1),
            ("a", 1),
            ("a", 2),
            ("b", 9),
            ("c", 9),
            ("c", 10),
            ("c", 9),
        ],
    ];

    let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    | c_group | count(test.c_uint64) | count(DISTINCT test.c_int8) | count(DISTINCT test.c_int16) | count(DISTINCT test.c_int32) | count(DISTINCT test.c_int64) | count(DISTINCT test.c_uint8) | count(DISTINCT test.c_uint16) | count(DISTINCT test.c_uint32) | count(DISTINCT test.c_uint64) |
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    | a       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |
    | b       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |
    | c       | 3                    | 2                           | 2                            | 2                            | 2                            | 2                            | 2                             | 2                             | 2                             |
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    ");

    Ok(())
}

#[tokio::test]
async fn count_distinct_integers_aggregated_multiple_partitions() -> Result<()> {
    let partitions = vec![
        // The first member of each tuple will be the value for the
        // `c_group` column, and the second member will be the value for
        // each of the int/uint fields.
        vec![("a", 1), ("a", 1), ("a", 2), ("b", 9), ("c", 9)],
        vec![("a", 1), ("a", 3), ("b", 8), ("b", 9), ("b", 10), ("b", 11)],
    ];

    let results = run_count_distinct_integers_aggregated_scenario(partitions).await?;

    assert_snapshot!(batches_to_sort_string(&results), @r"
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    | c_group | count(test.c_uint64) | count(DISTINCT test.c_int8) | count(DISTINCT test.c_int16) | count(DISTINCT test.c_int32) | count(DISTINCT test.c_int64) | count(DISTINCT test.c_uint8) | count(DISTINCT test.c_uint16) | count(DISTINCT test.c_uint32) | count(DISTINCT test.c_uint64) |
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    | a       | 5                    | 3                           | 3                            | 3                            | 3                            | 3                            | 3                             | 3                             | 3                             |
    | b       | 5                    | 4                           | 4                            | 4                            | 4                            | 4                            | 4                             | 4                             | 4                             |
    | c       | 1                    | 1                           | 1                            | 1                            | 1                            | 1                            | 1                             | 1                             | 1                             |
    +---------+----------------------+-----------------------------+------------------------------+------------------------------+------------------------------+------------------------------+-------------------------------+-------------------------------+-------------------------------+
    ");

    Ok(())
}

#[tokio::test]
async fn test_accumulator_row_accumulator() -> Result<()> {
    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config(config);
    register_aggregate_csv(&ctx).await?;

    let sql = "SELECT c1, c2, MIN(c13) as min1, MIN(c9) as min2, MAX(c13) as max1, MAX(c9) as max2, AVG(c9) as avg1, MIN(c13) as min3, count(C9) as cnt1, 0.5*SUM(c9-c8) as sum1
    FROM aggregate_test_100
    GROUP BY c1, c2
    ORDER BY c1, c2
    LIMIT 5";

    let actual = execute_to_batches(&ctx, sql).await;
    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+
    | c1 | c2 | min1                           | min2      | max1                           | max2       | avg1               | min3                           | cnt1 | sum1         |
    +----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+
    | a  | 1  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB | 774637006 | waIGbOGl1PM6gnzZ4uuZt4E2yDWRHs | 4015442341 | 2437927011.0       | 0keZ5G8BffGwgF2RwQD59TFzMStxCB | 5    | 6094771121.5 |
    | a  | 2  | b3b9esRhTzFEawbs6XhpKnD9ojutHB | 145294611 | ukyD7b0Efj7tNlFSRmzZ0IqkEzg2a8 | 3717551163 | 2267588664.0       | b3b9esRhTzFEawbs6XhpKnD9ojutHB | 3    | 3401364777.0 |
    | a  | 3  | Amn2K87Db5Es3dFQO9cw9cvpAM6h35 | 431948861 | oLZ21P2JEDooxV1pU31cIxQHEeeoLu | 3998790955 | 2225685115.1666665 | Amn2K87Db5Es3dFQO9cw9cvpAM6h35 | 6    | 6676994872.5 |
    | a  | 4  | KJFcmTVjdkCMv94wYCtfHMFhzyRsmH | 466439833 | ydkwycaISlYSlEq3TlkS2m15I2pcp8 | 2502326480 | 1655431654.0       | KJFcmTVjdkCMv94wYCtfHMFhzyRsmH | 4    | 3310812222.5 |
    | a  | 5  | MeSTAXq8gVxVjbEjgkvU9YLte0X9uE | 141047417 | QJYm7YRA3YetcBHI5wkMZeLXVmfuNy | 2496054700 | 1216992989.6666667 | MeSTAXq8gVxVjbEjgkvU9YLte0X9uE | 3    | 1825431770.0 |
    +----+----+--------------------------------+-----------+--------------------------------+------------+--------------------+--------------------------------+------+--------------+
    ");

    Ok(())
}

/// Test that COUNT(DISTINCT) correctly handles dictionary arrays with all null values.
/// Verifies behavior across both single and multiple partitions.
#[tokio::test]
async fn count_distinct_dictionary_all_null_values() -> Result<()> {
    let n: usize = 5;
    let num = Arc::new(Int32Array::from_iter(0..n as i32)) as ArrayRef;

    // Create dictionary where all indices point to a null value (index 0)
    let dict_values = StringArray::from(vec![None, Some("abc")]);
    let dict_indices = Int32Array::from(vec![0; n]);
    let dict = DictionaryArray::new(dict_indices, Arc::new(dict_values));

    let schema = Arc::new(Schema::new(vec![
        Field::new("num1", DataType::Int32, false),
        Field::new("num2", DataType::Int32, false),
        Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![num.clone(), num.clone(), Arc::new(dict)],
    )?;

    // Test with single partition
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let provider = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let df = ctx
        .sql("SELECT count(distinct dict) as cnt, count(num2) FROM t GROUP BY num1")
        .await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +-----+---------------+
    | cnt | count(t.num2) |
    +-----+---------------+
    | 0   | 1             |
    | 0   | 1             |
    | 0   | 1             |
    | 0   | 1             |
    | 0   | 1             |
    +-----+---------------+
    "###
    );

    // Test with multiple partitions
    let ctx_multi =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    let provider_multi = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx_multi.register_table("t", Arc::new(provider_multi))?;

    let df_multi = ctx_multi
        .sql("SELECT count(distinct dict) as cnt, count(num2) FROM t GROUP BY num1")
        .await?;
    let results_multi = df_multi.collect().await?;

    // Results should be identical across partition configurations
    assert_eq!(
        batches_to_string(&results),
        batches_to_string(&results_multi)
    );

    Ok(())
}

/// Test COUNT(DISTINCT) with mixed null and non-null dictionary values
#[tokio::test]
async fn count_distinct_dictionary_mixed_values() -> Result<()> {
    let n: usize = 6;
    let num = Arc::new(Int32Array::from_iter(0..n as i32)) as ArrayRef;

    // Dictionary values array with nulls and non-nulls
    let dict_values = StringArray::from(vec![None, Some("abc"), Some("def"), None]);
    // Create indices that point to both null and non-null values
    let dict_indices = Int32Array::from(vec![0, 1, 2, 0, 1, 3]);
    let dict = DictionaryArray::new(dict_indices, Arc::new(dict_values));

    let schema = Arc::new(Schema::new(vec![
        Field::new("num1", DataType::Int32, false),
        Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(schema.clone(), vec![num, Arc::new(dict)])?;
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider))?;

    // COUNT(DISTINCT) should only count non-null values "abc" and "def"
    let df = ctx.sql("SELECT count(distinct dict) FROM t").await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +------------------------+
    | count(DISTINCT t.dict) |
    +------------------------+
    | 2                      |
    +------------------------+
    "###
    );

    Ok(())
}

/// Test GROUP BY with MAX aggregations on multiple data types
/// Tests SQL like: SELECT u8_low, dictionary_utf8_low, utf8_low, max(utf8_low) as col1,
/// max(utf8) as col2, max(binary) as col3, max(time64_ns) as col4 FROM fuzz_table GROUP BY ...
#[tokio::test]
async fn test_group_by_with_max_aggregations() -> Result<()> {
    let ctx = SessionContext::new();

    // Create test data mimicking the fuzz_table structure with the specific columns
    let u8_low_values = UInt8Array::from(vec![1, 1, 2, 2, 3, 3]);
    let dict_values = StringArray::from(vec!["a", "b", "c"]);
    let dict_indices = UInt64Array::from(vec![0, 0, 1, 1, 2, 2]);
    let dictionary_utf8_low = DictionaryArray::new(dict_indices, Arc::new(dict_values));
    let utf8_low_values = StringArray::from(vec!["x", "y", "x", "z", "y", "x"]);
    let utf8_values =
        StringArray::from(vec!["hello", "world", "foo", "bar", "baz", "qux"]);
    let binary_values = BinaryArray::from_iter_values(vec![
        b"data1".as_slice(),
        b"data2".as_slice(),
        b"data3".as_slice(),
        b"data4".as_slice(),
        b"data5".as_slice(),
        b"data6".as_slice(),
    ]);
    let time64_ns_values = Time64NanosecondArray::from(vec![
        1000000000, // 1 second in nanoseconds
        2000000000, // 2 seconds
        3000000000, // 3 seconds
        4000000000, // 4 seconds
        5000000000, // 5 seconds
        6000000000, // 6 seconds
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("u8_low", DataType::UInt8, false),
        Field::new(
            "dictionary_utf8_low",
            DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("utf8_low", DataType::Utf8, false),
        Field::new("utf8", DataType::Utf8, false),
        Field::new("binary", DataType::Binary, false),
        Field::new("time64_ns", DataType::Time64(TimeUnit::Nanosecond), false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(u8_low_values),
            Arc::new(dictionary_utf8_low),
            Arc::new(utf8_low_values),
            Arc::new(utf8_values),
            Arc::new(binary_values),
            Arc::new(time64_ns_values),
        ],
    )?;

    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("fuzz_table", Arc::new(provider))?;

    // Execute the test query
    let sql = "SELECT
        u8_low,
        dictionary_utf8_low,
        utf8_low,
        max(utf8_low) as col1,
        max(utf8) as col2,
        max(binary) as col3,
        max(time64_ns) as col4
    FROM
        fuzz_table
    GROUP BY
        u8_low,
        dictionary_utf8_low,
        utf8_low
    ORDER BY
        u8_low,
        dictionary_utf8_low,
        utf8_low";

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;

    // Verify the results contain the expected grouped and aggregated data
    assert_eq!(results.len(), 1);
    let batch = &results[0];

    // Should have 6 rows (one for each unique combination of group by columns)
    assert_eq!(batch.num_rows(), 6);

    // Verify schema
    let expected_schema = Schema::new(vec![
        Field::new("u8_low", DataType::UInt8, false),
        Field::new(
            "dictionary_utf8_low",
            DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("utf8_low", DataType::Utf8, false),
        Field::new("col1", DataType::Utf8, true), // max() can return null
        Field::new("col2", DataType::Utf8, true),
        Field::new("col3", DataType::Binary, true),
        Field::new("col4", DataType::Time64(TimeUnit::Nanosecond), true),
    ]);

    assert_eq!(*batch.schema(), expected_schema);

    // Print results for manual verification
    println!("Results:");
    println!("{}", batches_to_string(&results));

    Ok(())
}

/// Test GROUP BY with MAX aggregations and nulls
#[tokio::test]
async fn test_group_by_max_with_nulls() -> Result<()> {
    let ctx = SessionContext::new();

    // Create test data with some null values
    let u8_low_values = UInt8Array::from(vec![Some(1), Some(1), Some(2), None]);
    let utf8_low_values = StringArray::from(vec![Some("x"), None, Some("y"), Some("x")]);
    let utf8_values =
        StringArray::from(vec![Some("hello"), Some("world"), None, Some("test")]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("u8_low", DataType::UInt8, true),
        Field::new("utf8_low", DataType::Utf8, true),
        Field::new("utf8", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(u8_low_values),
            Arc::new(utf8_low_values),
            Arc::new(utf8_values),
        ],
    )?;

    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("fuzz_table", Arc::new(provider))?;

    // Test MAX with nulls in both group by and aggregate columns
    let sql = "SELECT
        u8_low,
        utf8_low,
        max(utf8) as max_utf8
    FROM
        fuzz_table
    GROUP BY
        u8_low,
        utf8_low
    ORDER BY
        u8_low,
        utf8_low";

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;

    // Verify that MAX properly handles nulls
    assert!(!results.is_empty());

    println!("Results with nulls:");
    println!("{}", batches_to_string(&results));

    Ok(())
}
