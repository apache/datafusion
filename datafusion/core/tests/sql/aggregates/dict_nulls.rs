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
use insta::assert_snapshot;

/// Comprehensive test for aggregate functions with null values and dictionary columns
/// Tests COUNT, SUM, MIN, and MEDIAN null handling in single comprehensive test
#[tokio::test]
async fn test_aggregates_null_handling_comprehensive() -> Result<()> {
    let test_data_basic = TestData::new();
    let test_data_extended = TestData::new_extended();
    let test_data_min_max = TestData::new_for_min_max();
    let test_data_median = TestData::new_for_median();

    // Test COUNT null exclusion with basic data
    let sql_count = "SELECT dict_null_keys, COUNT(value) as cnt FROM t GROUP BY dict_null_keys ORDER BY dict_null_keys NULLS FIRST";
    let results_count = run_snapshot_test(&test_data_basic, sql_count).await?;

    assert_snapshot!(
        batches_to_string(&results_count),
        @r###"
    +----------------+-----+
    | dict_null_keys | cnt |
    +----------------+-----+
    |                | 0   |
    | group_a        | 2   |
    | group_b        | 1   |
    +----------------+-----+
    "###
    );

    // Test SUM null handling with extended data
    let sql_sum = "SELECT dict_null_vals, SUM(value) as total FROM t GROUP BY dict_null_vals ORDER BY dict_null_vals NULLS FIRST";
    let results_sum = run_snapshot_test(&test_data_extended, sql_sum).await?;

    assert_snapshot!(
        batches_to_string(&results_sum),
        @r"
    +----------------+-------+
    | dict_null_vals | total |
    +----------------+-------+
    |                | 4     |
    | group_x        | 4     |
    | group_y        | 2     |
    | group_z        | 5     |
    +----------------+-------+
    "
    );

    // Test MIN null handling with min/max data
    let sql_min = "SELECT dict_null_keys, MIN(value) as minimum FROM t GROUP BY dict_null_keys ORDER BY dict_null_keys NULLS FIRST";
    let results_min = run_snapshot_test(&test_data_min_max, sql_min).await?;

    assert_snapshot!(
        batches_to_string(&results_min),
        @r###"
    +----------------+---------+
    | dict_null_keys | minimum |
    +----------------+---------+
    |                | 2       |
    | group_a        | 3       |
    | group_b        | 1       |
    | group_c        | 7       |
    +----------------+---------+
    "###
    );

    // Test MEDIAN null handling with median data
    let sql_median = "SELECT dict_null_vals, MEDIAN(value) as median_value FROM t GROUP BY dict_null_vals ORDER BY dict_null_vals NULLS FIRST";
    let results_median = run_snapshot_test(&test_data_median, sql_median).await?;

    assert_snapshot!(
        batches_to_string(&results_median),
        @r"
    +----------------+--------------+
    | dict_null_vals | median_value |
    +----------------+--------------+
    |                | 3            |
    | group_x        | 1            |
    | group_y        | 5            |
    | group_z        | 7            |
    +----------------+--------------+
    ");

    Ok(())
}

/// Test FIRST_VAL and LAST_VAL with null values and GROUP BY dict with null keys and null values - may return null if first/last value is null (single and multiple partitions)
#[tokio::test]
async fn test_first_last_val_null_handling() -> Result<()> {
    let test_data = TestData::new_for_first_last();

    // Test FIRST_VALUE and LAST_VALUE with window functions over groups
    let sql = "SELECT dict_null_keys, value, FIRST_VALUE(value) OVER (PARTITION BY dict_null_keys ORDER BY value NULLS FIRST) as first_val, LAST_VALUE(value) OVER (PARTITION BY dict_null_keys ORDER BY value NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_val FROM t ORDER BY dict_null_keys NULLS FIRST, value NULLS FIRST";

    let results_single = run_snapshot_test(&test_data, sql).await?;

    assert_snapshot!(batches_to_string(&results_single), @r"
    +----------------+-------+-----------+----------+
    | dict_null_keys | value | first_val | last_val |
    +----------------+-------+-----------+----------+
    |                | 1     | 1         | 3        |
    |                | 3     | 1         | 3        |
    | group_a        |       |           |          |
    | group_a        |       |           |          |
    | group_b        | 2     | 2         | 2        |
    +----------------+-------+-----------+----------+
    ");

    Ok(())
}

/// Test FIRST_VALUE and LAST_VALUE with ORDER BY - comprehensive null handling
#[tokio::test]
async fn test_first_last_value_order_by_null_handling() -> Result<()> {
    let ctx = SessionContext::new();

    // Create test data with nulls mixed in
    let dict_keys = create_test_dict(
        &[Some("group_a"), Some("group_b"), Some("group_c")],
        &[Some(0), Some(1), Some(2), Some(0), Some(1)],
    );

    let values = Int32Array::from(vec![None, Some(10), Some(20), Some(5), None]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("dict_group", string_dict_type(), true),
        Field::new("value", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(dict_keys), Arc::new(values)],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("test_data", Arc::new(table))?;

    // Test all combinations of FIRST_VALUE and LAST_VALUE with null handling
    let sql = "SELECT 
        dict_group,
        value,
        FIRST_VALUE(value IGNORE NULLS) OVER (ORDER BY value NULLS LAST) as first_ignore_nulls,
        FIRST_VALUE(value RESPECT NULLS) OVER (ORDER BY value NULLS FIRST) as first_respect_nulls,
        LAST_VALUE(value IGNORE NULLS) OVER (ORDER BY value NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_ignore_nulls,
        LAST_VALUE(value RESPECT NULLS) OVER (ORDER BY value NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_respect_nulls
    FROM test_data 
    ORDER BY value NULLS LAST";

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +------------+-------+--------------------+---------------------+-------------------+--------------------+
    | dict_group | value | first_ignore_nulls | first_respect_nulls | last_ignore_nulls | last_respect_nulls |
    +------------+-------+--------------------+---------------------+-------------------+--------------------+
    | group_a    | 5     | 5                  |                     | 20                |                    |
    | group_b    | 10    | 5                  |                     | 20                |                    |
    | group_c    | 20    | 5                  |                     | 20                |                    |
    | group_a    |       | 5                  |                     | 20                |                    |
    | group_b    |       | 5                  |                     | 20                |                    |
    +------------+-------+--------------------+---------------------+-------------------+--------------------+
    "###
    );

    Ok(())
}

/// Test GROUP BY with dictionary columns containing null keys and values for FIRST_VALUE/LAST_VALUE
#[tokio::test]
async fn test_first_last_value_group_by_dict_nulls() -> Result<()> {
    let ctx = SessionContext::new();

    // Create dictionary with null keys
    let dict_null_keys = create_test_dict(
        &[Some("group_a"), Some("group_b")],
        &[
            Some(0), // group_a
            None,    // null key
            Some(1), // group_b
            None,    // null key
            Some(0), // group_a
        ],
    );

    // Create dictionary with null values
    let dict_null_vals = create_test_dict(
        &[Some("val_x"), None, Some("val_y")],
        &[
            Some(0), // val_x
            Some(1), // null value
            Some(2), // val_y
            Some(1), // null value
            Some(0), // val_x
        ],
    );

    // Create test values
    let values = Int32Array::from(vec![Some(10), Some(20), Some(30), Some(40), Some(50)]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("dict_null_keys", string_dict_type(), true),
        Field::new("dict_null_vals", string_dict_type(), true),
        Field::new("value", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(dict_null_keys),
            Arc::new(dict_null_vals),
            Arc::new(values),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("test_data", Arc::new(table))?;

    // Test GROUP BY with null keys
    let sql = "SELECT 
        dict_null_keys,
        FIRST_VALUE(value) as first_val,
        LAST_VALUE(value) as last_val,
        COUNT(*) as cnt
    FROM test_data 
    GROUP BY dict_null_keys 
    ORDER BY dict_null_keys NULLS FIRST";

    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +----------------+-----------+----------+-----+
    | dict_null_keys | first_val | last_val | cnt |
    +----------------+-----------+----------+-----+
    |                | 20        | 40       | 2   |
    | group_a        | 10        | 50       | 2   |
    | group_b        | 30        | 30       | 1   |
    +----------------+-----------+----------+-----+
    "###
    );

    // Test GROUP BY with null values in dictionary
    let sql2 = "SELECT 
        dict_null_vals,
        FIRST_VALUE(value) as first_val,
        LAST_VALUE(value) as last_val,
        COUNT(*) as cnt
    FROM test_data 
    GROUP BY dict_null_vals 
    ORDER BY dict_null_vals NULLS FIRST";

    let df2 = ctx.sql(sql2).await?;
    let results2 = df2.collect().await?;

    assert_snapshot!(
        batches_to_string(&results2),
        @r###"
    +----------------+-----------+----------+-----+
    | dict_null_vals | first_val | last_val | cnt |
    +----------------+-----------+----------+-----+
    |                | 20        | 40       | 2   |
    | val_x          | 10        | 50       | 2   |
    | val_y          | 30        | 30       | 1   |
    +----------------+-----------+----------+-----+
    "###
    );

    Ok(())
}

/// Test MAX with dictionary columns containing null keys and values as specified in the SQL query
#[tokio::test]
async fn test_max_with_fuzz_table_dict_nulls() -> Result<()> {
    let (ctx_single, ctx_multi) = setup_fuzz_test_contexts().await?;

    // Execute the SQL query with MAX aggregations
    let sql = "SELECT
        u8_low,
        dictionary_utf8_low,
        utf8_low,
        max(utf8_low) as col1,
        max(utf8) as col2
    FROM
        fuzz_table
    GROUP BY
        u8_low,
        dictionary_utf8_low,
        utf8_low
    ORDER BY u8_low, dictionary_utf8_low NULLS FIRST, utf8_low";

    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +--------+---------------------+----------+-------+---------+
    | u8_low | dictionary_utf8_low | utf8_low | col1  | col2    |
    +--------+---------------------+----------+-------+---------+
    | 1      |                     | str_b    | str_b | value_2 |
    | 1      | dict_a              | str_a    | str_a | value_5 |
    | 2      |                     | str_c    | str_c | value_7 |
    | 2      |                     | str_d    | str_d | value_4 |
    | 2      | dict_b              | str_c    | str_c | value_3 |
    | 3      |                     | str_e    | str_e |         |
    | 3      | dict_c              | str_f    | str_f | value_6 |
    +--------+---------------------+----------+-------+---------+
    ");

    Ok(())
}

/// Test MIN with fuzz table containing dictionary columns with null keys and values and timestamp data (single and multiple partitions)
#[tokio::test]
async fn test_min_timestamp_with_fuzz_table_dict_nulls() -> Result<()> {
    let (ctx_single, ctx_multi) = setup_fuzz_timestamp_test_contexts().await?;

    // Execute the SQL query with MIN aggregation on timestamp
    let sql = "SELECT
        utf8_low,
        u8_low,
        dictionary_utf8_low,
        min(timestamp_us) as col1
    FROM
        fuzz_table
    GROUP BY
        utf8_low,
        u8_low,
        dictionary_utf8_low
    ORDER BY utf8_low, u8_low, dictionary_utf8_low NULLS FIRST";

    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +----------+--------+---------------------+-------------------------+
    | utf8_low | u8_low | dictionary_utf8_low | col1                    |
    +----------+--------+---------------------+-------------------------+
    | alpha    | 10     | dict_x              | 1970-01-01T00:00:01     |
    | beta     | 20     |                     | 1970-01-01T00:00:02     |
    | delta    | 20     |                     | 1970-01-01T00:00:03.500 |
    | epsilon  | 40     |                     | 1970-01-01T00:00:04     |
    | gamma    | 30     | dict_y              | 1970-01-01T00:00:02.800 |
    | zeta     | 30     | dict_z              | 1970-01-01T00:00:02.500 |
    +----------+--------+---------------------+-------------------------+
    "
    );

    Ok(())
}

/// Test COUNT and COUNT DISTINCT with fuzz table containing dictionary columns with null keys and values (single and multiple partitions)
#[tokio::test]
async fn test_count_distinct_with_fuzz_table_dict_nulls() -> Result<()> {
    let (ctx_single, ctx_multi) = setup_fuzz_count_test_contexts().await?;

    // Execute the SQL query with COUNT and COUNT DISTINCT aggregations
    let sql = "SELECT
        u8_low,
        utf8_low,
        dictionary_utf8_low,
        count(duration_nanosecond) as col1,
        count(DISTINCT large_binary) as col2
    FROM
        fuzz_table
    GROUP BY
        u8_low,
        utf8_low,
        dictionary_utf8_low
    ORDER BY u8_low, utf8_low, dictionary_utf8_low NULLS FIRST";

    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r###"
    +--------+----------+---------------------+------+------+
    | u8_low | utf8_low | dictionary_utf8_low | col1 | col2 |
    +--------+----------+---------------------+------+------+
    | 5      | text_a   | group_alpha         | 3    | 1    |
    | 10     | text_b   |                     | 1    | 1    |
    | 10     | text_d   |                     | 2    | 0    |
    | 15     | text_c   | group_beta          | 1    | 1    |
    | 20     | text_e   |                     | 0    | 1    |
    | 25     | text_f   | group_gamma         | 1    | 1    |
    +--------+----------+---------------------+------+------+
    "###
    );

    Ok(())
}

/// Test MEDIAN and MEDIAN DISTINCT with fuzz table containing various numeric types and dictionary columns with null keys and values (single and multiple partitions)
#[tokio::test]
async fn test_median_distinct_with_fuzz_table_dict_nulls() -> Result<()> {
    let (ctx_single, ctx_multi) = setup_fuzz_median_test_contexts().await?;

    // Execute the SQL query with MEDIAN and MEDIAN DISTINCT aggregations
    let sql = "SELECT
        u8_low,
        dictionary_utf8_low,
        median(DISTINCT u64) as col1,
        median(DISTINCT u16) as col2,
        median(u64) as col3,
        median(decimal128) as col4,
        median(DISTINCT u32) as col5
    FROM
        fuzz_table
    GROUP BY
        u8_low,
        dictionary_utf8_low
    ORDER BY u8_low, dictionary_utf8_low NULLS FIRST";

    let results = test_query_consistency(&ctx_single, &ctx_multi, sql).await?;

    assert_snapshot!(
        batches_to_string(&results),
        @r"
    +--------+---------------------+------+------+------+--------+--------+
    | u8_low | dictionary_utf8_low | col1 | col2 | col3 | col4   | col5   |
    +--------+---------------------+------+------+------+--------+--------+
    | 50     |                     |      | 30   |      | 987.65 | 400000 |
    | 50     | group_three         | 5000 | 50   | 5000 | 555.55 | 500000 |
    | 75     |                     | 4000 |      | 4000 |        | 450000 |
    | 100    | group_one           | 1100 | 11   | 1000 | 123.45 | 110000 |
    | 100    | group_two           | 1500 | 15   | 1500 | 111.11 | 150000 |
    | 200    |                     | 2500 | 22   | 2500 | 506.11 | 250000 |
    +--------+---------------------+------+------+------+--------+--------+
    "
    );

    Ok(())
}
