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
async fn csv_query_sum_cast() {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv_by_sql(&mut ctx).await;
    // c8 = i32; c9 = i64
    let sql = "SELECT c8 + c9 FROM aggregate_test_100";
    // check that the physical and logical schemas are equal
    execute(&mut ctx, sql).await;
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
async fn test_cast_expressions() -> Result<()> {
    test_expression!("CAST('0' AS INT)", "0");
    test_expression!("CAST(NULL AS INT)", "NULL");
    test_expression!("TRY_CAST('0' AS INT)", "0");
    test_expression!("TRY_CAST('x' AS INT)", "NULL");
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
