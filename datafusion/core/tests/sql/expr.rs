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

use datafusion::datasource::empty::EmptyTable;

use super::*;

#[tokio::test]
#[cfg_attr(not(feature = "crypto_expressions"), ignore)]
async fn test_encoding_expressions() -> Result<()> {
    // Input Utf8
    test_expression!("encode('tom','base64')", "dG9t");
    test_expression!("arrow_cast(decode('dG9t','base64'), 'Utf8')", "tom");
    test_expression!("encode('tom','hex')", "746f6d");
    test_expression!("arrow_cast(decode('746f6d','hex'), 'Utf8')", "tom");

    // Input LargeUtf8
    test_expression!("encode(arrow_cast('tom', 'LargeUtf8'),'base64')", "dG9t");
    test_expression!(
        "arrow_cast(decode(arrow_cast('dG9t', 'LargeUtf8'),'base64'), 'Utf8')",
        "tom"
    );
    test_expression!("encode(arrow_cast('tom', 'LargeUtf8'),'hex')", "746f6d");
    test_expression!(
        "arrow_cast(decode(arrow_cast('746f6d', 'LargeUtf8'),'hex'), 'Utf8')",
        "tom"
    );

    // Input Binary
    test_expression!("encode(arrow_cast('tom', 'Binary'),'base64')", "dG9t");
    test_expression!(
        "arrow_cast(decode(arrow_cast('dG9t', 'Binary'),'base64'), 'Utf8')",
        "tom"
    );
    test_expression!("encode(arrow_cast('tom', 'Binary'),'hex')", "746f6d");
    test_expression!(
        "arrow_cast(decode(arrow_cast('746f6d', 'Binary'),'hex'), 'Utf8')",
        "tom"
    );

    // Input LargeBinary
    test_expression!("encode(arrow_cast('tom', 'LargeBinary'),'base64')", "dG9t");
    test_expression!(
        "arrow_cast(decode(arrow_cast('dG9t', 'LargeBinary'),'base64'), 'Utf8')",
        "tom"
    );
    test_expression!("encode(arrow_cast('tom', 'LargeBinary'),'hex')", "746f6d");
    test_expression!(
        "arrow_cast(decode(arrow_cast('746f6d', 'LargeBinary'),'hex'), 'Utf8')",
        "tom"
    );

    // NULL
    test_expression!("encode(NULL,'base64')", "NULL");
    test_expression!("decode(NULL,'base64')", "NULL");
    test_expression!("encode(NULL,'hex')", "NULL");
    test_expression!("decode(NULL,'hex')", "NULL");

    // Empty string
    test_expression!("encode('','base64')", "");
    test_expression!("decode('','base64')", "");
    test_expression!("encode('','hex')", "");
    test_expression!("decode('','hex')", "");

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

#[cfg(feature = "unicode_expressions")]
#[tokio::test]
async fn test_substring_expr() -> Result<()> {
    test_expression!("substring('alphabet' from 2 for 1)", "l");
    test_expression!("substring('alphabet' from 8)", "t");
    test_expression!("substring('alphabet' for 1)", "a");

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
async fn csv_query_nullif_divide_by_0() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c8/nullif(c7, 0) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
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
async fn nested_subquery() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("a", DataType::Int16, false),
    ]);
    let empty_table = Arc::new(EmptyTable::new(Arc::new(schema)));
    ctx.register_table("t1", empty_table.clone())?;
    ctx.register_table("t2", empty_table)?;
    let sql = "SELECT COUNT(*) as cnt \
    FROM (\
        (SELECT id FROM t1) EXCEPT \
        (SELECT id FROM t2)\
        ) foo";
    let actual = execute_to_batches(&ctx, sql).await;
    // the purpose of this test is just to make sure the query produces a valid plan
    #[rustfmt::skip]
    let expected = ["+-----+",
        "| cnt |",
        "+-----+",
        "| 0   |",
        "+-----+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn comparisons_with_null_lt() {
    let ctx = SessionContext::new();

    // we expect all the following queries to yield a two null values
    let cases = vec![
        // 1. Numeric comparison with NULL
        "select column1 < NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 <= NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 > NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 >= NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 = NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 != NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // 1.1 Float value comparison with NULL
        "select column3 < NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // String comparison with NULL
        "select column2 < NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // Boolean comparison with NULL
        "select column1 < NULL from (VALUES (true), (false)) as t",
        // ----
        // ---- same queries, reversed argument order (as they go through
        // ---- a different evaluation path)
        // ----

        // 1. Numeric comparison with NULL
        "select NULL < column1  from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL <= column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL > column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL >= column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL = column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL != column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // 1.1 Float value comparison with NULL
        "select NULL < column3 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // String comparison with NULL
        "select NULL < column2 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // Boolean comparison with NULL
        "select NULL < column1 from (VALUES (true), (false)) as t",
    ];

    for sql in cases {
        println!("Computing: {sql}");

        let mut actual = execute_to_batches(&ctx, sql).await;
        assert_eq!(actual.len(), 1);

        let batch = actual.pop().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.columns()[0].is_null(0));
        assert!(batch.columns()[0].is_null(1));
    }
}

#[tokio::test]
async fn binary_mathematical_operator_with_null_lt() {
    let ctx = SessionContext::new();

    let cases = vec![
        // 1. Integer and NULL
        "select column1 + NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column1 - NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column1 * NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column1 / NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column1 % NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        // 2. Float and NULL
        "select column2 + NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column2 - NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column2 * NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column2 / NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column2 % NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        // ----
        // ---- same queries, reversed argument order
        // ----
        // 3. NULL and Integer
        "select NULL + column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL - column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL * column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL / column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL % column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        // 4. NULL and Float
        "select NULL + column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL - column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL * column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL / column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL % column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
    ];

    for sql in cases {
        println!("Computing: {sql}");

        let mut actual = execute_to_batches(&ctx, sql).await;
        assert_eq!(actual.len(), 1);

        let batch = actual.pop().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.columns()[0].is_null(0));
        assert!(batch.columns()[0].is_null(1));
    }
}

#[tokio::test]
async fn query_binary_eq() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Binary, true),
        Field::new("c2", DataType::LargeBinary, true),
        Field::new("c3", DataType::Binary, true),
        Field::new("c4", DataType::LargeBinary, true),
    ]));

    let c1 = BinaryArray::from_opt_vec(vec![
        Some(b"one"),
        Some(b"two"),
        None,
        Some(b""),
        Some(b"three"),
    ]);
    let c2 = LargeBinaryArray::from_opt_vec(vec![
        Some(b"one"),
        Some(b"two"),
        None,
        Some(b""),
        Some(b"three"),
    ]);
    let c3 = BinaryArray::from_opt_vec(vec![
        Some(b"one"),
        Some(b""),
        None,
        Some(b""),
        Some(b"three"),
    ]);
    let c4 = LargeBinaryArray::from_opt_vec(vec![
        Some(b"one"),
        Some(b"two"),
        None,
        Some(b""),
        Some(b""),
    ]);

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("test", data)?;

    let sql = "
        SELECT sha256(c1)=digest('one', 'sha256'), sha256(c2)=sha256('two'), digest(c1, 'blake2b')=digest(c3, 'blake2b'), c2=c4
        FROM test
    ";
    let actual = execute(&ctx, sql).await;
    let expected = vec![
        vec!["true", "false", "true", "true"],
        vec!["false", "true", "false", "true"],
        vec!["NULL", "NULL", "NULL", "NULL"],
        vec!["false", "false", "true", "true"],
        vec!["false", "false", "true", "false"],
    ];
    assert_eq!(expected, actual);
    Ok(())
}
