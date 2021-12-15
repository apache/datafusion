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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::{
    array::{Int32Array, StringArray},
    record_batch::RecordBatch,
};

use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;

use datafusion::error::Result;

// use datafusion::logical_plan::Expr;
use datafusion::prelude::*;

use datafusion::execution::context::ExecutionContext;

use datafusion::assert_batches_eq;

fn create_test_table() -> Result<Arc<dyn DataFrame>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "abcDEF",
                "abc123",
                "CBAdef",
                "123AbcDef",
            ])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let mut ctx = ExecutionContext::new();

    let table = MemTable::try_new(schema, vec![vec![batch]])?;

    ctx.register_table("test", Arc::new(table))?;

    ctx.table("test")
}

/// Excutes an expression on the test dataframe as a select.
/// Compares formatted output of a record batch with an expected
/// vector of strings, using the assert_batch_eq! macro
macro_rules! assert_fn_batches {
    ($EXPR:expr, $EXPECTED: expr) => {
        assert_fn_batches!($EXPR, $EXPECTED, 10)
    };
    ($EXPR:expr, $EXPECTED: expr, $LIMIT: expr) => {
        let df = create_test_table()?;
        let df = df.select(vec![$EXPR])?.limit($LIMIT)?;
        let batches = df.collect().await?;

        assert_batches_eq!($EXPECTED, &batches);
    };
}

#[tokio::test]
async fn test_fn_ascii() -> Result<()> {
    let expr = ascii(col("a"));

    let expected = vec![
        "+---------------+",
        "| ascii(test.a) |",
        "+---------------+",
        "| 97            |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_bit_length() -> Result<()> {
    let expr = bit_length(col("a"));

    let expected = vec![
        "+-------------------+",
        "| bitlength(test.a) |",
        "+-------------------+",
        "| 48                |",
        "| 48                |",
        "| 48                |",
        "| 72                |",
        "+-------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_btrim() -> Result<()> {
    let expr = btrim(vec![lit("      a b c             ")]);

    let expected = vec![
        "+-----------------------------------------+",
        "| btrim(Utf8(\"      a b c             \")) |",
        "+-----------------------------------------+",
        "| a b c                                   |",
        "+-----------------------------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_btrim_with_chars() -> Result<()> {
    let expr = btrim(vec![col("a"), lit("ab")]);

    let expected = vec![
        "+--------------------------+",
        "| btrim(test.a,Utf8(\"ab\")) |",
        "+--------------------------+",
        "| cDEF                     |",
        "| c123                     |",
        "| CBAdef                   |",
        "| 123AbcDef                |",
        "+--------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_character_length() -> Result<()> {
    let expr = character_length(col("a"));

    let expected = vec![
        "+-------------------------+",
        "| characterlength(test.a) |",
        "+-------------------------+",
        "| 6                       |",
        "| 6                       |",
        "| 6                       |",
        "| 9                       |",
        "+-------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_chr() -> Result<()> {
    let expr = chr(lit(128175));

    let expected = vec![
        "+--------------------+",
        "| chr(Int32(128175)) |",
        "+--------------------+",
        "| 💯                 |",
        "+--------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_initcap() -> Result<()> {
    let expr = initcap(col("a"));

    let expected = vec![
        "+-----------------+",
        "| initcap(test.a) |",
        "+-----------------+",
        "| Abcdef          |",
        "| Abc123          |",
        "| Cbadef          |",
        "| 123abcdef       |",
        "+-----------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_left() -> Result<()> {
    let expr = left(col("a"), lit(3));

    let expected = vec![
        "+-----------------------+",
        "| left(test.a,Int32(3)) |",
        "+-----------------------+",
        "| abc                   |",
        "| abc                   |",
        "| CBA                   |",
        "| 123                   |",
        "+-----------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_lower() -> Result<()> {
    let expr = lower(col("a"));

    let expected = vec![
        "+---------------+",
        "| lower(test.a) |",
        "+---------------+",
        "| abcdef        |",
        "| abc123        |",
        "| cbadef        |",
        "| 123abcdef     |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_lpad() -> Result<()> {
    let expr = lpad(vec![col("a"), lit(10)]);

    let expected = vec![
        "+------------------------+",
        "| lpad(test.a,Int32(10)) |",
        "+------------------------+",
        "|     abcDEF             |",
        "|     abc123             |",
        "|     CBAdef             |",
        "|  123AbcDef             |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_lpad_with_string() -> Result<()> {
    let expr = lpad(vec![col("a"), lit(10), lit("*")]);

    let expected = vec![
        "+----------------------------------+",
        "| lpad(test.a,Int32(10),Utf8(\"*\")) |",
        "+----------------------------------+",
        "| ****abcDEF                       |",
        "| ****abc123                       |",
        "| ****CBAdef                       |",
        "| *123AbcDef                       |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_ltrim() -> Result<()> {
    let expr = ltrim(lit("      a b c             "));

    let expected = vec![
        "+-----------------------------------------+",
        "| ltrim(Utf8(\"      a b c             \")) |",
        "+-----------------------------------------+",
        "| a b c                                   |",
        "+-----------------------------------------+",
    ];

    assert_fn_batches!(expr, expected, 1);

    Ok(())
}

#[tokio::test]
async fn test_fn_ltrim_with_columns() -> Result<()> {
    let expr = ltrim(col("a"));

    let expected = vec![
        "+---------------+",
        "| ltrim(test.a) |",
        "+---------------+",
        "| abcDEF        |",
        "| abc123        |",
        "| CBAdef        |",
        "| 123AbcDef     |",
        "+---------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_md5() -> Result<()> {
    let expr = md5(col("a"));

    let expected = vec![
        "+----------------------------------+",
        "| md5(test.a)                      |",
        "+----------------------------------+",
        "| ea2de8bd80f3a1f52c754214fc9b0ed1 |",
        "| e99a18c428cb38d5f260853678922e03 |",
        "| 11ed4a6e9985df40913eead67f022e27 |",
        "| 8f5e60e523c9253e623ae38bb58c399a |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

// TODO: tobyhede - Issue #1429
//       https://github.com/apache/arrow-datafusion/issues/1429
//       g flag doesn't compile
#[tokio::test]
async fn test_fn_regexp_match() -> Result<()> {
    let expr = regexp_match(vec![col("a"), lit("[a-z]")]);
    // The below will fail
    // let expr = regexp_match( vec![col("a"), lit("[a-z]"), lit("g")]);

    let expected = vec![
        "+-----------------------------------+",
        "| regexpmatch(test.a,Utf8(\"[a-z]\")) |",
        "+-----------------------------------+",
        "| []                                |",
        "| []                                |",
        "| []                                |",
        "| []                                |",
        "+-----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_regexp_replace() -> Result<()> {
    let expr = regexp_replace(vec![col("a"), lit("[a-z]"), lit("x"), lit("g")]);

    let expected = vec![
        "+---------------------------------------------------------+",
        "| regexpreplace(test.a,Utf8(\"[a-z]\"),Utf8(\"x\"),Utf8(\"g\")) |",
        "+---------------------------------------------------------+",
        "| xxxDEF                                                  |",
        "| xxx123                                                  |",
        "| CBAxxx                                                  |",
        "| 123AxxDxx                                               |",
        "+---------------------------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_replace() -> Result<()> {
    let expr = replace(col("a"), lit("abc"), lit("x"));

    let expected = vec![
        "+---------------------------------------+",
        "| replace(test.a,Utf8(\"abc\"),Utf8(\"x\")) |",
        "+---------------------------------------+",
        "| xDEF                                  |",
        "| x123                                  |",
        "| CBAdef                                |",
        "| 123AbcDef                             |",
        "+---------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_repeat() -> Result<()> {
    let expr = repeat(col("a"), lit(2));

    let expected = vec![
        "+-------------------------+",
        "| repeat(test.a,Int32(2)) |",
        "+-------------------------+",
        "| abcDEFabcDEF            |",
        "| abc123abc123            |",
        "| CBAdefCBAdef            |",
        "| 123AbcDef123AbcDef      |",
        "+-------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_reverse() -> Result<()> {
    let expr = reverse(col("a"));

    let expected = vec![
        "+-----------------+",
        "| reverse(test.a) |",
        "+-----------------+",
        "| FEDcba          |",
        "| 321cba          |",
        "| fedABC          |",
        "| feDcbA321       |",
        "+-----------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_right() -> Result<()> {
    let expr = right(col("a"), lit(3));

    let expected = vec![
        "+------------------------+",
        "| right(test.a,Int32(3)) |",
        "+------------------------+",
        "| DEF                    |",
        "| 123                    |",
        "| def                    |",
        "| Def                    |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_rpad() -> Result<()> {
    let expr = rpad(vec![col("a"), lit(11)]);

    let expected = vec![
        "+------------------------+",
        "| rpad(test.a,Int32(11)) |",
        "+------------------------+",
        "| abcDEF                 |",
        "| abc123                 |",
        "| CBAdef                 |",
        "| 123AbcDef              |",
        "+------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_rpad_with_characters() -> Result<()> {
    let expr = rpad(vec![col("a"), lit(11), lit("x")]);

    let expected = vec![
        "+----------------------------------+",
        "| rpad(test.a,Int32(11),Utf8(\"x\")) |",
        "+----------------------------------+",
        "| abcDEFxxxxx                      |",
        "| abc123xxxxx                      |",
        "| CBAdefxxxxx                      |",
        "| 123AbcDefxx                      |",
        "+----------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_sha224() -> Result<()> {
    let expr = sha224(col("a"));

    let expected = vec![
        "+----------------------------------------------------------+",
        "| sha224(test.a)                                           |",
        "+----------------------------------------------------------+",
        "| 8b9ef961d2b19cfe7ee2a8452e3adeea98c7b22954b4073976bf80ee |",
        "| 5c69bb695cc29b93d655e1a4bb5656cda624080d686f74477ea09349 |",
        "| b3b3783b7470594e7ddb845eca0aec5270746dd6d0bc309bb948ceab |",
        "| fc8a30d59386d78053328440c6670c3b583404a905cbe9bbd491a517 |",
        "+----------------------------------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_split_part() -> Result<()> {
    let expr = split_part(col("a"), lit("b"), lit(1));

    let expected = vec![
        "+--------------------------------------+",
        "| splitpart(test.a,Utf8(\"b\"),Int32(1)) |",
        "+--------------------------------------+",
        "| a                                    |",
        "| a                                    |",
        "| CBAdef                               |",
        "| 123A                                 |",
        "+--------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_starts_with() -> Result<()> {
    let expr = starts_with(col("a"), lit("abc"));

    let expected = vec![
        "+--------------------------------+",
        "| startswith(test.a,Utf8(\"abc\")) |",
        "+--------------------------------+",
        "| true                           |",
        "| true                           |",
        "| false                          |",
        "| false                          |",
        "+--------------------------------+",
    ];

    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_strpos() -> Result<()> {
    let expr = strpos(col("a"), lit("f"));

    let expected = vec![
        "+--------------------------+",
        "| strpos(test.a,Utf8(\"f\")) |",
        "+--------------------------+",
        "| 0                        |",
        "| 0                        |",
        "| 6                        |",
        "| 9                        |",
        "+--------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_substr() -> Result<()> {
    let expr = substr(col("a"), lit(2));

    let expected = vec![
        "+-------------------------+",
        "| substr(test.a,Int32(2)) |",
        "+-------------------------+",
        "| bcDEF                   |",
        "| bc123                   |",
        "| BAdef                   |",
        "| 23AbcDef                |",
        "+-------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_to_hex() -> Result<()> {
    let expr = to_hex(col("b"));

    let expected = vec![
        "+---------------+",
        "| tohex(test.b) |",
        "+---------------+",
        "| 1             |",
        "| a             |",
        "| a             |",
        "| 64            |",
        "+---------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_translate() -> Result<()> {
    let expr = translate(col("a"), lit("bc"), lit("xx"));

    let expected = vec![
        "+-----------------------------------------+",
        "| translate(test.a,Utf8(\"bc\"),Utf8(\"xx\")) |",
        "+-----------------------------------------+",
        "| axxDEF                                  |",
        "| axx123                                  |",
        "| CBAdef                                  |",
        "| 123AxxDef                               |",
        "+-----------------------------------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}

#[tokio::test]
async fn test_fn_upper() -> Result<()> {
    let expr = upper(col("a"));

    let expected = vec![
        "+---------------+",
        "| upper(test.a) |",
        "+---------------+",
        "| ABCDEF        |",
        "| ABC123        |",
        "| CBADEF        |",
        "| 123ABCDEF     |",
        "+---------------+",
    ];
    assert_fn_batches!(expr, expected);

    Ok(())
}
