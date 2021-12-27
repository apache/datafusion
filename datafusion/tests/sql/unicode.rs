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
async fn query_length() -> Result<()> {
    generic_query_length::<StringArray>(DataType::Utf8).await
}

#[tokio::test]
async fn query_large_length() -> Result<()> {
    generic_query_length::<LargeStringArray>(DataType::LargeUtf8).await
}

#[tokio::test]
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
