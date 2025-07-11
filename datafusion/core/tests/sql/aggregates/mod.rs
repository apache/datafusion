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

//! Aggregate function tests

use super::*;
use arrow::datatypes::TimeUnit;
use datafusion::common::test_util::batches_to_string;
use datafusion_catalog::MemTable;
use datafusion_common::ScalarValue;
use insta::assert_snapshot;
use std::cmp::min;

/// Helper function to create the commonly used UInt32 indexed UTF-8 dictionary data type
pub fn string_dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
}

pub mod basic;
pub mod dict_nulls;
