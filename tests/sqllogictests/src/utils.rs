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

use datafusion::arrow::{
    array::{Array, Decimal128Builder},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use std::sync::Arc;

// TODO: move this to datafusion::test_utils?
pub fn make_decimal() -> RecordBatch {
    let mut decimal_builder = Decimal128Builder::with_capacity(20);
    for i in 110000..110010 {
        decimal_builder.append_value(i as i128);
    }
    for i in 100000..100010 {
        decimal_builder.append_value(-i as i128);
    }
    let array = decimal_builder
        .finish()
        .with_precision_and_scale(10, 3)
        .unwrap();
    let schema = Schema::new(vec![Field::new("c1", array.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}
