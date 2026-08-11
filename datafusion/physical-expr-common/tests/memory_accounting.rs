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

use arrow::array::{ArrayRef, StringArray, StringViewArray};
use datafusion_physical_expr_common::binary_map::{ArrowBytesMap, OutputType};
use datafusion_physical_expr_common::binary_view_map::ArrowBytesViewMap;

type LargePayload = [u8; 32];

#[test]
fn arrow_bytes_map_size_accounts_for_hash_table_allocations() {
    let mut unit_map = ArrowBytesMap::<i32, ()>::new(OutputType::Utf8);
    let mut payload_map = ArrowBytesMap::<i32, LargePayload>::new(OutputType::Utf8);

    let initial_allocation_difference = payload_map.size() - unit_map.size();
    assert!(initial_allocation_difference > 0);

    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..1024).map(|index| format!("value-{index}")),
    ));
    unit_map.insert_if_new(&values, |_| (), |_| {});
    payload_map.insert_if_new(&values, |_| LargePayload::default(), |_| {});

    let grown_allocation_difference = payload_map.size() - unit_map.size();
    assert!(grown_allocation_difference > initial_allocation_difference);

    let populated_unit_map = unit_map.take();
    let populated_payload_map = payload_map.take();
    assert_eq!(
        populated_payload_map.size() - populated_unit_map.size(),
        grown_allocation_difference
    );
    assert_eq!(
        payload_map.size() - unit_map.size(),
        initial_allocation_difference
    );
}

#[test]
fn arrow_bytes_view_map_size_accounts_for_hash_table_allocations() {
    let mut unit_map = ArrowBytesViewMap::<()>::new(OutputType::Utf8View);
    let mut payload_map = ArrowBytesViewMap::<LargePayload>::new(OutputType::Utf8View);

    let initial_allocation_difference = payload_map.size() - unit_map.size();
    assert!(initial_allocation_difference > 0);

    let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
        (0..1024).map(|index| format!("value-{index}")),
    ));
    unit_map.insert_if_new(&values, |_| (), |_| {});
    payload_map.insert_if_new(&values, |_| LargePayload::default(), |_| {});

    let grown_allocation_difference = payload_map.size() - unit_map.size();
    assert!(grown_allocation_difference > initial_allocation_difference);

    let populated_unit_map = unit_map.take();
    let populated_payload_map = payload_map.take();
    assert_eq!(
        populated_payload_map.size() - populated_unit_map.size(),
        grown_allocation_difference
    );
    assert_eq!(
        payload_map.size() - unit_map.size(),
        initial_allocation_difference
    );
}
