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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::array::ArrayRef;
use datafusion_common::ScalarValue;
use rand::random;

use super::record_batch_generator::{RecordBatchGenerator, get_supported_types_columns};

const NUM_SEEDS: usize = 32;

#[test]
fn scalar_value_eq_array_consistency() {
    for_each_array(|context, array| {
        for (index, expected) in extract_scalars(array, context).iter().enumerate() {
            assert!(
                expected
                    .eq_array(array, index)
                    .unwrap_or_else(|e| panic!("eq_array failed for {context}: {e}")),
                "eq_array disagreed with try_from_array at index {index} for {context}"
            );
        }
    });
}

#[test]
fn scalar_value_iter_to_array_roundtrip() {
    for_each_array(|context, array| {
        let expected = extract_scalars(array, context);
        let rebuilt = ScalarValue::iter_to_array(expected.iter().cloned())
            .unwrap_or_else(|e| panic!("iter_to_array failed for {context}: {e}"));

        assert_eq!(
            rebuilt.data_type(),
            array.data_type(),
            "iter_to_array changed the type for {context}"
        );
        assert_eq!(
            rebuilt.len(),
            array.len(),
            "iter_to_array changed the length for {context}"
        );

        let actual = extract_scalars(&rebuilt, context);
        assert_eq!(
            actual, expected,
            "iter_to_array changed values for {context}"
        );
    });
}

#[test]
fn scalar_value_to_array_roundtrip() {
    for_each_array(|context, array| {
        for (index, expected) in extract_scalars(array, context).iter().enumerate() {
            let singleton = expected
                .to_array()
                .unwrap_or_else(|e| panic!("to_array failed for {context}: {e}"));
            assert_eq!(
                singleton.len(),
                1,
                "to_array returned the wrong length at index {index} for {context}"
            );

            let actual = ScalarValue::try_from_array(&singleton, 0).unwrap_or_else(|e| {
                panic!(
                    "try_from_array failed on to_array output at index {index} for {context}: {e}"
                )
            });
            assert_eq!(
                &actual, expected,
                "to_array roundtrip changed index {index} for {context}"
            );
        }
    });
}

fn for_each_array(check: impl Fn(&str, &ArrayRef)) {
    for _ in 0..NUM_SEEDS {
        let seed = random();
        let columns = get_supported_types_columns(seed);
        let batch = RecordBatchGenerator::new(1, 128, columns)
            .with_seed(seed)
            .generate()
            .unwrap_or_else(|e| panic!("failed to generate batch for seed {seed}: {e}"));

        for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
            let check_array = |array: &ArrayRef| {
                let context = format!(
                    "seed={seed}, column={}, type={}, len={}, offset={}",
                    field.name(),
                    array.data_type(),
                    array.len(),
                    array.offset()
                );
                check(&context, array);
            };

            check_array(array);

            // Also exercise arrays with non-zero offsets.
            if array.len() > 2 {
                check_array(&array.slice(1, array.len() - 2));
            }
        }
    }
}

fn extract_scalars(array: &ArrayRef, context: &str) -> Vec<ScalarValue> {
    (0..array.len())
        .map(|index| {
            ScalarValue::try_from_array(array, index).unwrap_or_else(|e| {
                panic!("try_from_array failed at index {index} for {context}: {e}")
            })
        })
        .collect()
}
