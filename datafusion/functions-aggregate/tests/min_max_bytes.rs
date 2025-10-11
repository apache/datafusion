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

use arrow::datatypes::DataType;
use datafusion_expr::EmitTo;
use datafusion_functions_aggregate::min_max::min_max_bytes::{
    BatchStats, MinMaxBytesState, ScratchEntry, ScratchLocation, SimpleSlot,
    WorkloadMode, DENSE_INLINE_MAX_TOTAL_GROUPS, DENSE_INLINE_STABILITY_THRESHOLD,
    SPARSE_SWITCH_GROUP_THRESHOLD,
};
use rand::{rngs::StdRng, Rng, SeedableRng};

#[allow(dead_code)]
#[derive(Debug)]
enum Operation {
    Expand {
        new_total: usize,
    },
    Update {
        total_groups: usize,
        groups: Vec<usize>,
        values: Vec<Option<Vec<u8>>>,
    },
    Emit {
        emit_count: usize,
    },
}

fn random_ascii_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    (0..len)
        .map(|_| {
            let offset = rng.random_range(0..26_u8);
            b'a' + offset
        })
        .collect()
}

fn random_binary_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
    (0..len).map(|_| rng.random_range(0..=u8::MAX)).collect()
}

#[test]
fn min_updates_across_batches_dense_inline_variants() {
    fn run_scenario(data_type: DataType) {
        let mut state = MinMaxBytesState::new(data_type.clone());
        let total_groups = 4_usize;
        let group_indices = [0_usize, 1, 2, 3, 0];
        let first_values = ["m0", "n1", "o2", "p3", "z9"];
        let second_values = ["a0", "n1", "o2", "p3", "z9"];

        let first_batch: Vec<Vec<u8>> = first_values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect();
        state
            .update_batch(
                first_batch.iter().map(|value| Some(value.as_slice())),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .expect("first batch");

        assert!(
            matches!(state.workload_mode, WorkloadMode::DenseInline),
            "expected DenseInline for {data_type:?}, found {:?}",
            state.workload_mode
        );
        assert_eq!(
            state.min_max[0].as_deref(),
            Some(first_values[0].as_bytes()),
            "initial minimum should match first batch for {data_type:?}"
        );

        let second_batch: Vec<Vec<u8>> = second_values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect();
        state
            .update_batch(
                second_batch.iter().map(|value| Some(value.as_slice())),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .expect("second batch");

        assert_eq!(
            state.min_max[0].as_deref(),
            Some(second_values[0].as_bytes()),
            "second batch should lower the minimum for {data_type:?}"
        );
    }

    for data_type in [DataType::Utf8, DataType::Binary, DataType::BinaryView] {
        run_scenario(data_type);
    }
}

#[test]
fn randomized_min_matches_reference() {
    let mut rng = StdRng::seed_from_u64(0xDAB5_C0DE);

    for data_type in [DataType::Utf8, DataType::Binary, DataType::BinaryView] {
        for trial in 0..256 {
            let max_total_groups = rng.random_range(1..=48_usize);
            let mut current_total = rng.random_range(1..=max_total_groups);
            let mut state = MinMaxBytesState::new(data_type.clone());
            let mut expected: Vec<Option<Vec<u8>>> = vec![None; current_total];
            let batches = rng.random_range(1..=8_usize);
            let mut history = Vec::new();

            for _ in 0..batches {
                if current_total == 0 {
                    current_total = rng.random_range(1..=max_total_groups);
                    expected.resize(current_total, None);
                    history.push(Operation::Expand {
                        new_total: current_total,
                    });
                } else if rng.random_bool(0.3) && current_total < max_total_groups {
                    let new_total =
                        rng.random_range((current_total + 1)..=max_total_groups);
                    expected.resize(new_total, None);
                    current_total = new_total;
                    history.push(Operation::Expand {
                        new_total: current_total,
                    });
                }

                let batch_len = rng.random_range(1..=48_usize);
                let mut group_indices = Vec::with_capacity(batch_len);
                let mut values: Vec<Option<Vec<u8>>> = Vec::with_capacity(batch_len);

                for _ in 0..batch_len {
                    let group_index = rng.random_range(0..current_total);
                    group_indices.push(group_index);

                    if rng.random_bool(0.1) {
                        values.push(None);
                    } else {
                        let len = rng.random_range(0..=12_usize);
                        let bytes = match data_type {
                            DataType::Utf8 => random_ascii_bytes(&mut rng, len),
                            DataType::Binary | DataType::BinaryView => {
                                random_binary_bytes(&mut rng, len)
                            }
                            other => unreachable!(
                                "randomized_min_matches_reference unexpected data type {other:?}"
                            ),
                        };
                        values.push(Some(bytes));
                    }
                }

                let iter = values
                    .iter()
                    .map(|value| value.as_ref().map(|bytes| bytes.as_slice()));
                history.push(Operation::Update {
                    total_groups: current_total,
                    groups: group_indices.clone(),
                    values: values.clone(),
                });

                state
                    .update_batch(iter, &group_indices, current_total, |a, b| a < b)
                    .expect("randomized batch");

                for (group_index, value) in group_indices.into_iter().zip(values) {
                    if let Some(bytes) = value {
                        let entry = &mut expected[group_index];
                        let should_replace = entry
                            .as_ref()
                            .map(|existing| bytes.as_slice() < existing.as_slice())
                            .unwrap_or(true);
                        if should_replace {
                            *entry = Some(bytes);
                        }
                    }
                }

                if rng.random_bool(0.2) && !state.min_max.is_empty() {
                    let emit_count = rng.random_range(1..=state.min_max.len());
                    let _ = state.emit_to(EmitTo::First(emit_count));
                    expected.drain(..emit_count);
                    current_total = expected.len();
                    history.push(Operation::Emit { emit_count });
                }
            }

            assert_eq!(state.min_max.len(), expected.len());

            for (group_index, expected_bytes) in expected.iter().enumerate() {
                let actual = state.min_max[group_index]
                    .as_ref()
                    .map(|buffer| buffer.as_slice());
                let expected = expected_bytes.as_ref().map(|buffer| buffer.as_slice());
                assert_eq!(
                    actual, expected,
                    "randomized min mismatch for {:?} in group {group_index} (trial {trial}) history: {:?}",
                    data_type,
                    history
                );
            }
        }
    }
}

#[test]
fn reproduces_randomized_failure_case() {
    fn apply_update(
        state: &mut MinMaxBytesState,
        expected: &mut Vec<Option<Vec<u8>>>,
        total: usize,
        groups: Vec<usize>,
        values: Vec<Option<Vec<u8>>>,
    ) {
        if expected.len() < total {
            expected.resize(total, None);
        }

        let iter = values
            .iter()
            .map(|value| value.as_ref().map(|bytes| bytes.as_slice()));

        state
            .update_batch(iter, &groups, total, |a, b| a < b)
            .expect("structured update");

        for (group_index, value) in groups.into_iter().zip(values) {
            if let Some(bytes) = value {
                let entry = &mut expected[group_index];
                let should_replace = entry
                    .as_ref()
                    .map(|existing| bytes.as_slice() < existing.as_slice())
                    .unwrap_or(true);
                if should_replace {
                    *entry = Some(bytes);
                }
            }
        }
    }

    let mut state = MinMaxBytesState::new(DataType::Utf8);
    let mut expected: Vec<Option<Vec<u8>>> = Vec::new();

    {
        let groups = vec![23, 28];
        let values = vec![
            Some(vec![121, 103, 113, 122, 115, 111, 104, 101, 100]),
            Some(vec![121, 112, 107, 97]),
        ];
        apply_update(&mut state, &mut expected, 45, groups, values);
    }
    assert_eq!(state.emit_to(EmitTo::First(11)).1.len(), 11);
    expected.drain(..11);

    {
        let groups = vec![
            33, 17, 31, 0, 27, 3, 12, 6, 3, 27, 20, 28, 2, 9, 0, 1, 17, 33, 25, 28, 20,
            2, 29, 10, 32, 28, 32, 26, 2, 27, 22, 27, 14, 32, 30, 23, 13, 19, 26, 14, 26,
            32, 4, 32, 14, 21,
        ];
        let values = vec![
            Some(vec![118, 114, 97, 97]),
            Some(vec![108]),
            Some(vec![114, 118, 106, 99, 122, 103, 122]),
            Some(vec![
                98, 112, 103, 114, 99, 100, 111, 113, 114, 100, 121, 115,
            ]),
            Some(vec![114, 105, 114, 113, 110, 122]),
            Some(vec![105, 117]),
            Some(vec![111, 119, 106, 99, 98, 100, 102, 100, 99, 102]),
            Some(vec![116, 118, 98, 121]),
            Some(vec![114, 119, 117, 107, 118, 115]),
            Some(vec![110, 113, 103, 114, 120, 109, 108, 117]),
            Some(vec![105, 121, 97, 111, 99, 101, 118, 122, 121]),
            Some(vec![115, 121, 111, 121, 120, 97, 109, 109, 104, 105, 108]),
            Some(vec![117, 101]),
            Some(vec![112, 107, 113, 105]),
            None,
            Some(vec![99, 117, 114, 103, 118, 107, 107]),
            Some(vec![]),
            Some(vec![]),
            Some(vec![113, 98, 104, 119, 101]),
            Some(vec![122, 114]),
            Some(vec![119, 98]),
            Some(vec![101, 99, 111, 116, 112, 116, 113, 101, 113]),
            Some(vec![114, 109, 101, 107, 117, 111, 106]),
            None,
            Some(vec![121, 111, 118, 106, 116, 120, 108, 119, 118]),
            Some(vec![]),
            None,
            Some(vec![108]),
            Some(vec![
                121, 102, 105, 97, 118, 117, 120, 97, 109, 118, 97, 122,
            ]),
            Some(vec![98, 102, 118, 108]),
            Some(vec![117, 106, 116, 103, 122]),
            Some(vec![104, 103, 117, 107, 118]),
            Some(vec![109, 99, 112, 112, 106, 109]),
            Some(vec![117, 100, 116, 117, 120, 116, 100, 111, 119, 120]),
            Some(vec![109, 104, 99, 98]),
            Some(vec![107]),
            Some(vec![114, 107, 110, 112, 100, 98]),
            Some(vec![122, 110, 103, 104]),
            Some(vec![103, 113, 122, 104, 107, 117, 113, 122, 106]),
            Some(vec![
                122, 114, 116, 101, 106, 102, 118, 106, 114, 104, 122, 105,
            ]),
            Some(vec![98, 106, 107, 115, 115, 118, 122]),
            Some(vec![
                114, 122, 107, 115, 108, 105, 99, 122, 106, 110, 122, 103,
            ]),
            Some(vec![119, 106, 120, 104, 115, 118, 108, 113, 120, 122, 121]),
            Some(vec![113, 104, 113, 101, 98, 122, 97, 100, 106]),
            Some(vec![105]),
            Some(vec![]),
        ];
        apply_update(&mut state, &mut expected, 34, groups, values);
    }

    {
        let groups = vec![
            38, 22, 20, 37, 0, 33, 9, 9, 8, 21, 34, 32, 8, 20, 8, 1, 25, 27, 17, 3, 20,
            32, 34, 36, 8, 29, 2, 39, 38, 20, 38, 16, 11, 13, 15, 22, 30, 15, 13,
        ];
        let values = vec![
            Some(vec![104, 107, 105, 101, 99, 118]),
            Some(vec![100, 110, 114]),
            Some(vec![120, 107, 119, 111, 118]),
            Some(vec![121, 120, 109, 109, 118, 97, 119, 122, 110, 115]),
            Some(vec![111, 106]),
            Some(vec![98, 113, 114, 116]),
            Some(vec![114, 113, 105, 113, 122, 110, 105, 97, 100]),
            Some(vec![97, 116, 107, 102, 97, 107]),
            Some(vec![
                102, 103, 105, 115, 121, 119, 103, 107, 118, 100, 101, 99,
            ]),
            Some(vec![]),
            Some(vec![99, 102, 110, 109, 103, 109, 120]),
            Some(vec![104]),
            Some(vec![
                107, 101, 101, 115, 115, 97, 115, 114, 101, 113, 121, 97,
            ]),
            Some(vec![114]),
            Some(vec![116, 118, 113, 106, 109, 120, 100, 121, 99]),
            Some(vec![114, 100, 110, 111, 100, 110, 98]),
            Some(vec![114, 105, 111, 104, 111, 100, 98, 114, 99, 113]),
            Some(vec![122, 100, 97, 119, 121, 101, 117, 104, 110, 113]),
            Some(vec![116, 109, 114, 110, 103, 121, 108, 114]),
            Some(vec![
                106, 122, 102, 120, 105, 103, 122, 109, 118, 113, 100, 118,
            ]),
            None,
            Some(vec![114, 112, 97, 102, 113, 114, 107, 104]),
            None,
            Some(vec![116, 102]),
            Some(vec![100, 116, 103, 104, 97, 114, 117]),
            Some(vec![117, 119, 107, 104, 106, 99, 120, 103]),
            Some(vec![104]),
            Some(vec![]),
            Some(vec![120, 115, 122, 119, 97, 102, 110, 100, 118, 117, 97]),
            Some(vec![
                98, 112, 121, 102, 118, 101, 100, 110, 108, 118, 108, 100,
            ]),
            Some(vec![117, 114, 115, 111, 122, 98, 98, 115, 112, 100]),
            Some(vec![106, 99, 113, 116, 103, 100, 110, 117, 102, 122, 104]),
            Some(vec![
                102, 101, 121, 97, 121, 99, 98, 104, 103, 100, 112, 113,
            ]),
            Some(vec![114, 107, 100, 101]),
            Some(vec![98, 115, 112, 100, 106, 119, 103, 104, 111]),
            Some(vec![]),
            Some(vec![121, 116, 112, 121, 114, 110, 104, 119]),
            Some(vec![99, 104, 101, 109, 115, 101, 105]),
            Some(vec![97, 104]),
        ];
        apply_update(&mut state, &mut expected, 40, groups, values);
    }

    assert_eq!(
        state.min_max[38].as_ref().map(|buffer| buffer.as_slice()),
        expected[38].as_ref().map(|buffer| buffer.as_slice()),
        "state should hold expected minimum before re-expansion"
    );

    {
        let groups = vec![
            33, 24, 30, 5, 24, 13, 0, 8, 24, 40, 27, 25, 14, 8, 36, 23, 28, 22, 14, 20,
            23, 10, 28, 22, 31, 35, 13, 11, 10, 36, 39, 4, 40, 5, 13, 1, 20, 17, 0, 5, 3,
            24, 19, 38,
        ];
        let values = vec![
            Some(vec![106, 98, 105, 119, 115, 110, 116, 119, 111, 104, 118]),
            Some(vec![]),
            Some(vec![
                108, 115, 97, 110, 112, 105, 102, 100, 117, 114, 110, 116,
            ]),
            None,
            Some(vec![111, 114, 110]),
            Some(vec![107]),
            Some(vec![111, 106, 121, 114, 113, 105]),
            Some(vec![100, 109, 119, 122, 111, 105, 116, 104]),
            Some(vec![98, 103]),
            Some(vec![118, 99, 118, 118, 115, 116, 104, 110, 114, 115, 115]),
            Some(vec![102, 107]),
            Some(vec![105, 107, 119, 115, 98, 110, 110]),
            Some(vec![120, 121, 114, 121, 102, 120, 117, 109, 122]),
            Some(vec![104, 101, 115, 104, 103, 106]),
            Some(vec![108, 97, 99, 111]),
            Some(vec![98, 115, 102, 98, 101, 109, 120, 118, 112, 104, 102]),
            Some(vec![]),
            Some(vec![122, 116, 111, 107, 107]),
            Some(vec![97, 118, 104, 111, 122, 100, 99, 106, 101, 107, 104]),
            Some(vec![105, 119, 114, 99, 122]),
            Some(vec![106, 122, 117, 116, 111, 104, 109, 105, 111, 121, 122]),
            Some(vec![
                107, 106, 111, 109, 107, 97, 105, 104, 117, 98, 105, 114,
            ]),
            Some(vec![115, 116, 120, 102, 109, 112, 122, 102, 102, 120, 110]),
            Some(vec![114, 105, 109]),
            Some(vec![117, 97, 121, 109, 120, 109, 122, 101, 112, 104]),
            Some(vec![103, 111, 99]),
            Some(vec![120, 120, 115, 101, 101, 109, 100, 122]),
            Some(vec![115, 107, 121, 122, 121, 108, 118]),
            Some(vec![107, 109, 120, 102, 121, 109, 118]),
            Some(vec![98, 104, 122, 100, 97, 111, 116]),
            Some(vec![121, 120]),
            Some(vec![118, 110, 99, 109, 122, 103, 98, 100, 111]),
            Some(vec![107, 113, 108, 97, 110, 114, 105, 122, 112, 99]),
            Some(vec![105, 104, 99, 117, 108, 107, 115, 97]),
            Some(vec![108, 114, 109, 106, 103, 99, 100, 99]),
            Some(vec![
                106, 112, 114, 112, 101, 117, 108, 106, 112, 116, 107, 109,
            ]),
            Some(vec![]),
            Some(vec![102, 109, 102]),
            Some(vec![111, 122, 115, 102, 98, 101, 105, 105, 109]),
            Some(vec![105, 104, 101, 117, 100, 110, 103, 99, 113]),
            Some(vec![111, 100, 103]),
            Some(vec![113, 112, 111, 111, 107, 111, 103]),
            Some(vec![111]),
            Some(vec![
                108, 122, 116, 107, 108, 112, 108, 110, 114, 116, 120, 98,
            ]),
        ];
        apply_update(&mut state, &mut expected, 41, groups, values);
    }

    {
        let groups = vec![7, 35, 27, 39, 2, 16, 19, 40, 24, 10, 32, 27];
        let values = vec![
            Some(vec![111, 98, 115, 115, 107, 121, 101, 119]),
            Some(vec![]),
            None,
            Some(vec![98]),
            Some(vec![110, 112, 103, 98, 118, 104, 103, 119, 120]),
            Some(vec![104, 101, 115, 100, 102, 102, 113, 111]),
            Some(vec![97]),
            Some(vec![111, 116, 106, 110, 117, 121, 122, 104, 113, 110]),
            Some(vec![122, 103, 111, 99, 103, 112, 108, 100, 117, 105, 100]),
            Some(vec![108]),
            Some(vec![100, 111, 114, 98, 98, 112, 99, 115, 120, 120]),
            Some(vec![104]),
        ];
        apply_update(&mut state, &mut expected, 41, groups, values);
    }

    {
        let groups = vec![4, 10, 30, 6, 5, 14, 31, 20, 2, 31, 35];
        let values = vec![
            None,
            Some(vec![115, 109, 111, 112]),
            Some(vec![112, 113, 108]),
            Some(vec![113, 116]),
            Some(vec![112, 106]),
            Some(vec![104]),
            Some(vec![106, 115, 122, 113, 107, 111, 101, 112, 108, 122]),
            Some(vec![114, 116, 107, 106, 102, 118, 97, 114, 119, 116]),
            Some(vec![99, 106]),
            Some(vec![107, 98, 100, 109, 115, 114, 114, 104, 103]),
            Some(vec![98, 111, 122, 110, 117, 103, 102, 110, 115, 114, 105]),
        ];
        apply_update(&mut state, &mut expected, 41, groups, values);
    }

    let actual = state.min_max[38].as_ref().map(|buffer| buffer.clone());
    let expected_bytes = expected[38].clone();
    assert_eq!(actual, expected_bytes);
}

#[test]
fn min_updates_across_batches_simple_variants() {
    fn run_scenario(data_type: DataType) {
        let mut state = MinMaxBytesState::new(data_type.clone());
        let total_groups = 10_usize;
        let first_groups = [0_usize, 9, 0, 9];
        let second_groups = first_groups;
        let first_values = ["m0", "t9", "n0", "u9"];
        let second_values = ["a0", "t9", "n0", "u9"];

        let first_batch: Vec<Vec<u8>> = first_values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect();
        state
            .update_batch(
                first_batch.iter().map(|value| Some(value.as_slice())),
                &first_groups,
                total_groups,
                |a, b| a < b,
            )
            .expect("first batch");

        assert!(
            matches!(state.workload_mode, WorkloadMode::Simple),
            "expected Simple for {data_type:?}, found {:?}",
            state.workload_mode
        );
        assert_eq!(
            state.min_max[0].as_deref(),
            Some(first_values[0].as_bytes()),
            "initial minimum should match first batch for {data_type:?}"
        );

        let second_batch: Vec<Vec<u8>> = second_values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect();
        state
            .update_batch(
                second_batch.iter().map(|value| Some(value.as_slice())),
                &second_groups,
                total_groups,
                |a, b| a < b,
            )
            .expect("second batch");

        assert_eq!(
            state.min_max[0].as_deref(),
            Some(second_values[0].as_bytes()),
            "second batch should lower the minimum for {data_type:?}"
        );
    }

    for data_type in [DataType::Utf8, DataType::Binary, DataType::BinaryView] {
        run_scenario(data_type);
    }
}

#[test]
fn min_updates_across_batches_sparse_variants() {
    fn run_scenario(data_type: DataType) {
        let mut state = MinMaxBytesState::new(data_type.clone());
        let total_groups = 1_024_usize;
        let group_indices = [0_usize, 512, 0, 512];
        let first_values = ["m0", "t9", "n0", "u9"];
        let second_values = ["a0", "t9", "n0", "u9"];

        let first_batch: Vec<Vec<u8>> = first_values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect();
        state
            .update_batch(
                first_batch.iter().map(|value| Some(value.as_slice())),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .expect("first batch");

        assert!(
            matches!(state.workload_mode, WorkloadMode::SparseOptimized),
            "expected SparseOptimized for {data_type:?}, found {:?}",
            state.workload_mode
        );
        assert_eq!(
            state.min_max[0].as_deref(),
            Some(first_values[0].as_bytes()),
            "initial minimum should match first batch for {data_type:?}"
        );

        let second_batch: Vec<Vec<u8>> = second_values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect();
        state
            .update_batch(
                second_batch.iter().map(|value| Some(value.as_slice())),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .expect("second batch");

        assert_eq!(
            state.min_max[0].as_deref(),
            Some(second_values[0].as_bytes()),
            "second batch should lower the minimum for {data_type:?}"
        );
    }

    for data_type in [DataType::Utf8, DataType::Binary, DataType::BinaryView] {
        run_scenario(data_type);
    }
}

#[test]
fn min_updates_after_dense_inline_commit() {
    fn run_scenario(data_type: DataType) {
        let mut state = MinMaxBytesState::new(data_type.clone());
        let total_groups = 8_usize;
        let group_indices = [0_usize, 1, 2, 3, 4, 5, 6, 7];
        let initial_values = ["m0", "n1", "o2", "p3", "q4", "r5", "s6", "t7"];
        let initial_batch: Vec<Vec<u8>> = initial_values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect();

        // Drive the accumulator into DenseInline mode and allow it to commit.
        for _ in 0..=DENSE_INLINE_STABILITY_THRESHOLD {
            state
                .update_batch(
                    initial_batch.iter().map(|value| Some(value.as_slice())),
                    &group_indices,
                    total_groups,
                    |a, b| a < b,
                )
                .expect("stable dense batch");
        }

        assert!(
            matches!(state.workload_mode, WorkloadMode::DenseInline),
            "expected DenseInline for {data_type:?}, found {:?}",
            state.workload_mode
        );
        assert!(state.dense_inline_committed);
        assert_eq!(
            state.min_max[0].as_deref(),
            Some(initial_values[0].as_bytes()),
            "initial committed minimum should match the seeded batch for {data_type:?}"
        );

        let updated_values = ["a0", "n1", "o2", "p3", "q4", "r5", "s6", "t7"];
        let updated_batch: Vec<Vec<u8>> = updated_values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect();

        state
            .update_batch(
                updated_batch.iter().map(|value| Some(value.as_slice())),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .expect("dense inline committed batch");

        assert!(state.dense_inline_committed);
        assert_eq!(
            state.min_max[0].as_deref(),
            Some(updated_values[0].as_bytes()),
            "committed dense inline path should accept the new minimum for {data_type:?}"
        );
    }

    for data_type in [DataType::Utf8, DataType::Binary, DataType::BinaryView] {
        run_scenario(data_type);
    }
}

#[test]
fn min_updates_after_dense_inline_reconsideration() {
    fn run_scenario(data_type: DataType) {
        let mut state = MinMaxBytesState::new(data_type.clone());
        let seed_groups: Vec<usize> = (0..8).collect();
        let seed_values: Vec<Vec<u8>> = seed_groups
            .iter()
            .map(|group| format!("seed_{group}").into_bytes())
            .collect();

        // Establish DenseInline mode with a committed state.
        for _ in 0..=DENSE_INLINE_STABILITY_THRESHOLD {
            state
                .update_batch(
                    seed_values.iter().map(|value| Some(value.as_slice())),
                    &seed_groups,
                    seed_groups.len(),
                    |a, b| a < b,
                )
                .expect("seed dense batch");
        }

        assert!(state.dense_inline_committed);

        // Expand the domain substantially and provide a new minimum for group 0.
        let expanded_total = 32_usize;
        let expanded_groups: Vec<usize> = (0..expanded_total).collect();
        let mut expanded_values: Vec<Vec<u8>> = expanded_groups
            .iter()
            .map(|group| format!("expanded_{group}").into_bytes())
            .collect();
        expanded_values[0] = b"a0".to_vec();

        state
            .update_batch(
                expanded_values.iter().map(|value| Some(value.as_slice())),
                &expanded_groups,
                expanded_total,
                |a, b| a < b,
            )
            .expect("expanded dense batch");

        assert!(matches!(state.workload_mode, WorkloadMode::DenseInline));
        assert_eq!(
            state.min_max[0].as_deref(),
            Some(b"a0".as_slice()),
            "reconsidered dense inline path should adopt the new minimum for {data_type:?}"
        );
    }

    for data_type in [DataType::Utf8, DataType::Binary, DataType::BinaryView] {
        run_scenario(data_type);
    }
}

#[test]
fn randomized_minimum_matches_baseline_for_byte_types() {
    struct Lcg(u64);

    impl Lcg {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            self.0
        }
    }

    fn generate_batches(
        rng: &mut Lcg,
        total_groups: usize,
        batches: usize,
    ) -> Vec<(Vec<usize>, Vec<Option<Vec<u8>>>)> {
        (0..batches)
            .map(|_| {
                let rows = (rng.next() % 16 + 1) as usize;
                let mut groups = Vec::with_capacity(rows);
                let mut values = Vec::with_capacity(rows);

                for _ in 0..rows {
                    let group = (rng.next() as usize) % total_groups;
                    groups.push(group);

                    let is_null = rng.next() % 5 == 0;
                    if is_null {
                        values.push(None);
                        continue;
                    }

                    let len = (rng.next() % 5) as usize;
                    let mut value = Vec::with_capacity(len);
                    for _ in 0..len {
                        value.push((rng.next() & 0xFF) as u8);
                    }
                    values.push(Some(value));
                }

                (groups, values)
            })
            .collect()
    }

    fn run_scenario(data_type: DataType) {
        let mut rng = Lcg::new(0x5EED5EED);
        let total_groups = 128_usize;

        for case in 0..512 {
            let mut state = MinMaxBytesState::new(data_type.clone());
            let mut baseline: Vec<Option<Vec<u8>>> = vec![None; total_groups];
            let batches = (rng.next() % 6 + 1) as usize;
            let payloads = generate_batches(&mut rng, total_groups, batches);

            for (batch_index, (groups, values)) in payloads.into_iter().enumerate() {
                let iter = values
                    .iter()
                    .map(|value| value.as_ref().map(|bytes| bytes.as_slice()));
                state
                    .update_batch(iter, &groups, total_groups, |a, b| a < b)
                    .expect("update batch");

                for (group, value) in groups.iter().zip(values.iter()) {
                    if let Some(candidate) = value {
                        match &mut baseline[*group] {
                            Some(existing) => {
                                if candidate < existing {
                                    *existing = candidate.clone();
                                }
                            }
                            slot @ None => {
                                *slot = Some(candidate.clone());
                            }
                        }
                    }
                }

                for (group_index, expected) in baseline.iter().enumerate() {
                    assert_eq!(
                        state.min_max[group_index].as_ref().map(|v| v.as_slice()),
                        expected.as_ref().map(|v| v.as_slice()),
                        "case {case}, batch {batch_index}, group {group_index}, type {data_type:?}"
                    );
                }
            }
        }
    }

    for data_type in [DataType::Utf8, DataType::Binary, DataType::BinaryView] {
        run_scenario(data_type);
    }
}

#[test]
fn dense_batches_use_dense_inline_mode() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    let total_groups = 32_usize;
    // Use sequential + extra pattern to avoid our fast path detection
    // but still exercise DenseInline mode's internal logic
    // Pattern: [0, 1, 2, ..., 30, 31, 0] - sequential plus one duplicate
    let mut groups: Vec<usize> = (0..total_groups).collect();
    groups.push(0); // Add one duplicate to break our fast path check
    let mut raw_values: Vec<Vec<u8>> = (0..total_groups)
        .map(|idx| format!("value_{idx:02}").into_bytes())
        .collect();
    raw_values.push(b"value_00".to_vec()); // Corresponding value for duplicate

    state
        .update_batch(
            raw_values.iter().map(|value| Some(value.as_slice())),
            &groups,
            total_groups,
            |a, b| a < b,
        )
        .expect("update batch");

    assert!(matches!(state.workload_mode, WorkloadMode::DenseInline));
    assert!(!state.scratch_dense_enabled);
    assert_eq!(state.scratch_dense_limit, 0);
    assert!(state.scratch_sparse.is_empty());
    // Marks may be allocated or not depending on when fast path breaks
    assert!(state.dense_inline_marks_ready);
    assert_eq!(state.populated_groups, total_groups);

    // Verify values are correct
    for i in 0..total_groups {
        let expected = format!("value_{i:02}");
        assert_eq!(state.min_max[i].as_deref(), Some(expected.as_bytes()));
    }
}

#[test]
fn dense_inline_commits_after_stable_batches() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    // Use non-sequential indices to avoid fast path
    let group_indices = vec![0_usize, 2, 1];
    let values = ["a", "b", "c"];

    for batch in 0..5 {
        let iter = values.iter().map(|value| Some(value.as_bytes()));
        state
            .update_batch(iter, &group_indices, 3, |a, b| a < b)
            .expect("update batch");

        if batch < DENSE_INLINE_STABILITY_THRESHOLD {
            assert!(!state.dense_inline_committed);
        } else {
            assert!(state.dense_inline_committed);
            assert!(state.dense_inline_marks.is_empty());
        }
    }
}

#[test]
fn dense_inline_reconsiders_after_commit_when_domain_grows() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    // Use a pattern with one extra element to avoid the sequential fast path
    let group_indices = vec![0_usize, 1, 2, 0];
    let values: Vec<&[u8]> =
        vec![b"a".as_ref(), b"b".as_ref(), b"c".as_ref(), b"z".as_ref()];

    for _ in 0..=DENSE_INLINE_STABILITY_THRESHOLD {
        let iter = values.iter().copied().map(Some);
        state
            .update_batch(iter, &group_indices, 3, |a, b| a < b)
            .expect("stable dense batch");
    }

    assert!(state.dense_inline_committed);
    assert_eq!(state.dense_inline_committed_groups, 3);

    // Expand with one more group (breaking sequential pattern)
    let expanded_groups = vec![0_usize, 1, 2, 3, 0];
    let expanded_values = vec![
        Some(b"a".as_ref()),
        Some(b"b".as_ref()),
        Some(b"c".as_ref()),
        Some(b"z".as_ref()),
        Some(b"zz".as_ref()),
    ];

    state
        .update_batch(expanded_values, &expanded_groups, 4, |a, b| a < b)
        .expect("dense batch with new group");

    assert!(matches!(state.workload_mode, WorkloadMode::DenseInline));
    assert!(!state.dense_inline_committed);
    assert_eq!(state.dense_inline_committed_groups, 0);
    assert_eq!(state.lifetime_max_group_index, Some(3));
}

#[test]
fn dense_inline_defers_marks_first_batch() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    // Use a pattern with one extra element to avoid the sequential fast path
    // but maintain sequential core to avoid breaking DenseInline's internal fast path
    let groups = vec![0_usize, 1, 2, 0]; // Sequential + one duplicate
    let values = ["a", "b", "c", "z"]; // Last value won't replace first

    state
        .update_batch(
            values.iter().map(|value| Some(value.as_bytes())),
            &groups,
            3, // total_num_groups=3, not 4
            |a, b| a < b,
        )
        .expect("first batch");

    // After first batch, marks_ready is set but marks may or may not be allocated
    // depending on when the fast path broke
    assert!(state.dense_inline_marks_ready);

    state
        .update_batch(
            values.iter().map(|value| Some(value.as_bytes())),
            &groups,
            3,
            |a, b| a < b,
        )
        .expect("second batch");

    assert!(state.dense_inline_marks_ready);
    // Marks should be sized to total_num_groups, not the input array length
    assert!(state.dense_inline_marks.len() >= 3);
}

#[test]
fn sparse_batch_switches_mode_after_first_update() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    let groups = vec![10_usize, 20_usize];
    let values = [Some("b".as_bytes()), Some("a".as_bytes())];

    state
        .update_batch(values.iter().copied(), &groups, 1_000_000, |a, b| a < b)
        .expect("first batch");

    assert!(matches!(state.workload_mode, WorkloadMode::SparseOptimized));
    assert_eq!(state.min_max[10].as_deref(), Some("b".as_bytes()));
    assert_eq!(state.min_max[20].as_deref(), Some("a".as_bytes()));

    let groups_second = vec![20_usize];
    let values_second = [Some("c".as_bytes())];

    state
        .update_batch(
            values_second.iter().copied(),
            &groups_second,
            1_000_000,
            |a, b| a > b,
        )
        .expect("second batch");

    assert!(matches!(state.workload_mode, WorkloadMode::SparseOptimized));
    assert!(state.scratch_sparse.capacity() >= groups_second.len());
    assert_eq!(state.scratch_dense_limit, 0);
    assert_eq!(state.min_max[20].as_deref(), Some("c".as_bytes()));
}

#[test]
fn sparse_mode_updates_values_from_start() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    state.workload_mode = WorkloadMode::SparseOptimized;

    let groups = vec![1_000_000_usize, 2_000_000_usize];
    let values = [Some("left".as_bytes()), Some("right".as_bytes())];

    state
        .update_batch(values.iter().copied(), &groups, 2_000_001, |a, b| a < b)
        .expect("sparse update");

    assert!(matches!(state.workload_mode, WorkloadMode::SparseOptimized));
    assert_eq!(state.scratch_dense.len(), 0);
    assert_eq!(state.scratch_dense_limit, 0);
    assert!(state.scratch_sparse.capacity() >= groups.len());
    assert_eq!(state.min_max[1_000_000].as_deref(), Some("left".as_bytes()));
    assert_eq!(
        state.min_max[2_000_000].as_deref(),
        Some("right".as_bytes())
    );
}

#[test]
fn sparse_mode_reenables_dense_before_use() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    state.workload_mode = WorkloadMode::SparseOptimized;

    let total_groups = 64_usize;
    state.resize_min_max(total_groups);
    state.set_value(0, b"mango");
    state.set_value(5, b"zebra");

    state.scratch_dense_limit = 6;
    state.scratch_dense_enabled = false;
    state.scratch_dense.clear();

    assert!(state.total_data_bytes > 0);
    assert_eq!(state.scratch_dense.len(), 0);

    let groups = vec![0_usize, 5_usize];
    let values = [b"apple".as_slice(), b"aardvark".as_slice()];

    state
        .update_batch(
            values.iter().copied().map(Some),
            &groups,
            total_groups,
            |a, b| a < b,
        )
        .expect("sparse update without dense scratch");

    assert!(state.scratch_dense_enabled);
    assert!(state.scratch_dense.len() >= state.scratch_dense_limit);
    assert_eq!(state.scratch_dense_limit, 6);
    assert_eq!(state.min_max[0].as_deref(), Some(b"apple".as_slice()));
    assert_eq!(state.min_max[5].as_deref(), Some(b"aardvark".as_slice()));
}

#[test]
fn simple_mode_switches_to_sparse_on_low_density() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);

    state.record_batch_stats(
        BatchStats {
            unique_groups: 32,
            max_group_index: Some(31),
        },
        DENSE_INLINE_MAX_TOTAL_GROUPS,
    );
    assert!(matches!(state.workload_mode, WorkloadMode::Simple));

    state.populated_groups = SPARSE_SWITCH_GROUP_THRESHOLD + 1;
    state.lifetime_max_group_index = Some(SPARSE_SWITCH_GROUP_THRESHOLD * 200);

    state.record_batch_stats(
        BatchStats {
            unique_groups: 1,
            max_group_index: Some(SPARSE_SWITCH_GROUP_THRESHOLD * 200),
        },
        SPARSE_SWITCH_GROUP_THRESHOLD * 200 + 1,
    );

    assert!(matches!(state.workload_mode, WorkloadMode::SparseOptimized));
}

#[test]
fn emit_to_all_resets_populated_groups() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    state.resize_min_max(3);

    state.set_value(0, b"alpha");
    state.set_value(1, b"beta");

    state.workload_mode = WorkloadMode::SparseOptimized;
    state.processed_batches = 3;
    state.total_groups_seen = 5;
    state.lifetime_max_group_index = Some(7);
    state.scratch_dense_enabled = true;
    state.scratch_dense_limit = 128;
    state.scratch_epoch = 42;
    state.scratch_group_ids.push(1);
    state.scratch_dense.push(ScratchEntry {
        epoch: 1,
        location: ScratchLocation::Existing,
    });
    state.scratch_sparse.insert(0, ScratchLocation::Existing);
    state.simple_epoch = 9;
    state.simple_slots.resize_with(3, SimpleSlot::new);
    state.simple_touched_groups.push(2);
    state.dense_inline_marks_ready = true;
    state.dense_inline_marks.push(99);
    state.dense_inline_epoch = 17;
    state.dense_inline_stable_batches = 11;
    state.dense_inline_committed = true;
    state.dense_inline_committed_groups = 3;
    // Note: dense_enable_invocations and dense_sparse_detours are #[cfg(test)] fields
    // and not available in integration tests

    assert_eq!(state.populated_groups, 2);

    let (_capacity, values) = state.emit_to(EmitTo::All);
    assert_eq!(values.len(), 3);
    assert_eq!(values.iter().filter(|value| value.is_some()).count(), 2);
    assert_eq!(state.populated_groups, 0);
    assert!(state.min_max.is_empty());
    assert_eq!(state.total_data_bytes, 0);
    assert!(matches!(state.workload_mode, WorkloadMode::Undecided));
    assert_eq!(state.processed_batches, 0);
    assert_eq!(state.total_groups_seen, 0);
    assert_eq!(state.lifetime_max_group_index, None);
    assert!(!state.scratch_dense_enabled);
    assert_eq!(state.scratch_dense_limit, 0);
    assert_eq!(state.scratch_epoch, 0);
    assert!(state.scratch_group_ids.is_empty());
    assert!(state.scratch_dense.is_empty());
    assert!(state.scratch_sparse.is_empty());
    assert_eq!(state.simple_epoch, 0);
    assert!(state.simple_slots.is_empty());
    assert!(state.simple_touched_groups.is_empty());
    assert!(!state.dense_inline_marks_ready);
    assert!(state.dense_inline_marks.is_empty());
    assert_eq!(state.dense_inline_epoch, 0);
    assert_eq!(state.dense_inline_stable_batches, 0);
    assert!(!state.dense_inline_committed);
    assert_eq!(state.dense_inline_committed_groups, 0);
    // Note: dense_enable_invocations and dense_sparse_detours are #[cfg(test)] fields
    // and not available in integration tests
}

#[test]
fn emit_to_first_updates_populated_groups() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    state.resize_min_max(4);

    state.set_value(0, b"left");
    state.set_value(1, b"middle");
    state.set_value(3, b"right");

    assert_eq!(state.populated_groups, 3);

    let (_capacity, values) = state.emit_to(EmitTo::First(2));
    assert_eq!(values.len(), 2);
    assert_eq!(state.populated_groups, 1);
    assert_eq!(state.min_max.len(), 2);

    // Remaining groups should retain their data (original index 3)
    assert_eq!(state.min_max[1].as_deref(), Some(b"right".as_slice()));
}

#[test]
fn min_updates_after_emit_first_realigns_indices() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    let initial_groups: Vec<usize> = (0..4).collect();
    let initial_values = ["m0", "n1", "o2", "p3"];
    let initial_batch: Vec<Vec<u8>> = initial_values
        .iter()
        .map(|value| value.as_bytes().to_vec())
        .collect();

    state
        .update_batch(
            initial_batch.iter().map(|value| Some(value.as_slice())),
            &initial_groups,
            initial_groups.len(),
            |a, b| a < b,
        )
        .expect("seed batch");

    state.workload_mode = WorkloadMode::SparseOptimized;
    state.scratch_dense_enabled = true;
    state.scratch_dense_limit = initial_groups.len();
    state.scratch_dense = vec![ScratchEntry::new(); initial_groups.len()];
    state.scratch_group_ids = initial_groups.clone();
    state.scratch_epoch = 42;
    state
        .simple_slots
        .resize_with(initial_groups.len(), SimpleSlot::new);
    state.simple_epoch = 7;
    state.simple_touched_groups = initial_groups.clone();
    state.dense_inline_marks = vec![99; initial_groups.len()];
    state.dense_inline_marks_ready = true;
    state.dense_inline_epoch = 9;
    state.dense_inline_stable_batches = 5;
    state.dense_inline_committed = true;
    state.dense_inline_committed_groups = initial_groups.len();
    state.total_groups_seen = 16;
    state.lifetime_max_group_index = Some(initial_groups.len() - 1);

    let (_capacity, emitted) = state.emit_to(EmitTo::First(2));
    assert_eq!(emitted.len(), 2);
    assert_eq!(state.min_max.len(), 2);
    assert_eq!(
        state.min_max[0].as_deref(),
        Some(initial_values[2].as_bytes())
    );
    assert_eq!(state.populated_groups, 2);
    assert_eq!(state.total_groups_seen, state.populated_groups);
    assert_eq!(state.lifetime_max_group_index, Some(1));
    assert!(!state.scratch_dense_enabled);
    assert_eq!(state.scratch_dense_limit, 0);
    assert!(state.scratch_dense.is_empty());
    assert!(state.scratch_group_ids.is_empty());
    assert!(state.scratch_sparse.is_empty());
    assert_eq!(state.scratch_epoch, 0);
    assert_eq!(state.simple_slots.len(), state.min_max.len());
    assert_eq!(state.simple_epoch, 0);
    assert!(state.simple_touched_groups.is_empty());
    assert_eq!(state.dense_inline_marks.len(), state.min_max.len());
    assert!(!state.dense_inline_marks_ready);
    assert_eq!(state.dense_inline_epoch, 0);
    assert_eq!(state.dense_inline_stable_batches, 0);
    assert!(!state.dense_inline_committed);
    assert_eq!(state.dense_inline_committed_groups, 0);
    assert_eq!(state.processed_batches, 0);

    let update_groups = [0_usize];
    let updated_value = b"a0".to_vec();
    state
        .update_batch(
            std::iter::once(Some(updated_value.as_slice())),
            &update_groups,
            state.min_max.len(),
            |a, b| a < b,
        )
        .expect("update after emit");

    assert_eq!(state.min_max[0].as_deref(), Some(updated_value.as_slice()));
}

#[test]
fn emit_to_first_resets_state_when_everything_is_drained() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    state.resize_min_max(2);
    state.set_value(0, b"left");
    state.set_value(1, b"right");

    state.workload_mode = WorkloadMode::DenseInline;
    state.processed_batches = 10;
    state.total_groups_seen = 12;
    state.scratch_dense_enabled = true;
    state.dense_inline_committed = true;
    state.dense_inline_committed_groups = 2;
    state.simple_epoch = 5;
    state.simple_slots.resize_with(2, SimpleSlot::new);

    let (_capacity, values) = state.emit_to(EmitTo::First(2));
    assert_eq!(values.len(), 2);
    assert!(values.iter().all(|value| value.is_some()));
    assert!(state.min_max.is_empty());
    assert_eq!(state.total_data_bytes, 0);
    assert!(matches!(state.workload_mode, WorkloadMode::Undecided));
    assert_eq!(state.processed_batches, 0);
    assert_eq!(state.total_groups_seen, 0);
    assert!(!state.scratch_dense_enabled);
    assert!(!state.dense_inline_committed);
    assert_eq!(state.dense_inline_committed_groups, 0);
    assert_eq!(state.simple_epoch, 0);
    assert!(state.simple_slots.is_empty());
}

#[test]
fn resize_min_max_reclaims_truncated_entries() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    state.resize_min_max(4);
    state.set_value(0, b"a");
    state.set_value(1, b"bc");
    state.set_value(2, b"def");
    state.set_value(3, b"ghij");

    assert_eq!(state.populated_groups, 4);
    assert_eq!(state.total_data_bytes, 10);

    state.resize_min_max(2);
    assert_eq!(state.min_max.len(), 2);
    assert_eq!(state.total_data_bytes, 3);
    assert_eq!(state.populated_groups, 2);
    assert_eq!(state.min_max[0].as_deref(), Some(b"a".as_slice()));
    assert_eq!(state.min_max[1].as_deref(), Some(b"bc".as_slice()));

    state.resize_min_max(0);
    assert_eq!(state.min_max.len(), 0);
    assert_eq!(state.total_data_bytes, 0);
    assert_eq!(state.populated_groups, 0);
}

#[test]
fn sequential_dense_counts_non_null_groups_without_spurious_updates() {
    let total_groups = 6_usize;
    let existing_values: Vec<Vec<u8>> = (0..total_groups)
        .map(|group| format!("seed_{group:02}").into_bytes())
        .collect();
    let group_indices: Vec<usize> = (0..total_groups).collect();

    let owned_replacements: Vec<Option<Vec<u8>>> = vec![
        Some(b"aaa".to_vec()), // smaller -> should replace
        Some(b"zzz".to_vec()), // larger -> should not replace
        None,
        Some(b"seed_03".to_vec()), // equal -> should not replace
        None,
        Some(b"aaa".to_vec()), // smaller -> should replace
    ];

    {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        state.resize_min_max(total_groups);
        for (group, value) in existing_values.iter().enumerate() {
            state.set_value(group, value);
        }

        let stats = state
            .update_batch_sequential_dense(
                owned_replacements.iter().map(|value| value.as_deref()),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .expect("sequential dense update");

        // Only four groups supplied non-null values in the batch.
        assert_eq!(stats.unique_groups, 4);
        assert_eq!(stats.max_group_index, Some(5));

        // Groups 0 and 5 should have been updated with the smaller values.
        assert_eq!(state.min_max[0].as_deref(), Some(b"aaa".as_slice()));
        assert_eq!(state.min_max[5].as_deref(), Some(b"aaa".as_slice()));

        // Groups with larger/equal values must retain their existing minima.
        assert_eq!(state.min_max[1].as_deref(), Some(b"seed_01".as_slice()));
        assert_eq!(state.min_max[3].as_deref(), Some(b"seed_03".as_slice()));

        // Null groups are left untouched.
        assert_eq!(state.min_max[2].as_deref(), Some(b"seed_02".as_slice()));
        assert_eq!(state.min_max[4].as_deref(), Some(b"seed_04".as_slice()));
    }

    let owned_replacements_with_null_tail: Vec<Option<Vec<u8>>> = vec![
        Some(b"aaa".to_vec()), // smaller -> should replace
        Some(b"zzz".to_vec()), // larger -> should not replace
        None,
        Some(b"seed_03".to_vec()), // equal -> should not replace
        None,
        None, // regression: highest group index is null in the batch
    ];

    let mut state = MinMaxBytesState::new(DataType::Utf8);
    state.resize_min_max(total_groups);
    for (group, value) in existing_values.iter().enumerate() {
        state.set_value(group, value);
    }

    let stats = state
        .update_batch_sequential_dense(
            owned_replacements_with_null_tail
                .iter()
                .map(|value| value.as_deref()),
            &group_indices,
            total_groups,
            |a, b| a < b,
        )
        .expect("sequential dense update");

    // Only three groups supplied non-null values in the batch, but the maximum
    // group index should still reflect the last slot in the batch even when
    // that entry is null.
    assert_eq!(stats.unique_groups, 3);
    assert_eq!(stats.max_group_index, Some(5));

    // Only the first group should have been updated with the smaller value.
    assert_eq!(state.min_max[0].as_deref(), Some(b"aaa".as_slice()));

    // All other groups, including the null tail, must retain their original minima.
    assert_eq!(state.min_max[1].as_deref(), Some(b"seed_01".as_slice()));
    assert_eq!(state.min_max[2].as_deref(), Some(b"seed_02".as_slice()));
    assert_eq!(state.min_max[3].as_deref(), Some(b"seed_03".as_slice()));
    assert_eq!(state.min_max[4].as_deref(), Some(b"seed_04".as_slice()));
    assert_eq!(state.min_max[5].as_deref(), Some(b"seed_05".as_slice()));
}

#[test]
fn sequential_dense_reuses_allocation_across_batches() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    let total_groups = 512_usize;
    let group_indices: Vec<usize> = (0..total_groups).collect();

    let make_batch = |prefix: u8| -> Vec<Option<Vec<u8>>> {
        (0..total_groups)
            .map(|group| {
                Some(format!("{ch}{ch}_{group:05}", ch = char::from(prefix)).into_bytes())
            })
            .collect()
    };

    // Seed the accumulator with a batch of lexicographically large values.
    let initial = make_batch(b'z');
    let stats = state
        .update_batch_sequential_dense(
            initial.iter().map(|value| value.as_deref()),
            &group_indices,
            total_groups,
            |a, b| a < b,
        )
        .expect("initial sequential dense update");
    assert_eq!(stats.unique_groups, total_groups);

    let baseline_size = state.size();

    // Process several more batches where each value is strictly smaller than the
    // previous one. All replacements keep the payload length constant so any
    // increase in size would indicate a new allocation.
    for step in 1..=5 {
        let prefix = b'z' - step as u8;
        let batch = make_batch(prefix);
        state
            .update_batch_sequential_dense(
                batch.iter().map(|value| value.as_deref()),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .expect("sequential dense update");

        assert_eq!(state.size(), baseline_size);
    }
}

#[test]
fn sequential_dense_batches_skip_dense_inline_marks_allocation() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    let total_groups = 2_048_usize;
    let batch_size = 1_536_usize; // 75% density keeps DenseInline preferred
    let group_indices: Vec<usize> = (0..batch_size).collect();

    let make_batch = |step: usize| -> Vec<Vec<u8>> {
        group_indices
            .iter()
            .map(|group| format!("{step:02}_{group:05}").into_bytes())
            .collect()
    };

    // First batch should drive the accumulator into DenseInline mode without
    // touching the marks table because the internal fast path stays active.
    let first_batch = make_batch(0);
    state
        .update_batch(
            first_batch.iter().map(|value| Some(value.as_slice())),
            &group_indices,
            total_groups,
            |a, b| a < b,
        )
        .expect("first sequential dense batch");

    assert!(matches!(state.workload_mode, WorkloadMode::DenseInline));
    assert!(state.dense_inline_marks_ready);
    assert!(state.dense_inline_marks.is_empty());
    let initial_epoch = state.dense_inline_epoch;

    // Subsequent sequential batches should continue using the fast path
    // without allocating or clearing the marks table.
    for step in 1..=2 {
        let batch = make_batch(step);
        state
            .update_batch(
                batch.iter().map(|value| Some(value.as_slice())),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .unwrap_or_else(|err| panic!("sequential dense batch {step} failed: {err}"));

        assert!(state.dense_inline_marks.is_empty());
        assert_eq!(state.dense_inline_epoch, initial_epoch);
    }
}

#[test]
fn update_batch_duplicate_batches_match_expected_unique_counts() {
    let mut state = MinMaxBytesState::new(DataType::Utf8);
    let total_groups = 8_usize;
    let repeats_per_group = 4_usize;

    let group_indices: Vec<usize> = (0..total_groups)
        .flat_map(|group| std::iter::repeat_n(group, repeats_per_group))
        .collect();
    let values: Vec<Vec<u8>> = group_indices
        .iter()
        .map(|group| format!("value_{group:02}").into_bytes())
        .collect();

    for batch in 0..3 {
        let before = state.total_groups_seen;
        state
            .update_batch(
                values.iter().map(|value| Some(value.as_slice())),
                &group_indices,
                total_groups,
                |a, b| a < b,
            )
            .expect("update batch");

        assert_eq!(
            state.total_groups_seen,
            before + total_groups,
            "batch {batch} should add exactly {total_groups} unique groups",
        );
    }
}
