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

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use rand::Rng;
use std::collections::VecDeque;

// === Old Two-Stack Queue Implementation ===
#[derive(Debug)]
pub struct TwoStackMax<T> {
    push_stack: Vec<(T, T)>,
    pop_stack: Vec<(T, T)>,
}

impl<T: Clone + PartialOrd> TwoStackMax<T> {
    pub fn new() -> Self {
        Self {
            push_stack: Vec::new(),
            pop_stack: Vec::new(),
        }
    }

    pub fn max(&self) -> Option<&T> {
        match (self.push_stack.last(), self.pop_stack.last()) {
            (None, None) => None,
            (Some((_, max)), None) => Some(max),
            (None, Some((_, max))) => Some(max),
            (Some((_, a)), Some((_, b))) => Some(if a > b { a } else { b }),
        }
    }

    pub fn push(&mut self, val: T) {
        self.push_stack.push(match self.push_stack.last() {
            Some((_, max)) => {
                if val < *max {
                    (val, max.clone())
                } else {
                    (val.clone(), val)
                }
            }
            None => (val.clone(), val),
        });
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.pop_stack.is_empty() {
            match self.push_stack.pop() {
                Some((val, _)) => {
                    let mut last = (val.clone(), val);
                    self.pop_stack.push(last.clone());
                    while let Some((val, _)) = self.push_stack.pop() {
                        let max = if last.1 > val {
                            last.1.clone()
                        } else {
                            val.clone()
                        };
                        last = (val.clone(), max);
                        self.pop_stack.push(last.clone());
                    }
                }
                None => return None,
            }
        }
        self.pop_stack.pop().map(|(val, _)| val)
    }
}

// === Production Monotonic Deque Implementation (storing T directly) ===
#[derive(Debug)]
pub struct MonotonicMax<T> {
    fifo: VecDeque<T>,
    deque: VecDeque<T>,
}

impl<T: Clone + PartialOrd> MonotonicMax<T> {
    pub fn new() -> Self {
        Self {
            fifo: VecDeque::new(),
            deque: VecDeque::new(),
        }
    }

    pub fn max(&self) -> Option<&T> {
        self.deque.front()
    }

    pub fn push(&mut self, val: T) {
        self.fifo.push_back(val.clone());
        while self.deque.back().map_or(false, |back_val| *back_val < val) {
            self.deque.pop_back();
        }
        self.deque.push_back(val);
    }

    pub fn pop(&mut self) -> Option<T> {
        if let Some(popped) = self.fifo.pop_front() {
            if self
                .deque
                .front()
                .map_or(false, |front_val| *front_val == popped)
            {
                self.deque.pop_front();
            }
            Some(popped)
        } else {
            None
        }
    }
}

// === Generator Utilities ===
fn generate_random_i64(size: usize) -> Vec<i64> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen_range(0..1000000)).collect()
}

fn generate_random_f64(size: usize) -> Vec<f64> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.r#gen()).collect()
}

fn generate_random_strings(size: usize) -> Vec<String> {
    let mut rng = rand::thread_rng();
    (0..size)
        .map(|_| {
            let len = rng.gen_range(10..40);
            (0..len)
                .map(|_| rng.gen_range(b'a'..=b'z') as char)
                .collect()
        })
        .collect()
}

fn bench_sliding_max(c: &mut Criterion) {
    let data_size = 50000;

    // Generate raw inputs
    let i64_raw = generate_random_i64(data_size);
    let f64_raw = generate_random_f64(data_size);
    let str_raw = generate_random_strings(data_size);

    // Map to ScalarValue types
    let scalar_int_data: Vec<ScalarValue> = i64_raw
        .iter()
        .map(|&val| ScalarValue::Int64(Some(val)))
        .collect();
    let scalar_float_data: Vec<ScalarValue> = f64_raw
        .iter()
        .map(|&val| ScalarValue::Float64(Some(val)))
        .collect();
    let scalar_timestamp_data: Vec<ScalarValue> = i64_raw
        .iter()
        .map(|&val| ScalarValue::TimestampNanosecond(Some(val), None))
        .collect();
    let scalar_decimal_data: Vec<ScalarValue> = i64_raw
        .iter()
        .map(|&val| ScalarValue::Decimal128(Some(val as i128), 38, 10))
        .collect();
    let scalar_utf8_data: Vec<ScalarValue> = str_raw
        .iter()
        .map(|val| ScalarValue::Utf8(Some(val.clone())))
        .collect();

    let datasets = vec![
        ("scalar_int64", scalar_int_data),
        ("scalar_float64", scalar_float_data),
        ("scalar_timestamp", scalar_timestamp_data),
        ("scalar_decimal128", scalar_decimal_data),
        ("scalar_utf8_string", scalar_utf8_data),
    ];

    for (label, data) in datasets {
        let mut group = c.benchmark_group(format!("sliding_window_max_{}", label));

        for window_size in [100, 1000, 5000] {
            group.throughput(Throughput::Elements(data_size as u64));

            group.bench_with_input(
                BenchmarkId::new("two_stack_queue", window_size),
                &window_size,
                |b, &w| {
                    b.iter(|| {
                        let mut q = TwoStackMax::new();
                        for (i, val) in data.iter().enumerate() {
                            q.push(val.clone());
                            if i >= w {
                                q.pop();
                            }
                            let _res = q.max();
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("monotonic_deque", window_size),
                &window_size,
                |b, &w| {
                    b.iter(|| {
                        let mut q = MonotonicMax::new();
                        for (i, val) in data.iter().enumerate() {
                            q.push(val.clone());
                            if i >= w {
                                q.pop();
                            }
                            let _res = q.max();
                        }
                    });
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_sliding_max);
criterion_main!(benches);
