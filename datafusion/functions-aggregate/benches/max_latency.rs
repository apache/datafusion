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

use std::collections::VecDeque;
use std::time::{Instant, Duration};
use datafusion_common::ScalarValue;
use rand::Rng;

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

// === Production Monotonic Deque Implementation ===
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

    pub fn push(&mut self, val: T) {
        self.fifo.push_back(val.clone());
        while self.deque.back().map_or(false, |back_val| *back_val < val) {
            self.deque.pop_back();
        }
        self.deque.push_back(val);
    }

    pub fn pop(&mut self) -> Option<T> {
        if let Some(popped) = self.fifo.pop_front() {
            if self.deque.front().map_or(false, |front_val| *front_val == popped) {
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

fn generate_random_strings(size: usize) -> Vec<String> {
    let mut rng = rand::thread_rng();
    (0..size)
        .map(|_| {
            let len = rng.gen_range(10..40);
            (0..len).map(|_| rng.gen_range(b'a'..=b'z') as char).collect()
        })
        .collect()
}

fn main() {
    let dataset_size = 5000000; // 5M elements
    println!("Generating raw inputs of size {}...", dataset_size);
    let i64_raw = generate_random_i64(dataset_size);
    let str_raw = generate_random_strings(dataset_size);

    let scalar_int_data: Vec<ScalarValue> = i64_raw.iter().map(|&val| ScalarValue::Int64(Some(val))).collect();
    let scalar_utf8_data: Vec<ScalarValue> = str_raw.iter().map(|val| ScalarValue::Utf8(Some(val.clone()))).collect();

    let datasets = vec![
        ("scalar_int64", scalar_int_data),
        ("scalar_utf8_string", scalar_utf8_data),
    ];

    println!("Starting end-to-end latency measurements...");
    for (label, data) in datasets {
        println!("\n==================================================");
        println!("DATATYPE: {}", label);
        println!("==================================================");

        for window_size in [1000, 10000, 100000] {
            println!("  --- Window Size: {} ---", window_size);

            // Two-Stack Max Latency
            {
                let mut q = TwoStackMax::new();
                let mut max_push_time = Duration::ZERO;
                let mut max_pop_time = Duration::ZERO;

                let start_total = Instant::now();
                for (i, val) in data.iter().enumerate() {
                    let t0 = Instant::now();
                    q.push(val.clone());
                    let t_push = t0.elapsed();
                    if t_push > max_push_time {
                        max_push_time = t_push;
                    }

                    if i >= window_size {
                        let t0 = Instant::now();
                        q.pop();
                        let t_pop = t0.elapsed();
                        if t_pop > max_pop_time {
                            max_pop_time = t_pop;
                        }
                    }
                }
                let total_duration = start_total.elapsed();
                println!("    Two-Stack Queue: Total Time = {:?}, Max Push = {:?}, Max Pop = {:?}", total_duration, max_push_time, max_pop_time);
            }

            // Monotonic Deque Max Latency
            {
                let mut q = MonotonicMax::new();
                let mut max_push_time = Duration::ZERO;
                let mut max_pop_time = Duration::ZERO;

                let start_total = Instant::now();
                for (i, val) in data.iter().enumerate() {
                    let t0 = Instant::now();
                    q.push(val.clone());
                    let t_push = t0.elapsed();
                    if t_push > max_push_time {
                        max_push_time = t_push;
                    }

                    if i >= window_size {
                        let t0 = Instant::now();
                        q.pop();
                        let t_pop = t0.elapsed();
                        if t_pop > max_pop_time {
                            max_pop_time = t_pop;
                        }
                    }
                }
                let total_duration = start_total.elapsed();
                println!("    Monotonic Deque: Total Time = {:?}, Max Push = {:?}, Max Pop = {:?}", total_duration, max_push_time, max_pop_time);
            }
        }
    }
}
