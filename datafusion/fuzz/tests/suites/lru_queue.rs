// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Seeded random operation sequences on [`LruQueue`], checked against a
//! [`HashMap`] reference.

use std::collections::HashMap;

use datafusion_common::{Result, exec_err};
use datafusion_execution::cache::lru_queue::LruQueue;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use test_utils::fuzz::FuzzTest;

const OPS_PER_CASE: i32 = 10_000;
const MAX_KEYS: i32 = 1_000;

pub(crate) struct LruQueueFuzzer;

impl FuzzTest for LruQueueFuzzer {
    type Dataset = ();

    fn name(&self) -> &str {
        "lru_queue"
    }

    fn prepare_dataset(&self, _: u64) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }

    fn run_case(&self, _: &(), seed: u64) -> impl Future<Output = Result<()>> {
        std::future::ready(run_ops(seed))
    }
}

fn run_ops(seed: u64) -> Result<()> {
    let mut lru_queue: LruQueue<i32, i32> = LruQueue::new();
    let mut map: HashMap<i32, i32> = HashMap::new();
    let methods = ["get", "put", "remove", "pop", "contains", "len"];
    let mut rng = StdRng::seed_from_u64(seed);

    for i in 0..OPS_PER_CASE {
        let key = i % MAX_KEYS;
        let method = *methods.choose(&mut rng).unwrap();
        let mismatch = match method {
            "get" => {
                let (actual, expected) = (lru_queue.get(&key), map.get(&key));
                (actual != expected).then(|| format!("{actual:?} != {expected:?}"))
            }
            "put" => {
                let (actual, expected) = (lru_queue.put(key, i), map.insert(key, i));
                (actual != expected).then(|| format!("{actual:?} != {expected:?}"))
            }
            "remove" => {
                let (actual, expected) = (lru_queue.remove(&key), map.remove(&key));
                (actual != expected).then(|| format!("{actual:?} != {expected:?}"))
            }
            "pop" => lru_queue.pop().and_then(|(k, v)| {
                let expected = map.remove(&k);
                (Some(v) != expected)
                    .then(|| format!("popped ({k}, {v}), reference has {expected:?}"))
            }),
            "contains" => {
                let (actual, expected) =
                    (lru_queue.contains_key(&key), map.contains_key(&key));
                (actual != expected).then(|| format!("{actual} != {expected}"))
            }
            "len" => {
                let (actual, expected) = (lru_queue.len(), map.len());
                (actual != expected).then(|| format!("{actual} != {expected}"))
            }
            _ => unreachable!(),
        };
        if let Some(mismatch) = mismatch {
            return exec_err!(
                "seed {seed}, op #{i}: {method}({key}) mismatch: {mismatch}"
            );
        }
    }
    Ok(())
}
