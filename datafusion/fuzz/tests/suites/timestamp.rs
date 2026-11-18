// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Adding and subtracting an interval must round-trip at every timestamp precision.

use std::future::Future;

use arrow::datatypes::TimeUnit;
use datafusion_common::{Result, ScalarValue};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_utils::fuzz::FuzzTest;

pub(crate) struct TimestampArithmeticFuzzer;

impl FuzzTest for TimestampArithmeticFuzzer {
    type Dataset = ();

    fn name(&self) -> &str {
        "timestamp_arithmetic"
    }

    fn prepare_dataset(&self, _seed: u64) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }

    /// Generates random timestamps and intervals, then asserts that adding and
    /// subtracting the same interval (in either order) leaves the timestamp unchanged.
    fn run_case(&self, _dataset: &(), seed: u64) -> impl Future<Output = Result<()>> {
        let mut rng = StdRng::seed_from_u64(seed);
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            // 1. Generate a random timestamp and interval at this precision.
            let timestamp = random_timestamp(&mut rng, unit);
            let interval = random_interval(&mut rng, unit);

            // 2. Assert that add/sub and sub/add both leave the timestamp unchanged.
            assert_eq!(
                timestamp.add(&interval).unwrap().sub(&interval).unwrap(),
                timestamp,
                "add then subtract {interval:?}"
            );
            assert_eq!(
                timestamp.sub(&interval).unwrap().add(&interval).unwrap(),
                timestamp,
                "subtract then add {interval:?}"
            );
        }
        std::future::ready(Ok(()))
    }
}

fn random_timestamp(rng: &mut StdRng, unit: TimeUnit) -> ScalarValue {
    // 1995-01-01..2051-01-01 leaves room for interval arithmetic at every precision.
    let nanos = rng.random_range(788_918_400_000_000_000..2_556_144_000_000_000_000);
    match unit {
        TimeUnit::Second => {
            ScalarValue::TimestampSecond(Some(nanos / 1_000_000_000), None)
        }
        TimeUnit::Millisecond => {
            ScalarValue::TimestampMillisecond(Some(nanos / 1_000_000), None)
        }
        TimeUnit::Microsecond => {
            ScalarValue::TimestampMicrosecond(Some(nanos / 1_000), None)
        }
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(nanos), None),
    }
}

fn random_interval(rng: &mut StdRng, unit: TimeUnit) -> ScalarValue {
    let days = rng.random_range(0..5000);
    // Avoid months and fractions finer than the timestamp so arithmetic is reversible.
    match unit {
        TimeUnit::Second => {
            ScalarValue::new_interval_dt(days, rng.random_range(0..86_400) * 1_000)
        }
        TimeUnit::Millisecond => {
            ScalarValue::new_interval_dt(days, rng.random_range(0..86_400_000))
        }
        TimeUnit::Microsecond => ScalarValue::new_interval_mdn(
            0,
            days,
            rng.random_range(0..86_400_000_000) * 1_000,
        ),
        TimeUnit::Nanosecond => ScalarValue::new_interval_mdn(
            0,
            days,
            rng.random_range(0..86_400_000_000_000),
        ),
    }
}
