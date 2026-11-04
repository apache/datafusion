// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks for functions behind the `math_expressions` feature.
//!
//! Each benchmark beside this file is a module rather than its own `[[bench]]`
//! target to reduce compile times and output size.
//!
//! Run one of them with `cargo bench --bench math_expressions -- <filter>`.

use criterion::criterion_main;

mod atan2;
mod cot;
mod factorial;
mod floor_ceil;
mod gcd;
mod isnan;
mod iszero;
mod lcm;
mod nanvl;
mod power;
mod random;
mod round;
mod round_dense;
mod signum;
mod trunc;
mod trunc_precision;
mod unary_math;

criterion_main!(
    atan2::benches,
    cot::benches,
    factorial::benches,
    floor_ceil::benches,
    gcd::benches,
    isnan::benches,
    iszero::benches,
    lcm::benches,
    nanvl::benches,
    power::benches,
    random::benches,
    round::benches,
    round_dense::benches,
    signum::benches,
    trunc::benches,
    trunc_precision::benches,
    unary_math::benches,
);
