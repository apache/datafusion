// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::case_conversion_inputs;
use criterion::{Criterion, criterion_group};
use datafusion_functions::string;

fn criterion_benchmark(c: &mut Criterion) {
    let lower = string::lower();
    for (name, array) in case_conversion_inputs::inputs() {
        case_conversion_inputs::bench_array(c, &lower, &format!("lower_{name}"), &array);
    }
}

criterion_group!(benches, criterion_benchmark);
