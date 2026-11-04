// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::scalar::ScalarValue;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("to_array_of_size 100000", |b| {
        let scalar = ScalarValue::Int32(Some(100));

        b.iter(|| {
            assert_eq!(
                scalar
                    .to_array_of_size(100000)
                    .expect("Failed to convert to array of size")
                    .null_count(),
                0
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
