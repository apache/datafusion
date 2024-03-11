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

extern crate criterion;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::ThreadRng;
use rand::Rng;

use datafusion_expr::{lit, Expr};
use datafusion_functions::expr_fn::make_date;

fn years(rng: &mut ThreadRng) -> Vec<Expr> {
    let mut years = vec![];
    for _ in 0..1000 {
        years.push(lit(rng.gen_range(1900..2050)));
    }

    years
}

fn months(rng: &mut ThreadRng) -> Vec<Expr> {
    let mut months = vec![];
    for _ in 0..1000 {
        months.push(lit(rng.gen_range(1..13)));
    }

    months
}

fn days(rng: &mut ThreadRng) -> Vec<Expr> {
    let mut days = vec![];
    for _ in 0..1000 {
        days.push(lit(rng.gen_range(1..29)));
    }

    days
}
fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("make_date_col_col_col_1000", |b| {
        let mut rng = rand::thread_rng();
        let years = years(&mut rng);
        let months = months(&mut rng);
        let days = days(&mut rng);

        b.iter(|| {
            years.iter().enumerate().for_each(|(idx, i)| {
                black_box(make_date(
                    i.clone(),
                    months.get(idx).unwrap().clone(),
                    days.get(idx).unwrap().clone(),
                ));
            })
        })
    });

    c.bench_function("make_date_scalar_col_col_1000", |b| {
        let mut rng = rand::thread_rng();
        let year = lit(2025);
        let months = months(&mut rng);
        let days = days(&mut rng);

        b.iter(|| {
            months.iter().enumerate().for_each(|(idx, i)| {
                black_box(make_date(
                    year.clone(),
                    i.clone(),
                    days.get(idx).unwrap().clone(),
                ));
            })
        })
    });

    c.bench_function("make_date_scalar_scalar_col_1000", |b| {
        let mut rng = rand::thread_rng();
        let year = lit(2025);
        let months = lit(11);
        let days = days(&mut rng);

        b.iter(|| {
            days.iter().for_each(|i| {
                black_box(make_date(year.clone(), months.clone(), i.clone()));
            })
        })
    });

    c.bench_function("make_date_scalar_scalar_scalar", |b| {
        let year = lit(2025);
        let month = lit(11);
        let day = lit(26);

        b.iter(|| black_box(make_date(year.clone(), month.clone(), day.clone())))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
