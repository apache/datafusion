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

use datafusion_expr::lit;
use datafusion_functions::expr_fn::to_timestamp;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("to_timestamp_no_formats", |b| {
        let inputs = vec![
            lit("1997-01-31T09:26:56.123Z"),
            lit("1997-01-31T09:26:56.123-05:00"),
            lit("1997-01-31 09:26:56.123-05:00"),
            lit("2023-01-01 04:05:06.789 -08"),
            lit("1997-01-31T09:26:56.123"),
            lit("1997-01-31 09:26:56.123"),
            lit("1997-01-31 09:26:56"),
            lit("1997-01-31 13:26:56"),
            lit("1997-01-31 13:26:56+04:00"),
            lit("1997-01-31"),
        ];
        b.iter(|| {
            for i in inputs.iter() {
                black_box(to_timestamp(vec![i.clone()]));
            }
        });
    });

    c.bench_function("to_timestamp_with_formats", |b| {
        let mut inputs = vec![];
        let mut format1 = vec![];
        let mut format2 = vec![];
        let mut format3 = vec![];

        inputs.push(lit("1997-01-31T09:26:56.123Z"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%dT%H:%M:%S%.f%Z"));

        inputs.push(lit("1997-01-31T09:26:56.123-05:00"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%dT%H:%M:%S%.f%z"));

        inputs.push(lit("1997-01-31 09:26:56.123-05:00"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%d %H:%M:%S%.f%Z"));

        inputs.push(lit("2023-01-01 04:05:06.789 -08"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%d %H:%M:%S%.f %#z"));

        inputs.push(lit("1997-01-31T09:26:56.123"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%dT%H:%M:%S%.f"));

        inputs.push(lit("1997-01-31 09:26:56.123"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%d %H:%M:%S%.f"));

        inputs.push(lit("1997-01-31 09:26:56"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%d %H:%M:%S"));

        inputs.push(lit("1997-01-31 092656"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%d %H%M%S"));

        inputs.push(lit("1997-01-31 092656+04:00"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%d %H%M%S%:z"));

        inputs.push(lit("Sun Jul  8 00:34:60 2001"));
        format1.push(lit("%+"));
        format2.push(lit("%c"));
        format3.push(lit("%Y-%m-%d 00:00:00"));

        b.iter(|| {
            inputs.iter().enumerate().for_each(|(idx, i)| {
                black_box(to_timestamp(vec![
                    i.clone(),
                    format1.get(idx).unwrap().clone(),
                    format2.get(idx).unwrap().clone(),
                    format3.get(idx).unwrap().clone(),
                ]));
            })
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
