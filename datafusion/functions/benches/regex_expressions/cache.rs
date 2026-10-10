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

//! Benchmarks for regexp functions with repeated, alternating and distinct patterns.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::regex::{
    regexpcount::RegexpCountFunc, regexpinstr::RegexpInstrFunc,
    regexplike::RegexpLikeFunc, regexpmatch::RegexpMatchFunc,
    regexpreplace::RegexpReplaceFunc,
};

fn bench_cached_regex(c: &mut Criterion, func: &dyn ScalarUDFImpl) {
    const SIZE: usize = 8192;
    const VALUE: &str = "aaaabbbbccccddddeeeeffffgggghhhh";
    let values = Arc::new(StringArray::from(vec![VALUE; SIZE])) as ArrayRef;
    let mut group = c.benchmark_group(format!("{}_cached", func.name()));
    // Count input string payload, excluding patterns, flags and Arrow metadata.
    group.throughput(Throughput::BytesDecimal((SIZE * VALUE.len()) as u64));
    for data_type in [DataType::Utf8, DataType::Utf8View] {
        let values = cast(&values, &data_type).unwrap();
        // Array patterns exercise pattern reuse within a batch. Scalar patterns
        // already have specialized compile-once paths. Alternating patterns
        // require lookups rather than reuse of the previous row's pattern.
        for mode in [
            "repeated",
            "alternating",
            "distinct",
            "repeated_flags",
            "alternating_flags",
            "distinct_flags",
        ] {
            if mode.ends_with("_flags") && func.name() != "regexp_replace" {
                continue;
            }
            let patterns = StringArray::from(
                (0..SIZE)
                    .map(|i| {
                        if mode.starts_with("repeated") {
                            "(a+)".to_owned()
                        } else if mode.starts_with("distinct") {
                            // Every row compiles a new, equivalent pattern.
                            format!("(a+)(?:){{{i}}}")
                        } else if i % 2 == 0 {
                            "(a+)".to_owned()
                        } else {
                            "(b+)".to_owned()
                        }
                    })
                    .collect::<Vec<_>>(),
            );
            let patterns = cast(&(Arc::new(patterns) as ArrayRef), &data_type).unwrap();
            let mut args = vec![
                ColumnarValue::Array(Arc::clone(&values)),
                ColumnarValue::Array(patterns),
            ];
            if func.name() == "regexp_replace" {
                args.push(ColumnarValue::Scalar(
                    ScalarValue::try_from_string("X".to_owned(), &data_type).unwrap(),
                ));
            }
            if mode.ends_with("_flags") {
                let flags = Arc::new(StringArray::from(vec!["gi"; SIZE])) as ArrayRef;
                args.push(ColumnarValue::Array(cast(&flags, &data_type).unwrap()));
            }
            let arg_types = args.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
            let arg_fields = arg_types
                .iter()
                .enumerate()
                .map(|(i, dt)| Arc::new(Field::new(format!("arg_{i}"), dt.clone(), true)))
                .collect::<Vec<_>>();
            let return_field = Arc::new(Field::new(
                "result",
                func.return_type(&arg_types).unwrap(),
                true,
            ));
            let config = Arc::new(ConfigOptions::default());
            group.bench_with_input(
                BenchmarkId::new(data_type.to_string(), mode),
                &args,
                |b, args| {
                    b.iter(|| {
                        black_box(
                            func.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: arg_fields.clone(),
                                number_rows: SIZE,
                                return_field: Arc::clone(&return_field),
                                config_options: Arc::clone(&config),
                            })
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }
    group.finish();
}

fn bench_regexp_count(c: &mut Criterion) {
    bench_cached_regex(c, &RegexpCountFunc::new());
}

fn bench_regexp_instr(c: &mut Criterion) {
    bench_cached_regex(c, &RegexpInstrFunc::new());
}

fn bench_regexp_like(c: &mut Criterion) {
    bench_cached_regex(c, &RegexpLikeFunc::new());
}

fn bench_regexp_match(c: &mut Criterion) {
    bench_cached_regex(c, &RegexpMatchFunc::new());
}

fn bench_regexp_replace(c: &mut Criterion) {
    bench_cached_regex(c, &RegexpReplaceFunc::new());
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_millis(800));
    targets = bench_regexp_count, bench_regexp_instr, bench_regexp_like,
        bench_regexp_match, bench_regexp_replace
}
