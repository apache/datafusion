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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray};
use arrow::compute::cast;
use arrow::datatypes::{Field, Int32Type};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};

const NUM_ROWS: usize = 8_192;
const DICTIONARY_CARDINALITIES: [usize; 4] = [10, 100, 1_000, 8_192];

fn create_string_dictionary(cardinality: usize) -> ArrayRef {
    let values = (0..NUM_ROWS)
        .map(|index| Some(format!("value_{:04}", index % cardinality)))
        .collect::<Vec<_>>();
    Arc::new(
        values
            .iter()
            .map(|value| value.as_deref())
            .collect::<DictionaryArray<Int32Type>>(),
    )
}

fn benchmark_dictionary_string_udfs(c: &mut Criterion) {
    let udfs: [(&str, Arc<ScalarUDF>); 3] = [
        ("ascii", datafusion_functions::string::ascii()),
        ("bit_length", datafusion_functions::string::bit_length()),
        ("octet_length", datafusion_functions::string::octet_length()),
    ];
    let config_options = Arc::new(ConfigOptions::default());

    for cardinality in DICTIONARY_CARDINALITIES {
        let dictionary = create_string_dictionary(cardinality);
        let mut group = c.benchmark_group(format!(
            "dictionary_encoding/string/cardinality_{cardinality}"
        ));
        for (name, udf) in &udfs {
            let input_field =
                Field::new("a", dictionary.data_type().clone(), false).into();
            let coerced_field = fields_with_udf(&[input_field], udf.as_ref())
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            let coerced_type = coerced_field.data_type();
            let return_type =
                udf.return_type(std::slice::from_ref(coerced_type)).unwrap();
            let return_field = Field::new("f", return_type, false).into();
            let input = if dictionary.data_type() == coerced_type {
                Arc::clone(&dictionary)
            } else {
                cast(dictionary.as_ref(), coerced_type).unwrap()
            };

            group.bench_function(*name, |b| {
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: vec![ColumnarValue::Array(Arc::clone(&input))],
                            arg_fields: vec![Arc::clone(&coerced_field)],
                            number_rows: NUM_ROWS,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                    )
                })
            });
        }
        group.finish();
    }
}

criterion_group!(benches, benchmark_dictionary_string_udfs);
criterion_main!(benches);
