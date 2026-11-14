// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks `replace(dictionary, scalar, scalar)` through function argument
//! coercion. This keeps the benchmark unchanged if `replace` learns to preserve
//! dictionary encoding: coercion determines whether each invocation decodes the
//! input or passes the dictionary through.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions::string;

const ROWS: usize = 65_536;

struct ReplaceCase {
    function: Arc<ScalarUDF>,
    args: Vec<ColumnarValue>,
    arg_fields: Vec<FieldRef>,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
}

impl ReplaceCase {
    fn new(dictionary: ArrayRef) -> Self {
        let function = string::replace();
        let args = vec![
            ColumnarValue::Array(dictionary),
            ColumnarValue::Scalar(ScalarValue::from("ab")),
            ColumnarValue::Scalar(ScalarValue::from("XYZ")),
        ];
        let input_fields = args
            .iter()
            .enumerate()
            .map(|(index, arg)| {
                Arc::new(Field::new(format!("arg_{index}"), arg.data_type(), false))
            })
            .collect::<Vec<_>>();
        let arg_fields = fields_with_udf(&input_fields, function.as_ref())
            .expect("replace arguments should coerce");
        let scalar_arguments = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(value) => Some(value),
                ColumnarValue::Array(_) => None,
            })
            .collect::<Vec<_>>();
        let return_field = function
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
            .expect("replace should resolve its return field");

        let case = Self {
            function,
            args,
            arg_fields,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        };
        case.validate();
        case
    }

    fn invoke(&self) -> Result<ColumnarValue> {
        // These are the casts the type-coercion analyzer places around the
        // arguments. Keep them in the measured path: on current main the first
        // cast materializes the dictionary, while an encoding-aware signature
        // can make it a no-op.
        let args = self
            .args
            .iter()
            .zip(&self.arg_fields)
            .map(|(arg, field)| arg.cast_to(field.data_type(), None))
            .collect::<Result<Vec<_>>>()?;

        self.function.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: self.arg_fields.clone(),
            number_rows: ROWS,
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        })
    }

    fn validate(&self) {
        let output = self.invoke().expect("replace should succeed");
        assert_eq!(output.data_type(), self.return_field.data_type().clone());
        let output = output.into_array(ROWS).expect("replace output array");
        let output = cast(&output, &DataType::Utf8).expect("decode replace output");
        let output = output
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 replace output");
        assert_eq!(output.len(), ROWS);
        assert_eq!(output.value(0), "prefix-XYZ-value-00000000");
    }
}

fn dictionary(referenced_values: usize, backing_values: usize) -> ArrayRef {
    assert!(referenced_values <= backing_values);
    let values = Arc::new(StringArray::from_iter_values(
        (0..backing_values).map(|i| format!("prefix-ab-value-{i:08}")),
    ));
    let keys = Int32Array::from_iter_values(
        (0..ROWS).map(|index| (index % referenced_values) as i32),
    );
    Arc::new(
        DictionaryArray::<Int32Type>::try_new(keys, values)
            .expect("valid dictionary array"),
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("replace_dictionary");
    group.throughput(Throughput::Elements(ROWS as u64));

    for (name, referenced_values, backing_values) in [
        ("cardinality_16", 16, 16),
        ("cardinality_256", 256, 256),
        ("cardinality_4096", 4_096, 4_096),
        ("cardinality_65536", 65_536, 65_536),
        ("retained_backing_4096", 16, 4_096),
        ("oversized_backing_262144", 16, 262_144),
    ] {
        let case = ReplaceCase::new(dictionary(referenced_values, backing_values));
        group.bench_with_input(BenchmarkId::from_parameter(name), &case, |b, case| {
            b.iter(|| black_box(case.invoke().expect("replace should succeed")))
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
