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

use arrow::array::ArrayRef;
use arrow_schema::{
    DECIMAL32_MAX_PRECISION, DECIMAL64_MAX_PRECISION, DECIMAL128_MAX_PRECISION,
    DECIMAL256_MAX_PRECISION,
};
use datafusion_common::ScalarValue;
use rand::random;

use super::record_batch_generator::{RecordBatchGenerator, get_supported_types_columns};

const NUM_SEEDS: usize = 32;

#[test]
fn scalar_value_eq_array_consistency() {
    for_each_array(|context, array| {
        for (index, expected) in extract_scalars(array, context).iter().enumerate() {
            assert!(
                expected
                    .eq_array(array, index)
                    .unwrap_or_else(|e| panic!("eq_array failed for {context}: {e}")),
                "eq_array disagreed with try_from_array at index {index} for {context}"
            );
        }
    });
}

#[test]
fn scalar_value_iter_to_array_roundtrip() {
    for_each_array(|context, array| {
        let expected = extract_scalars(array, context);
        let rebuilt = ScalarValue::iter_to_array(expected.iter().cloned())
            .unwrap_or_else(|e| panic!("iter_to_array failed for {context}: {e}"));

        assert_eq!(
            rebuilt.data_type(),
            array.data_type(),
            "iter_to_array changed the type for {context}"
        );
        assert_eq!(
            rebuilt.len(),
            array.len(),
            "iter_to_array changed the length for {context}"
        );

        let actual = extract_scalars(&rebuilt, context);
        assert_eq!(
            actual, expected,
            "iter_to_array changed values for {context}"
        );
    });
}

#[test]
fn scalar_value_iter_to_array_rejects_mixed_parameterized_types() {
    let mut mutation_counts = [0; MutationKind::COUNT];

    for_each_array(|context, array| {
        if array.len() < 2 {
            return;
        }

        let scalars = extract_scalars(array, context);
        for (kind, mutated) in type_parameter_mutations(&scalars[1]) {
            assert_ne!(mutated.data_type(), scalars[0].data_type());
            let mut mutated_scalars = scalars.clone();
            mutated_scalars[1] = mutated;

            let error = ScalarValue::iter_to_array(mutated_scalars).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("Inconsistent types in ScalarValue::iter_to_array"),
                "iter_to_array returned an unexpected error for {kind:?} mutation of {context}: {error}"
            );
            mutation_counts[kind as usize] += 1;
        }
    });

    for kind in MutationKind::ALL {
        assert!(
            mutation_counts[kind as usize] > 0,
            "no {kind:?} mutations were tested"
        );
    }
}

#[test]
fn scalar_value_to_array_roundtrip() {
    for_each_array(|context, array| {
        for (index, expected) in extract_scalars(array, context).iter().enumerate() {
            let singleton = expected
                .to_array()
                .unwrap_or_else(|e| panic!("to_array failed for {context}: {e}"));
            assert_eq!(
                singleton.len(),
                1,
                "to_array returned the wrong length at index {index} for {context}"
            );

            let actual = ScalarValue::try_from_array(&singleton, 0).unwrap_or_else(|e| {
                panic!(
                    "try_from_array failed on to_array output at index {index} for {context}: {e}"
                )
            });
            assert_eq!(
                &actual, expected,
                "to_array roundtrip changed index {index} for {context}"
            );
        }
    });
}

#[derive(Debug, Clone, Copy)]
enum MutationKind {
    DecimalPrecision,
    DecimalScale,
    TimestampTimezone,
    FixedSizeBinaryWidth,
}

impl MutationKind {
    const ALL: [Self; 4] = [
        Self::DecimalPrecision,
        Self::DecimalScale,
        Self::TimestampTimezone,
        Self::FixedSizeBinaryWidth,
    ];
    const COUNT: usize = Self::ALL.len();
}

fn type_parameter_mutations(scalar: &ScalarValue) -> Vec<(MutationKind, ScalarValue)> {
    use ScalarValue::*;

    macro_rules! decimal_mutations {
        ($CONSTRUCTOR:ident, $VALUE:ident, $PRECISION:ident, $SCALE:ident, $MAX:expr) => {{
            let mut mutations = vec![(
                MutationKind::DecimalScale,
                $CONSTRUCTOR(*$VALUE, *$PRECISION, different_scale(*$SCALE)),
            )];
            if let Some(precision) = different_precision(*$PRECISION, *$SCALE, $MAX) {
                mutations.push((
                    MutationKind::DecimalPrecision,
                    $CONSTRUCTOR(*$VALUE, precision, *$SCALE),
                ));
            }
            mutations
        }};
    }

    match scalar {
        Decimal32(value, precision, scale) => decimal_mutations!(
            Decimal32,
            value,
            precision,
            scale,
            DECIMAL32_MAX_PRECISION
        ),
        Decimal64(value, precision, scale) => decimal_mutations!(
            Decimal64,
            value,
            precision,
            scale,
            DECIMAL64_MAX_PRECISION
        ),
        Decimal128(value, precision, scale) => decimal_mutations!(
            Decimal128,
            value,
            precision,
            scale,
            DECIMAL128_MAX_PRECISION
        ),
        Decimal256(value, precision, scale) => decimal_mutations!(
            Decimal256,
            value,
            precision,
            scale,
            DECIMAL256_MAX_PRECISION
        ),
        TimestampSecond(value, timezone) => vec![(
            MutationKind::TimestampTimezone,
            TimestampSecond(*value, toggled_timezone(timezone.as_ref())),
        )],
        TimestampMillisecond(value, timezone) => vec![(
            MutationKind::TimestampTimezone,
            TimestampMillisecond(*value, toggled_timezone(timezone.as_ref())),
        )],
        TimestampMicrosecond(value, timezone) => vec![(
            MutationKind::TimestampTimezone,
            TimestampMicrosecond(*value, toggled_timezone(timezone.as_ref())),
        )],
        TimestampNanosecond(value, timezone) => vec![(
            MutationKind::TimestampTimezone,
            TimestampNanosecond(*value, toggled_timezone(timezone.as_ref())),
        )],
        FixedSizeBinary(width, _) => vec![(
            MutationKind::FixedSizeBinaryWidth,
            FixedSizeBinary(width + 1, None),
        )],
        _ => vec![],
    }
}

fn different_precision(precision: u8, scale: i8, max_precision: u8) -> Option<u8> {
    if precision < max_precision {
        Some(precision + 1)
    } else if precision > 1 && scale <= (precision - 1) as i8 {
        Some(precision - 1)
    } else {
        None
    }
}

fn different_scale(scale: i8) -> i8 {
    if scale == i8::MIN {
        scale + 1
    } else {
        scale - 1
    }
}

fn toggled_timezone(
    timezone: Option<&std::sync::Arc<str>>,
) -> Option<std::sync::Arc<str>> {
    if timezone.is_some() {
        None
    } else {
        Some(std::sync::Arc::from("UTC"))
    }
}

fn for_each_array(mut check: impl FnMut(&str, &ArrayRef)) {
    for _ in 0..NUM_SEEDS {
        let seed = random();
        let columns = get_supported_types_columns(seed);
        let batch = RecordBatchGenerator::new(1, 128, columns)
            .with_seed(seed)
            .generate()
            .unwrap_or_else(|e| panic!("failed to generate batch for seed {seed}: {e}"));

        for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
            let mut check_array = |array: &ArrayRef| {
                let context = format!(
                    "seed={seed}, column={}, type={}, len={}, offset={}",
                    field.name(),
                    array.data_type(),
                    array.len(),
                    array.offset()
                );
                check(&context, array);
            };

            check_array(array);

            // Also exercise arrays with non-zero offsets.
            if array.len() > 2 {
                check_array(&array.slice(1, array.len() - 2));
            }
        }
    }
}

fn extract_scalars(array: &ArrayRef, context: &str) -> Vec<ScalarValue> {
    (0..array.len())
        .map(|index| {
            ScalarValue::try_from_array(array, index).unwrap_or_else(|e| {
                panic!("try_from_array failed at index {index} for {context}: {e}")
            })
        })
        .collect()
}
