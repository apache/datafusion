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

//! Focused benchmarks for `InList` cases.
//!
//! This benchmark file adds targeted coverage for representative `IN LIST`
//! workloads with controlled parameters:
//!
//! - **Controlled match rates**: Exercises both hit-heavy and miss-heavy paths
//! - **List size scaling**: Measures behavior across small and large `IN` lists
//! - **Type coverage**: Covers primitive, string, string-view, dictionary, and
//!   fixed-size-binary inputs
//! - **Shared-prefix strings**: Adds collision-heavy string cases where values
//!   only differ late in the string
//! - **Mixed-length strings**: Covers inputs that combine short and long values
//! - **Null handling**: Includes representative `NULL` and `NOT IN` cases
//!
//! # Case Coverage
//!
//! | Case | Types | Characteristics | List Sizes Tested |
//! |------|-------|-----------------|-------------------|
//! | Narrow integer cases | UInt8 | small value domain | 4, 16 |
//! | Narrow integer cases | Int16 | larger value domain | 4, 64, 256 |
//! | 32-bit primitive cases | Int32, Float32 | small and large lists | 4, 32, 64, 256 |
//! | 64-bit primitive cases | Int64, TimestampNs | small and large lists | 4, 16, 32, 128 |
//! | Utf8 short-string cases | Utf8 | 8-byte strings | 4, 64, 256 |
//! | Utf8 long-string cases | Utf8 | 24-byte strings | 4, 64, 256 |
//! | Utf8View short-string cases | Utf8View | 8-byte strings | 4, 16, 64, 256 |
//! | Utf8View length-12 cases | Utf8View | 12-byte strings | 16, 64 |
//! | Utf8View long-string cases | Utf8View | 24-byte strings | 4, 16, 64, 256 |
//! | Shared-prefix string cases | Utf8, Utf8View | same prefix, different suffix | 16, 32, 64 |
//! | Fixed-size binary cases | FixedSizeBinary(16) | fixed-width binary values | 4, 64, 256, 10000 |

use arrow::array::*;
use arrow::datatypes::{Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::{col, in_list, lit};
use rand::distr::Alphanumeric;
use rand::prelude::*;
use std::sync::Arc;

const ARRAY_SIZE: usize = 8192;

/// Match rates to test both code paths (miss-heavy and balanced)
const MATCH_RATES: [u32; 2] = [0, 50];

// =============================================================================
// NUMERIC BENCHMARK HELPERS
// =============================================================================

/// Configuration for numeric benchmarks, grouping test parameters.
struct NumericBenchConfig<T> {
    list_size: usize,
    match_rate: f64,
    null_rate: f64,
    make_value: fn(&mut StdRng) -> T,
    to_scalar: fn(T) -> ScalarValue,
    negated: bool,
}

impl<T> NumericBenchConfig<T> {
    fn new(
        list_size: usize,
        match_rate: f64,
        make_value: fn(&mut StdRng) -> T,
        to_scalar: fn(T) -> ScalarValue,
    ) -> Self {
        Self {
            list_size,
            match_rate,
            null_rate: 0.0,
            make_value,
            to_scalar,
            negated: false,
        }
    }

    fn with_null_rate(mut self, null_rate: f64) -> Self {
        self.null_rate = null_rate;
        self
    }

    fn with_negated(mut self) -> Self {
        self.negated = true;
        self
    }
}

/// Creates and runs a benchmark for numeric types with controlled match rate.
/// Uses a seed derived from list_size to avoid subset correlation between sizes.
fn bench_numeric<T, A>(
    c: &mut Criterion,
    group: &str,
    name: &str,
    cfg: &NumericBenchConfig<T>,
) where
    T: Clone,
    A: Array + FromIterator<Option<T>> + 'static,
{
    // Use different seed per list_size to avoid subset correlation
    let seed = 0xDEAD_BEEF_u64.wrapping_add(cfg.list_size as u64 * 0x1234_5678);
    let mut rng = StdRng::seed_from_u64(seed);

    // Generate IN list values
    let haystack: Vec<T> = (0..cfg.list_size)
        .map(|_| (cfg.make_value)(&mut rng))
        .collect();

    // Generate array with controlled match rate and null rate
    let values: A = (0..ARRAY_SIZE)
        .map(|_| {
            if cfg.null_rate > 0.0 && rng.random_bool(cfg.null_rate) {
                None
            } else if !haystack.is_empty() && rng.random_bool(cfg.match_rate) {
                Some(haystack.choose(&mut rng).unwrap().clone())
            } else {
                Some((cfg.make_value)(&mut rng))
            }
        })
        .collect();

    let schema = Schema::new(vec![Field::new("a", values.data_type().clone(), true)]);
    let exprs: Vec<_> = haystack
        .iter()
        .map(|v: &T| lit((cfg.to_scalar)(v.clone())))
        .collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &cfg.negated, &schema).unwrap();
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values) as ArrayRef])
            .unwrap();

    c.bench_with_input(BenchmarkId::new(group, name), &batch, |b, batch| {
        b.iter(|| expr.evaluate(batch).unwrap())
    });
}

// =============================================================================
// STRING BENCHMARK HELPERS
// =============================================================================

fn random_string(rng: &mut StdRng, len: usize) -> String {
    String::from_utf8(rng.sample_iter(&Alphanumeric).take(len).collect()).unwrap()
}

/// Creates a set of strings that share a common prefix but differ in suffix.
/// Uses random alphanumeric suffix to avoid bench-maxing on numeric patterns.
fn strings_with_shared_prefix(
    rng: &mut StdRng,
    count: usize,
    prefix_len: usize,
) -> Vec<String> {
    let prefix = random_string(rng, prefix_len);
    (0..count)
        .map(|_| format!("{}{}", prefix, random_string(rng, 8))) // prefix + random 8-char suffix
        .collect()
}

/// Configuration for string benchmarks, grouping test parameters.
struct StringBenchConfig {
    list_size: usize,
    match_rate: f64,
    null_rate: f64,
    string_len: usize,
    to_scalar: fn(String) -> ScalarValue,
    negated: bool,
}

impl StringBenchConfig {
    fn new(
        list_size: usize,
        match_rate: f64,
        string_len: usize,
        to_scalar: fn(String) -> ScalarValue,
    ) -> Self {
        Self {
            list_size,
            match_rate,
            null_rate: 0.0,
            string_len,
            to_scalar,
            negated: false,
        }
    }

    fn with_null_rate(mut self, null_rate: f64) -> Self {
        self.null_rate = null_rate;
        self
    }

    fn with_negated(mut self) -> Self {
        self.negated = true;
        self
    }
}

/// Creates and runs a benchmark for string types with controlled match rate.
/// Uses a seed derived from list_size and string_len to avoid correlation.
fn bench_string<A>(c: &mut Criterion, group: &str, name: &str, cfg: &StringBenchConfig)
where
    A: Array + FromIterator<Option<String>> + 'static,
{
    // Use different seed per (list_size, string_len) to avoid correlation
    let seed = 0xCAFE_BABE_u64
        .wrapping_add(cfg.list_size as u64 * 0x1111)
        .wrapping_add(cfg.string_len as u64 * 0x2222);
    let mut rng = StdRng::seed_from_u64(seed);

    // Generate IN list values
    let haystack: Vec<String> = (0..cfg.list_size)
        .map(|_| random_string(&mut rng, cfg.string_len))
        .collect();

    // Generate array with controlled match rate and null rate
    let values: A = (0..ARRAY_SIZE)
        .map(|_| {
            if cfg.null_rate > 0.0 && rng.random_bool(cfg.null_rate) {
                None
            } else if !haystack.is_empty() && rng.random_bool(cfg.match_rate) {
                Some(haystack.choose(&mut rng).unwrap().clone())
            } else {
                Some(random_string(&mut rng, cfg.string_len))
            }
        })
        .collect();

    let schema = Schema::new(vec![Field::new("a", values.data_type().clone(), true)]);
    let exprs: Vec<_> = haystack
        .iter()
        .map(|v| lit((cfg.to_scalar)(v.clone())))
        .collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &cfg.negated, &schema).unwrap();
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values) as ArrayRef])
            .unwrap();

    c.bench_with_input(BenchmarkId::new(group, name), &batch, |b, batch| {
        b.iter(|| expr.evaluate(batch).unwrap())
    });
}

/// Benchmarks strings with shared prefixes and different suffixes.
/// Uses variable prefix lengths and random suffixes to avoid bench-maxing.
fn bench_string_shared_prefix<A>(
    c: &mut Criterion,
    group: &str,
    name: &str,
    list_size: usize,
    match_rate: f64,
    prefix_len: usize,
    to_scalar: fn(String) -> ScalarValue,
) where
    A: Array + FromIterator<Option<String>> + 'static,
{
    let seed = 0xFEED_FACE_u64
        .wrapping_add(list_size as u64 * 0x3333)
        .wrapping_add(prefix_len as u64 * 0x4444);
    let mut rng = StdRng::seed_from_u64(seed);

    // Generate IN list with a shared prefix.
    let haystack = strings_with_shared_prefix(&mut rng, list_size, prefix_len);

    // Generate non-matching strings with the same prefix to keep misses close
    // to the matching set.
    let non_match_pool = strings_with_shared_prefix(&mut rng, 100, prefix_len);

    // Generate array with controlled match rate
    let values: A = (0..ARRAY_SIZE)
        .map(|_| {
            Some(if !haystack.is_empty() && rng.random_bool(match_rate) {
                haystack.choose(&mut rng).unwrap().clone()
            } else {
                non_match_pool.choose(&mut rng).unwrap().clone()
            })
        })
        .collect();

    let schema = Schema::new(vec![Field::new("a", values.data_type().clone(), true)]);
    let exprs: Vec<_> = haystack.iter().map(|v| lit(to_scalar(v.clone()))).collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &false, &schema).unwrap();
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values) as ArrayRef])
            .unwrap();

    c.bench_with_input(BenchmarkId::new(group, name), &batch, |b, batch| {
        b.iter(|| expr.evaluate(batch).unwrap())
    });
}

/// Benchmarks mixed-length strings (some short <= 12, some long > 12).
/// Uses a more realistic length distribution than the fixed-width cases.
fn bench_string_mixed_lengths<A>(
    c: &mut Criterion,
    group: &str,
    name: &str,
    list_size: usize,
    match_rate: f64,
    to_scalar: fn(String) -> ScalarValue,
) where
    A: Array + FromIterator<Option<String>> + 'static,
{
    let seed = 0xABCD_EF01_u64.wrapping_add(list_size as u64 * 0x5555);
    let mut rng = StdRng::seed_from_u64(seed);

    // Mixed lengths: some short (<= 12), some long (> 12)
    let lengths = [4, 8, 12, 16, 20, 24];

    // Generate IN list with mixed lengths
    let haystack: Vec<String> = (0..list_size)
        .map(|_| {
            let len = *lengths.choose(&mut rng).unwrap();
            random_string(&mut rng, len)
        })
        .collect();

    // Generate array with controlled match rate and mixed lengths
    let values: A = (0..ARRAY_SIZE)
        .map(|_| {
            Some(if !haystack.is_empty() && rng.random_bool(match_rate) {
                haystack.choose(&mut rng).unwrap().clone()
            } else {
                let len = *lengths.choose(&mut rng).unwrap();
                random_string(&mut rng, len)
            })
        })
        .collect();

    let schema = Schema::new(vec![Field::new("a", values.data_type().clone(), true)]);
    let exprs: Vec<_> = haystack.iter().map(|v| lit(to_scalar(v.clone()))).collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &false, &schema).unwrap();
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(values) as ArrayRef])
            .unwrap();

    c.bench_with_input(BenchmarkId::new(group, name), &batch, |b, batch| {
        b.iter(|| expr.evaluate(batch).unwrap())
    });
}

// =============================================================================
// NARROW INTEGER CASE BENCHMARKS
// =============================================================================

fn bench_narrow_integer(c: &mut Criterion) {
    // UInt8: small value domain
    // NOTE: With 256 possible values, list_size=16 covers 6.25% of value space,
    // so even "match=0%" has ~6% accidental matches from random data.
    for list_size in [4, 16] {
        for match_pct in MATCH_RATES {
            bench_numeric::<u8, UInt8Array>(
                c,
                "narrow_integer",
                &format!("u8/list={list_size}/match={match_pct}%"),
                &NumericBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    |rng| rng.random(),
                    |v| ScalarValue::UInt8(Some(v)),
                ),
            );
        }
    }

    // Int16: larger value domain with wider list sizes
    for list_size in [4, 64, 256] {
        for match_pct in MATCH_RATES {
            bench_numeric::<i16, Int16Array>(
                c,
                "narrow_integer",
                &format!("i16/list={list_size}/match={match_pct}%"),
                &NumericBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    |rng| rng.random(),
                    |v| ScalarValue::Int16(Some(v)),
                ),
            );
        }
    }
}

// =============================================================================
// PRIMITIVE SIZE-SCALING BENCHMARKS
// =============================================================================

fn bench_primitive(c: &mut Criterion) {
    // Int32: small and larger list sizes
    for list_size in [4, 32, 64, 256] {
        let list_case = if list_size <= 32 {
            "small_list"
        } else {
            "large_list"
        };
        for match_pct in MATCH_RATES {
            bench_numeric::<i32, Int32Array>(
                c,
                "primitive",
                &format!("i32/{list_case}/list={list_size}/match={match_pct}%"),
                &NumericBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    |rng| rng.random(),
                    |v| ScalarValue::Int32(Some(v)),
                ),
            );
        }
    }

    // Int64: small and larger list sizes
    for list_size in [4, 16, 32, 128] {
        let list_case = if list_size <= 16 {
            "small_list"
        } else {
            "large_list"
        };
        for match_pct in MATCH_RATES {
            bench_numeric::<i64, Int64Array>(
                c,
                "primitive",
                &format!("i64/{list_case}/list={list_size}/match={match_pct}%"),
                &NumericBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    |rng| rng.random(),
                    |v| ScalarValue::Int64(Some(v)),
                ),
            );
        }
    }

    // NOT IN benchmark: test negated path
    bench_numeric::<i32, Int32Array>(
        c,
        "primitive",
        "i32/small_list/list=16/match=50%/NOT_IN",
        &NumericBenchConfig::new(
            16,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::Int32(Some(v)),
        )
        .with_negated(),
    );
}

// =============================================================================
// FLOAT AND TIMESTAMP CASE BENCHMARKS
// =============================================================================

fn bench_f32(c: &mut Criterion) {
    // Float32: uses the same list sizes as the Int32 cases.
    for list_size in [4, 32, 64] {
        let list_case = if list_size <= 32 {
            "small_list"
        } else {
            "large_list"
        };
        for match_pct in MATCH_RATES {
            bench_numeric::<f32, Float32Array>(
                c,
                "f32",
                &format!("{list_case}/list={list_size}/match={match_pct}%"),
                &NumericBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    |rng| rng.random::<f32>() * 1000.0,
                    |v| ScalarValue::Float32(Some(v)),
                ),
            );
        }
    }
}

fn bench_timestamp_ns(c: &mut Criterion) {
    // TimestampNanosecond: uses the same list sizes as the Int64-style cases.
    for list_size in [4, 16, 32] {
        let list_case = if list_size <= 16 {
            "small_list"
        } else {
            "large_list"
        };
        for match_pct in MATCH_RATES {
            bench_numeric::<i64, TimestampNanosecondArray>(
                c,
                "timestamp_ns",
                &format!("{list_case}/list={list_size}/match={match_pct}%"),
                &NumericBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    |rng| rng.random::<i64>().abs(),
                    |v| ScalarValue::TimestampNanosecond(Some(v), None),
                ),
            );
        }
    }
}

// =============================================================================
// UTF8 STRING CASE BENCHMARKS
// =============================================================================

fn bench_utf8(c: &mut Criterion) {
    let to_scalar: fn(String) -> ScalarValue = |s| ScalarValue::Utf8(Some(s));

    // Short strings (8 bytes)
    for list_size in [4, 64, 256] {
        for match_pct in MATCH_RATES {
            bench_string::<StringArray>(
                c,
                "utf8",
                &format!("short_8b/list={list_size}/match={match_pct}%"),
                &StringBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    8,
                    to_scalar,
                ),
            );
        }
    }

    // Long strings (24 bytes)
    for list_size in [4, 64, 256] {
        for match_pct in MATCH_RATES {
            bench_string::<StringArray>(
                c,
                "utf8",
                &format!("long_24b/list={list_size}/match={match_pct}%"),
                &StringBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    24,
                    to_scalar,
                ),
            );
        }
    }

    // Mixed-length strings: realistic distribution
    for list_size in [16, 64] {
        for match_pct in MATCH_RATES {
            bench_string_mixed_lengths::<StringArray>(
                c,
                "utf8",
                &format!("mixed_len/list={list_size}/match={match_pct}%"),
                list_size,
                match_pct as f64 / 100.0,
                to_scalar,
            );
        }
    }

    // Shared-prefix strings: same prefix, different suffix
    bench_string_shared_prefix::<StringArray>(
        c,
        "utf8",
        "shared_prefix/pfx=12/list=32/match=50%",
        32,
        0.5,
        12,
        to_scalar,
    );

    // NOT IN benchmark
    bench_string::<StringArray>(
        c,
        "utf8",
        "short_8b/list=16/match=50%/NOT_IN",
        &StringBenchConfig::new(16, 0.5, 8, to_scalar).with_negated(),
    );
}

// =============================================================================
// UTF8VIEW STRING CASE BENCHMARKS
// =============================================================================

fn bench_utf8view(c: &mut Criterion) {
    let to_scalar: fn(String) -> ScalarValue = |s| ScalarValue::Utf8View(Some(s));

    // Short strings (8 bytes)
    for list_size in [4, 16, 64, 256] {
        for match_pct in MATCH_RATES {
            bench_string::<StringViewArray>(
                c,
                "utf8view",
                &format!("short_8b/list={list_size}/match={match_pct}%"),
                &StringBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    8,
                    to_scalar,
                ),
            );
        }
    }

    // Length-12 strings
    for list_size in [16, 64] {
        for match_pct in MATCH_RATES {
            bench_string::<StringViewArray>(
                c,
                "utf8view",
                &format!("len_12b/list={list_size}/match={match_pct}%"),
                &StringBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    12,
                    to_scalar,
                ),
            );
        }
    }

    // Long strings (24 bytes)
    for list_size in [4, 16, 64, 256] {
        for match_pct in MATCH_RATES {
            bench_string::<StringViewArray>(
                c,
                "utf8view",
                &format!("long_24b/list={list_size}/match={match_pct}%"),
                &StringBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    24,
                    to_scalar,
                ),
            );
        }
    }

    // Mixed-length strings: realistic distribution
    for list_size in [16, 64] {
        for match_pct in MATCH_RATES {
            bench_string_mixed_lengths::<StringViewArray>(
                c,
                "utf8view",
                &format!("mixed_len/list={list_size}/match={match_pct}%"),
                list_size,
                match_pct as f64 / 100.0,
                to_scalar,
            );
        }
    }

    // Shared-prefix strings with varying prefix lengths
    for (prefix_len, list_size) in [(8, 16), (12, 32), (16, 64)] {
        for match_pct in MATCH_RATES {
            bench_string_shared_prefix::<StringViewArray>(
                c,
                "utf8view",
                &format!(
                    "shared_prefix/pfx={prefix_len}/list={list_size}/match={match_pct}%"
                ),
                list_size,
                match_pct as f64 / 100.0,
                prefix_len,
                to_scalar,
            );
        }
    }
}

// =============================================================================
// DICTIONARY ARRAY BENCHMARKS
// =============================================================================

/// Helper to benchmark dictionary-encoded Int32 arrays
fn bench_dict_int32(
    c: &mut Criterion,
    name: &str,
    dict_size: usize,
    list_size: usize,
    negated: bool,
) {
    let seed = 0xD1C7_0000_u64
        .wrapping_add(dict_size as u64 * 0x1111)
        .wrapping_add(list_size as u64 * 0x2222);
    let mut rng = StdRng::seed_from_u64(seed);

    let dict_values: Vec<i32> = (0..dict_size).map(|_| rng.random()).collect();
    let haystack: Vec<i32> = dict_values.iter().take(list_size).cloned().collect();

    let indices: Vec<i32> = (0..ARRAY_SIZE)
        .map(|_| rng.random_range(0..dict_size as i32))
        .collect();
    let indices_array = Int32Array::from(indices);
    let values_array = Int32Array::from(dict_values);
    let dict_array =
        DictionaryArray::<Int32Type>::try_new(indices_array, Arc::new(values_array))
            .unwrap();

    let schema = Schema::new(vec![Field::new("a", dict_array.data_type().clone(), true)]);
    let exprs: Vec<_> = haystack
        .iter()
        .map(|v| lit(ScalarValue::Int32(Some(*v))))
        .collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &negated, &schema).unwrap();
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict_array) as ArrayRef])
            .unwrap();

    c.bench_with_input(BenchmarkId::new("dictionary", name), &batch, |b, batch| {
        b.iter(|| expr.evaluate(batch).unwrap())
    });
}

/// Helper to benchmark dictionary-encoded string arrays
fn bench_dict_string(
    c: &mut Criterion,
    name: &str,
    dict_size: usize,
    list_size: usize,
    string_len: usize,
) {
    let seed = 0xD1C7_5778_u64
        .wrapping_add(dict_size as u64 * 0x3333)
        .wrapping_add(string_len as u64 * 0x4444);
    let mut rng = StdRng::seed_from_u64(seed);

    let dict_values: Vec<String> = (0..dict_size)
        .map(|_| random_string(&mut rng, string_len))
        .collect();
    let haystack: Vec<String> = dict_values.iter().take(list_size).cloned().collect();

    let indices: Vec<i32> = (0..ARRAY_SIZE)
        .map(|_| rng.random_range(0..dict_size as i32))
        .collect();
    let indices_array = Int32Array::from(indices);
    let values_array = StringArray::from(dict_values);
    let dict_array =
        DictionaryArray::<Int32Type>::try_new(indices_array, Arc::new(values_array))
            .unwrap();

    let schema = Schema::new(vec![Field::new("a", dict_array.data_type().clone(), true)]);
    let exprs: Vec<_> = haystack
        .iter()
        .map(|v| lit(ScalarValue::Utf8(Some(v.clone()))))
        .collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &false, &schema).unwrap();
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict_array) as ArrayRef])
            .unwrap();

    c.bench_with_input(BenchmarkId::new("dictionary", name), &batch, |b, batch| {
        b.iter(|| expr.evaluate(batch).unwrap())
    });
}

fn bench_dictionary(c: &mut Criterion) {
    // Int32 dictionary: varying list sizes across dictionary values
    // Dictionary with 100 unique values
    for list_size in [4, 16, 64] {
        bench_dict_int32(
            c,
            &format!("i32/dict=100/list={list_size}"),
            100,
            list_size,
            false,
        );
    }

    // Int32 dictionary: varying dictionary cardinality
    for dict_size in [10, 1000] {
        bench_dict_int32(
            c,
            &format!("i32/dict={dict_size}/list=16"),
            dict_size,
            16,
            false,
        );
    }

    // Int32 dictionary: NOT IN path
    bench_dict_int32(c, "i32/dict=100/list=16/NOT_IN", 100, 16, true);

    // String dictionary: short strings (<= 12 bytes, common for codes/categories)
    for list_size in [8, 32] {
        bench_dict_string(
            c,
            &format!("utf8_short/dict=50/list={list_size}"),
            50,
            list_size,
            8,
        );
    }

    // String dictionary: long strings (>12 bytes)
    bench_dict_string(c, "utf8_long/dict=100/list=16", 100, 16, 24);

    // String dictionary: large cardinality (realistic category counts)
    bench_dict_string(c, "utf8_short/dict=500/list=20", 500, 20, 10);
}

// =============================================================================
// NULL HANDLING BENCHMARKS
// =============================================================================
//
// Tests representative null-containing inputs across primitive and string cases.

fn bench_nulls(c: &mut Criterion) {
    // =========================================================================
    // PRIMITIVE CASES
    // =========================================================================

    // UInt8 case with nulls
    bench_numeric::<u8, UInt8Array>(
        c,
        "nulls",
        "narrow_integer/u8/list=16/match=50%/nulls=20%",
        &NumericBenchConfig::new(
            16,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::UInt8(Some(v)),
        )
        .with_null_rate(0.2),
    );

    // Int32 small-list case with nulls
    bench_numeric::<i32, Int32Array>(
        c,
        "nulls",
        "primitive/i32/small_list/list=16/match=50%/nulls=20%",
        &NumericBenchConfig::new(
            16,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::Int32(Some(v)),
        )
        .with_null_rate(0.2),
    );

    // Int32 large-list case with nulls
    bench_numeric::<i32, Int32Array>(
        c,
        "nulls",
        "primitive/i32/large_list/list=64/match=50%/nulls=20%",
        &NumericBenchConfig::new(
            64,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::Int32(Some(v)),
        )
        .with_null_rate(0.2),
    );

    // =========================================================================
    // STRING CASES
    // =========================================================================

    let utf8_scalar: fn(String) -> ScalarValue = |s| ScalarValue::Utf8(Some(s));
    let utf8view_scalar: fn(String) -> ScalarValue = |s| ScalarValue::Utf8View(Some(s));

    // Utf8 short-string case with nulls
    bench_string::<StringArray>(
        c,
        "nulls",
        "utf8/short_8b/list=16/match=50%/nulls=20%",
        &StringBenchConfig::new(16, 0.5, 8, utf8_scalar).with_null_rate(0.2),
    );

    // Utf8 long-string case with nulls
    bench_string::<StringArray>(
        c,
        "nulls",
        "utf8/long_24b/list=16/match=50%/nulls=20%",
        &StringBenchConfig::new(16, 0.5, 24, utf8_scalar).with_null_rate(0.2),
    );

    // Utf8View short-string case with nulls
    bench_string::<StringViewArray>(
        c,
        "nulls",
        "utf8view/short_8b/list=16/match=50%/nulls=20%",
        &StringBenchConfig::new(16, 0.5, 8, utf8view_scalar).with_null_rate(0.2),
    );

    // Utf8View long-string case with nulls
    bench_string::<StringViewArray>(
        c,
        "nulls",
        "utf8view/long_24b/list=16/match=50%/nulls=20%",
        &StringBenchConfig::new(16, 0.5, 24, utf8view_scalar).with_null_rate(0.2),
    );

    // =========================================================================
    // NOT IN CASES WITH NULLS
    // =========================================================================

    // Primitive NOT IN case with nulls
    bench_numeric::<i32, Int32Array>(
        c,
        "nulls",
        "primitive/i32/small_list/list=16/match=50%/nulls=20%/NOT_IN",
        &NumericBenchConfig::new(
            16,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::Int32(Some(v)),
        )
        .with_null_rate(0.2)
        .with_negated(),
    );

    // String NOT IN case with nulls
    bench_string::<StringViewArray>(
        c,
        "nulls",
        "utf8view/short_8b/list=16/match=50%/nulls=20%/NOT_IN",
        &StringBenchConfig::new(16, 0.5, 8, utf8view_scalar)
            .with_null_rate(0.2)
            .with_negated(),
    );

    // =========================================================================
    // HIGH NULL-RATE CASES
    // =========================================================================

    // 50% nulls - half the array is null
    bench_numeric::<i32, Int32Array>(
        c,
        "nulls",
        "primitive/i32/small_list/list=16/match=50%/nulls=50%",
        &NumericBenchConfig::new(
            16,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::Int32(Some(v)),
        )
        .with_null_rate(0.5),
    );

    bench_string::<StringViewArray>(
        c,
        "nulls",
        "utf8view/short_8b/list=16/match=50%/nulls=50%",
        &StringBenchConfig::new(16, 0.5, 8, utf8view_scalar).with_null_rate(0.5),
    );
}

// =============================================================================
// FIXED SIZE BINARY BENCHMARKS (FixedSizeBinary<16>, e.g. UUIDs)
// =============================================================================

/// Generates a random 16-byte value (UUID-sized).
fn random_fixed_binary_16(rng: &mut StdRng) -> Vec<u8> {
    let mut buf = vec![0u8; 16];
    rng.fill(&mut buf[..]);
    buf
}

/// Benchmarks FixedSizeBinary(16) IN list evaluation.
/// FixedSizeBinary doesn't use the generic numeric helpers since its array
/// construction differs from primitive types.
fn bench_fixed_size_binary_inner(
    c: &mut Criterion,
    name: &str,
    list_size: usize,
    match_rate: f64,
) {
    let seed = 0xF1ED_B1A7_u64.wrapping_add(list_size as u64 * 0x6666);
    let mut rng = StdRng::seed_from_u64(seed);

    // Generate IN list values (16-byte each)
    let haystack: Vec<Vec<u8>> = (0..list_size)
        .map(|_| random_fixed_binary_16(&mut rng))
        .collect();

    // Generate array with controlled match rate
    let values: Vec<Vec<u8>> = (0..ARRAY_SIZE)
        .map(|_| {
            if !haystack.is_empty() && rng.random_bool(match_rate) {
                haystack.choose(&mut rng).unwrap().clone()
            } else {
                random_fixed_binary_16(&mut rng)
            }
        })
        .collect();

    let refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
    let array = FixedSizeBinaryArray::from(refs);

    let schema = Schema::new(vec![Field::new("a", array.data_type().clone(), true)]);
    let exprs: Vec<_> = haystack
        .iter()
        .map(|v| lit(ScalarValue::FixedSizeBinary(16, Some(v.clone()))))
        .collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &false, &schema).unwrap();
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef])
        .unwrap();

    c.bench_with_input(
        BenchmarkId::new("fixed_size_binary", name),
        &batch,
        |b, batch| b.iter(|| expr.evaluate(batch).unwrap()),
    );
}

fn bench_fixed_size_binary(c: &mut Criterion) {
    for list_size in [4, 64, 256, 10000] {
        for match_pct in MATCH_RATES {
            bench_fixed_size_binary_inner(
                c,
                &format!("fsb16/list={list_size}/match={match_pct}%"),
                list_size,
                match_pct as f64 / 100.0,
            );
        }
    }
}

// =============================================================================
// CRITERION SETUP
// =============================================================================

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_narrow_integer, bench_primitive, bench_f32, bench_timestamp_ns, bench_utf8, bench_utf8view, bench_dictionary, bench_nulls, bench_fixed_size_binary
}

criterion_main!(benches);
