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

//! Focused benchmarks for InList optimizations
//!
//! This benchmark file provides targeted coverage of each optimization strategy
//! with controlled parameters to ensure statistical robustness:
//!
//! - **Controlled match rates**: Tests both "found" and "not found" code paths
//! - **List size scaling**: Measures performance across different list sizes
//! - **Strategy coverage**: Each optimization has dedicated benchmarks
//! - **Reinterpret coverage**: Tests types that use zero-copy reinterpretation
//! - **Stage 2 stress testing**: Prefix-collision strings for two-stage filters
//! - **Null handling**: Tests null short-circuit optimization paths
//!
//! # Optimization Coverage
//!
//! | Strategy | Types | Threshold | List Sizes Tested |
//! |----------|-------|-----------|-------------------|
//! | BitmapFilter (stack) | UInt8 | always | 4, 16 |
//! | BitmapFilter (heap) | Int16 | always | 4, 64, 256 |
//! | BranchlessFilter | Int32, Float32 | ≤32 | 4, 32 |
//! | DirectProbeFilter | Int32, Float32 | >32 | 64, 256 |
//! | BranchlessFilter | Int64, TimestampNs | ≤16 | 4, 16 |
//! | DirectProbeFilter | Int64, TimestampNs | >16 | 32, 128 |
//! | Utf8TwoStageFilter | Utf8 | always | 4, 64, 256 |
//! | ByteViewMaskedFilter | Utf8View | always | 4, 16, 64, 256 |

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

/// Benchmarks strings with shared prefixes to stress Stage 2 of two-stage filters.
/// Uses variable prefix lengths and random suffixes to avoid bench-maxing.
fn bench_string_prefix_collision<A>(
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

    // Generate IN list with shared prefix (forces Stage 2)
    let haystack = strings_with_shared_prefix(&mut rng, list_size, prefix_len);

    // Generate non-matching strings with SAME prefix (will pass Stage 1, fail Stage 2)
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

/// Benchmarks mixed-length strings (some short ≤12, some long >12).
/// Tests the two-stage filter with realistic length distribution.
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

    // Mixed lengths: some short (≤12), some long (>12)
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
// BITMAP FILTER BENCHMARKS (UInt8, Int16)
// =============================================================================

fn bench_bitmap(c: &mut Criterion) {
    // UInt8: 32-byte stack-allocated bitmap
    // NOTE: With 256 possible values, list_size=16 covers 6.25% of value space,
    // so even "match=0%" has ~6% accidental matches from random data.
    for list_size in [4, 16] {
        for match_pct in MATCH_RATES {
            bench_numeric::<u8, UInt8Array>(
                c,
                "bitmap",
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

    // Int16: 8KB heap-allocated bitmap (via zero-copy reinterpret)
    for list_size in [4, 64, 256] {
        for match_pct in MATCH_RATES {
            bench_numeric::<i16, Int16Array>(
                c,
                "bitmap",
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
// PRIMITIVE BENCHMARKS (Branchless vs Hash)
// =============================================================================

fn bench_primitive(c: &mut Criterion) {
    // Int32: branchless threshold is 32
    for list_size in [4, 32, 64, 256] {
        let strategy = if list_size <= 32 {
            "branchless"
        } else {
            "hash"
        };
        for match_pct in MATCH_RATES {
            bench_numeric::<i32, Int32Array>(
                c,
                "primitive",
                &format!("i32/{strategy}/list={list_size}/match={match_pct}%"),
                &NumericBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    |rng| rng.random(),
                    |v| ScalarValue::Int32(Some(v)),
                ),
            );
        }
    }

    // Int64: branchless threshold is 16
    for list_size in [4, 16, 32, 128] {
        let strategy = if list_size <= 16 {
            "branchless"
        } else {
            "hash"
        };
        for match_pct in MATCH_RATES {
            bench_numeric::<i64, Int64Array>(
                c,
                "primitive",
                &format!("i64/{strategy}/list={list_size}/match={match_pct}%"),
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
        "i32/branchless/list=16/match=50%/NOT_IN",
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
// REINTERPRETED TYPE BENCHMARKS (Float32, TimestampNs)
// =============================================================================

fn bench_reinterpret(c: &mut Criterion) {
    // Float32: reinterpreted as u32, uses same branchless/hash strategies
    // Threshold is 32 (same as Int32)
    for list_size in [4, 32, 64] {
        let strategy = if list_size <= 32 {
            "branchless"
        } else {
            "hash"
        };
        for match_pct in MATCH_RATES {
            bench_numeric::<f32, Float32Array>(
                c,
                "reinterpret",
                &format!("f32/{strategy}/list={list_size}/match={match_pct}%"),
                &NumericBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    |rng| rng.random::<f32>() * 1000.0,
                    |v| ScalarValue::Float32(Some(v)),
                ),
            );
        }
    }

    // TimestampNanosecond: reinterpreted as i64, threshold is 16
    for list_size in [4, 16, 32] {
        let strategy = if list_size <= 16 {
            "branchless"
        } else {
            "hash"
        };
        for match_pct in MATCH_RATES {
            bench_numeric::<i64, TimestampNanosecondArray>(
                c,
                "reinterpret",
                &format!("timestamp_ns/{strategy}/list={list_size}/match={match_pct}%"),
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
// UTF8 TWO-STAGE FILTER BENCHMARKS
// =============================================================================

fn bench_utf8(c: &mut Criterion) {
    let to_scalar: fn(String) -> ScalarValue = |s| ScalarValue::Utf8(Some(s));

    // Short strings (8 bytes < 12): Stage 1 definitive
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

    // Long strings (24 bytes > 12): hits Stage 2
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

    // Prefix collision: stresses Stage 2 comparison
    bench_string_prefix_collision::<StringArray>(
        c,
        "utf8",
        "prefix_collision/pfx=12/list=32/match=50%",
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
// UTF8VIEW TWO-STAGE FILTER BENCHMARKS
// =============================================================================

fn bench_utf8view(c: &mut Criterion) {
    let to_scalar: fn(String) -> ScalarValue = |s| ScalarValue::Utf8View(Some(s));

    // Short strings (8 bytes ≤ 12): inline storage path
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

    // Boundary strings (exactly 12 bytes): max inline size
    for list_size in [16, 64] {
        for match_pct in MATCH_RATES {
            bench_string::<StringViewArray>(
                c,
                "utf8view",
                &format!("boundary_12b/list={list_size}/match={match_pct}%"),
                &StringBenchConfig::new(
                    list_size,
                    match_pct as f64 / 100.0,
                    12,
                    to_scalar,
                ),
            );
        }
    }

    // Long strings (24 bytes > 12): out-of-line storage, two-stage filter
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

    // Prefix collision: stresses Stage 2 comparison with varying prefix lengths
    for (prefix_len, list_size) in [(8, 16), (12, 32), (16, 64)] {
        for match_pct in MATCH_RATES {
            bench_string_prefix_collision::<StringViewArray>(
                c,
                "utf8view",
                &format!(
                    "prefix_collision/pfx={prefix_len}/list={list_size}/match={match_pct}%"
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
    // Int32 dictionary: varying list sizes (tests branchless vs hash on values)
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

    // String dictionary: short strings (≤12 bytes, common for codes/categories)
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
// Tests null short-circuit optimization paths in:
// - build_in_list_result: computes contains for ALL positions, masks via bitmap ops
// - build_in_list_result_with_null_shortcircuit: skips contains for null positions
//
// The shortcircuit is beneficial for expensive contains checks (strings) but
// adds branch overhead for cheap checks (primitives).

fn bench_nulls(c: &mut Criterion) {
    // =========================================================================
    // PRIMITIVE TYPES: Tests build_in_list_result (no shortcircuit)
    // =========================================================================

    // BitmapFilter with nulls
    bench_numeric::<u8, UInt8Array>(
        c,
        "nulls",
        "bitmap/u8/list=16/match=50%/nulls=20%",
        &NumericBenchConfig::new(
            16,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::UInt8(Some(v)),
        )
        .with_null_rate(0.2),
    );

    // BranchlessFilter with nulls
    bench_numeric::<i32, Int32Array>(
        c,
        "nulls",
        "branchless/i32/list=16/match=50%/nulls=20%",
        &NumericBenchConfig::new(
            16,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::Int32(Some(v)),
        )
        .with_null_rate(0.2),
    );

    // DirectProbeFilter with nulls
    bench_numeric::<i32, Int32Array>(
        c,
        "nulls",
        "hash/i32/list=64/match=50%/nulls=20%",
        &NumericBenchConfig::new(
            64,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::Int32(Some(v)),
        )
        .with_null_rate(0.2),
    );

    // =========================================================================
    // STRING TYPES: Tests build_in_list_result_with_null_shortcircuit
    // =========================================================================

    let utf8_scalar: fn(String) -> ScalarValue = |s| ScalarValue::Utf8(Some(s));
    let utf8view_scalar: fn(String) -> ScalarValue = |s| ScalarValue::Utf8View(Some(s));

    // Utf8TwoStageFilter with nulls (short strings)
    bench_string::<StringArray>(
        c,
        "nulls",
        "utf8/short_8b/list=16/match=50%/nulls=20%",
        &StringBenchConfig::new(16, 0.5, 8, utf8_scalar).with_null_rate(0.2),
    );

    // Utf8TwoStageFilter with nulls (long strings - Stage 2)
    bench_string::<StringArray>(
        c,
        "nulls",
        "utf8/long_24b/list=16/match=50%/nulls=20%",
        &StringBenchConfig::new(16, 0.5, 24, utf8_scalar).with_null_rate(0.2),
    );

    // ByteViewMaskedFilter with nulls (short strings - inline)
    bench_string::<StringViewArray>(
        c,
        "nulls",
        "utf8view/short_8b/list=16/match=50%/nulls=20%",
        &StringBenchConfig::new(16, 0.5, 8, utf8view_scalar).with_null_rate(0.2),
    );

    // ByteViewMaskedFilter with nulls (long strings - out-of-line)
    bench_string::<StringViewArray>(
        c,
        "nulls",
        "utf8view/long_24b/list=16/match=50%/nulls=20%",
        &StringBenchConfig::new(16, 0.5, 24, utf8view_scalar).with_null_rate(0.2),
    );

    // =========================================================================
    // NOT IN WITH NULLS: Tests negated path with null propagation
    // =========================================================================

    // Primitive NOT IN with nulls
    bench_numeric::<i32, Int32Array>(
        c,
        "nulls",
        "branchless/i32/list=16/match=50%/nulls=20%/NOT_IN",
        &NumericBenchConfig::new(
            16,
            0.5,
            |rng| rng.random(),
            |v| ScalarValue::Int32(Some(v)),
        )
        .with_null_rate(0.2)
        .with_negated(),
    );

    // String NOT IN with nulls
    bench_string::<StringViewArray>(
        c,
        "nulls",
        "utf8view/short_8b/list=16/match=50%/nulls=20%/NOT_IN",
        &StringBenchConfig::new(16, 0.5, 8, utf8view_scalar)
            .with_null_rate(0.2)
            .with_negated(),
    );

    // =========================================================================
    // HIGH NULL RATE: Stress test null handling paths
    // =========================================================================

    // 50% nulls - half the array is null
    bench_numeric::<i32, Int32Array>(
        c,
        "nulls",
        "branchless/i32/list=16/match=50%/nulls=50%",
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
// CRITERION SETUP
// =============================================================================

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_bitmap, bench_primitive, bench_reinterpret, bench_utf8, bench_utf8view, bench_dictionary, bench_nulls
}

criterion_main!(benches);
