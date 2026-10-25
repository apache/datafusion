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

//! Benchmarks short SQL `IN` lists using the default optimizer. Comparing the
//! same cases across commits measures changes to the chosen evaluation strategy.
//! Criterion times expression evaluation, with planning and table setup outside
//! the measured loop. Input types and expected Boolean results are checked first.
//!
//! All types cover misses, balanced matches, and uniform or skewed
//! first-item hits. Floating-point and fixed-size-binary types also cover
//! nulls and small batches; a misaligned binary batch exercises alignment copies.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, FixedSizeBinaryArray};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::LogicalPlan;
use datafusion_physical_expr::PhysicalExpr;
use rand::prelude::*;
use tokio::runtime::Runtime;

const MISS_VALUE_BASE: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueKind {
    Int32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Utf8View,
    FixedSizeBinary(i32),
}

impl ValueKind {
    const ALL: [Self; 12] = [
        Self::Int32,
        Self::Int64,
        Self::UInt64,
        Self::Float32,
        Self::Float64,
        Self::Utf8,
        Self::Utf8View,
        Self::FixedSizeBinary(1),
        Self::FixedSizeBinary(2),
        Self::FixedSizeBinary(4),
        Self::FixedSizeBinary(8),
        Self::FixedSizeBinary(16),
    ];

    fn name(self) -> String {
        match self {
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::UInt64 => "uint64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Utf8View => "utf8view_inline",
            Self::FixedSizeBinary(width) => return format!("fixed_size_binary_{width}"),
        }
        .to_owned()
    }

    fn seed_tag(self) -> u64 {
        match self {
            Self::Int32 => 1,
            Self::Float64 => 2,
            Self::Utf8 => 3,
            Self::Utf8View => 4,
            Self::FixedSizeBinary(8) => 5,
            Self::Int64 => 6,
            Self::Float32 => 7,
            Self::UInt64 => 8,
            Self::FixedSizeBinary(width) => 20 + width as u64,
        }
    }

    // The same typed values build batches and bind SQL parameters, avoiding
    // implicit SQL literal widening for Float32 and UInt64.
    fn scalar(self, value: Option<usize>) -> ScalarValue {
        match self {
            Self::Int32 => ScalarValue::Int32(value.map(|v| v as i32)),
            Self::Int64 => ScalarValue::Int64(value.map(|v| v as i64)),
            Self::UInt64 => ScalarValue::UInt64(value.map(|v| v as u64)),
            Self::Float32 => ScalarValue::Float32(value.map(|v| v as f32)),
            Self::Float64 => ScalarValue::Float64(value.map(|v| v as f64)),
            Self::Utf8 => ScalarValue::Utf8(value.map(string_value)),
            Self::Utf8View => ScalarValue::Utf8View(value.map(string_value)),
            Self::FixedSizeBinary(width) => ScalarValue::FixedSizeBinary(
                width,
                value.map(|v| {
                    // Narrow misses stay disjoint from the hit values 0, 1, and 2.
                    let v = if width == 1 && v >= MISS_VALUE_BASE {
                        4 + (v - MISS_VALUE_BASE) % 252
                    } else {
                        v
                    };
                    (v as u128).to_be_bytes()[16 - width as usize..].to_vec()
                }),
            ),
        }
    }

    fn data_type(self) -> DataType {
        self.scalar(Some(0)).data_type()
    }

    fn make_array(self, values: &[Option<usize>]) -> ArrayRef {
        let array =
            ScalarValue::iter_to_array(values.iter().map(|&v| self.scalar(v))).unwrap();
        if let Self::FixedSizeBinary(width) = self {
            let binary = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(
                binary.values().as_ptr().align_offset(width as usize),
                0,
                "builder-created binary buffer must be aligned"
            );
        }
        array
    }
}

// Eight-byte strings use Arrow's inline byte-view representation.
fn string_value(value: usize) -> String {
    format!("v{value:07}")
}

#[derive(Clone, Copy)]
enum Profile {
    Miss,
    Balanced,
    AllFirstHit,
    SkewedHit,
    Nullable,
    SmallBatch,
    SingleMiss,
    SingleHit,
    ListNull,
    Unaligned,
}

impl Profile {
    const ALL: [Self; 10] = [
        Self::Miss,
        Self::Balanced,
        Self::AllFirstHit,
        Self::SkewedHit,
        Self::Nullable,
        Self::SmallBatch,
        Self::SingleMiss,
        Self::SingleHit,
        Self::ListNull,
        Self::Unaligned,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Miss => "miss",
            Self::Balanced => "balanced",
            Self::AllFirstHit => "all_first_hit",
            Self::SkewedHit => "skewed_hit",
            Self::Nullable => "balanced_nullable",
            Self::SmallBatch => "balanced_small_batch",
            Self::SingleMiss => "single_row_miss",
            Self::SingleHit => "single_row_hit",
            Self::ListNull => "list_null_balanced",
            Self::Unaligned => "balanced_unaligned",
        }
    }

    fn batch_size(self) -> usize {
        match self {
            Self::SmallBatch => 64,
            Self::SingleMiss | Self::SingleHit => 1,
            _ => 8192,
        }
    }

    fn null_percent(self) -> usize {
        if matches!(self, Self::Nullable) {
            20
        } else {
            0
        }
    }

    fn match_percent(self) -> usize {
        match self {
            Self::Miss | Self::SingleMiss => 0,
            Self::AllFirstHit | Self::SingleHit => 100,
            Self::SkewedHit => 99,
            _ => 50,
        }
    }

    fn permits_kind(self, kind: ValueKind) -> bool {
        match self {
            Self::Unaligned => kind == ValueKind::FixedSizeBinary(16),
            Self::Miss | Self::Balanced | Self::AllFirstHit | Self::SkewedHit => true,
            _ => matches!(
                kind,
                ValueKind::Float32 | ValueKind::Float64 | ValueKind::FixedSizeBinary(_)
            ),
        }
    }
}

// Project the predicate so optimization preserves its Boolean result, including
// NULLs, rather than simplifying it based on WHERE semantics.
fn plan_in_list(
    ctx: &SessionContext,
    runtime: &Runtime,
    kind: ValueKind,
    batch: &RecordBatch,
    list_len: usize,
    negated: bool,
    list_has_null: bool,
) -> Arc<dyn PhysicalExpr> {
    ctx.deregister_table(kind.name()).unwrap();
    ctx.register_batch(kind.name(), batch.clone()).unwrap();
    let params = (0..list_len)
        .map(|i| kind.scalar((!list_has_null || i + 1 < list_len).then_some(i)))
        .collect::<Vec<_>>();
    assert!(
        params
            .iter()
            .all(|value| value.data_type() == kind.data_type())
    );
    let placeholders = (1..=list_len).map(|i| format!("${i}")).collect::<Vec<_>>();
    let sql = format!(
        "SELECT a {}IN ({}) AS matches FROM {}",
        if negated { "NOT " } else { "" },
        placeholders.join(", "),
        kind.name()
    );
    let plan = runtime
        .block_on(ctx.sql(&sql))
        .unwrap()
        .with_param_values(params)
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("SQL must project the predicate");
    };
    assert_eq!(projection.expr.len(), 1);
    assert_eq!(
        projection.input.schema().field(0).data_type(),
        &kind.data_type()
    );
    let expr = ctx
        .create_physical_expr(
            projection.expr[0].clone().unalias(),
            projection.input.schema(),
        )
        .unwrap();
    let schema = batch.schema();
    assert_eq!(expr.data_type(schema.as_ref()).unwrap(), DataType::Boolean);
    // Check types without requiring either IN or comparison-chain expressions.
    let mut nodes = vec![Arc::clone(&expr)];
    while let Some(node) = nodes.pop() {
        let data_type = node.data_type(schema.as_ref()).unwrap();
        assert!(data_type == DataType::Boolean || data_type == kind.data_type());
        nodes.extend(node.children().into_iter().cloned());
    }
    expr
}

fn make_batch(
    kind: ValueKind,
    profile: Profile,
    profile_index: usize,
    list_len: usize,
    negated: bool,
) -> (RecordBatch, BooleanArray) {
    let null_count = profile.batch_size() * profile.null_percent() / 100;
    let non_null_count = profile.batch_size() - null_count;
    let match_count = non_null_count * profile.match_percent() / 100;
    let list_has_null = matches!(profile, Profile::ListNull);
    let first_hits = matches!(
        profile,
        Profile::AllFirstHit | Profile::SkewedHit | Profile::SingleHit
    );
    let mut values = Vec::with_capacity(profile.batch_size());
    values.extend(std::iter::repeat_n(None, null_count));
    values.extend((0..match_count).map(|i| {
        Some(if first_hits {
            0
        } else {
            i % (list_len - usize::from(list_has_null))
        })
    }));
    values.extend((0..non_null_count - match_count).map(|i| Some(MISS_VALUE_BASE + i)));
    let seed = 0x1A11_1575_5EED_u64
        ^ (kind.seed_tag() << 48)
        ^ ((profile_index as u64) << 32)
        ^ list_len as u64;
    values.shuffle(&mut StdRng::seed_from_u64(seed));
    let expected = values
        .iter()
        .map(|value| {
            value.and_then(|v| {
                if v < list_len - usize::from(list_has_null) {
                    Some(!negated)
                } else if list_has_null {
                    None
                } else {
                    Some(negated)
                }
            })
        })
        .collect::<BooleanArray>();
    let mut array = kind.make_array(&values);
    if matches!(profile, Profile::Unaligned) {
        let binary = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let mut bytes = MutableBuffer::with_capacity(binary.values().len() + 1);
        bytes.push(0_u8);
        bytes.extend_from_slice(binary.values());
        let buffer = Buffer::from(bytes).slice(1);
        assert_ne!(buffer.as_ptr().align_offset(16), 0);
        array = Arc::new(FixedSizeBinaryArray::new(
            16,
            buffer,
            binary.nulls().cloned(),
        ));
    }
    let schema = Arc::new(Schema::new(vec![Field::new("a", kind.data_type(), true)]));
    (RecordBatch::try_new(schema, vec![array]).unwrap(), expected)
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let ctx = SessionContext::new();
    for kind in ValueKind::ALL {
        for (profile_index, profile) in Profile::ALL.into_iter().enumerate() {
            if !profile.permits_kind(kind) {
                continue;
            }
            let mut group = c.benchmark_group(format!(
                "in_list_rewrite/{}/{}/batch={}/nulls={}%/match={}%",
                kind.name(),
                profile.name(),
                profile.batch_size(),
                profile.null_percent(),
                profile.match_percent()
            ));
            group.throughput(Throughput::Elements(profile.batch_size() as u64));
            for list_len in [2, 3] {
                for negated in [false, true] {
                    let (batch, expected) =
                        make_batch(kind, profile, profile_index, list_len, negated);
                    let expr = plan_in_list(
                        &ctx,
                        &runtime,
                        kind,
                        &batch,
                        list_len,
                        negated,
                        matches!(profile, Profile::ListNull),
                    );
                    let output = expr
                        .evaluate(&batch)
                        .unwrap()
                        .into_array(batch.num_rows())
                        .unwrap();
                    assert_eq!(output.as_boolean(), &expected);
                    let case = format!(
                        "{}/list={list_len}",
                        if negated { "not_in" } else { "in" }
                    );
                    group.bench_function(case, |b| {
                        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
                    });
                }
            }
            group.finish();
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500));
    targets = criterion_benchmark
}
criterion_main!(benches);
