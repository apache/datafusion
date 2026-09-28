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

//! Benchmark for `UnionExec` construction cost as a function of child count.
//!
//! Scenarios (run against a flat and a nested/struct schema):
//! - `shared_arc`: every child returns the same `Arc<Schema>`
//! - `content_equal`: pointer-distinct but identical schemas per child
//! - `last_differs`: all children equal except extra metadata on the last
//!   child's last top-level field. Worst case for the equality fast path:
//!   the scan runs, fails on the final child, then the full merge runs anyway.
//! - `names_differ`: the first field is renamed in every child but the first,
//!   the common real-world unequal union where equality fails immediately.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::union::UnionExec;

const NUM_FIELDS: usize = 10;
const NESTED_CHILDREN: usize = 5;
const METADATA_PER_FIELD: usize = 2;

fn metadata(tag: &str, i: usize) -> HashMap<String, String> {
    (0..METADATA_PER_FIELD)
        .map(|m| (format!("key_{m}"), format!("value_{tag}_{i}_{m}")))
        .collect()
}

fn flat_schema() -> Schema {
    let fields: Vec<Field> = (0..NUM_FIELDS)
        .map(|i| {
            Field::new(format!("col_{i}"), DataType::Int64, true)
                .with_metadata(metadata("f", i))
        })
        .collect();
    Schema::new(fields)
}

fn nested_schema() -> Schema {
    let fields: Vec<Field> = (0..NUM_FIELDS)
        .map(|i| {
            let children: Vec<Field> = (0..NESTED_CHILDREN)
                .map(|c| {
                    Field::new(format!("sub_{i}_{c}"), DataType::Int64, true)
                        .with_metadata(metadata("n", i * NESTED_CHILDREN + c))
                })
                .collect();
            Field::new(format!("col_{i}"), DataType::Struct(children.into()), true)
                .with_metadata(metadata("s", i))
        })
        .collect();
    Schema::new(fields)
}

/// Clone `schema` with extra metadata on its last field. Divergence must stay
/// at the top level: differing nested fields change the field `DataType`
/// itself, which `UnionExec::try_new` rejects ("Schemas have to be aligned").
fn divergent(schema: &Schema) -> Schema {
    let mut fields: Vec<Field> =
        schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let last = fields.pop().unwrap();
    let mut md = last.metadata().clone();
    md.insert("divergent".to_string(), "true".to_string());
    fields.push(last.with_metadata(md));
    Schema::new(fields)
}

fn child(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(schema))
}

fn children_shared_arc(schema: &Schema, n: usize) -> Vec<Arc<dyn ExecutionPlan>> {
    let schema: SchemaRef = Arc::new(schema.clone());
    (0..n).map(|_| child(Arc::clone(&schema))).collect()
}

fn children_content_equal(schema: &Schema, n: usize) -> Vec<Arc<dyn ExecutionPlan>> {
    (0..n).map(|_| child(Arc::new(schema.clone()))).collect()
}

/// First child keeps `schema`; the rest rename the first field — the common
/// real-world unequal union (`SELECT a .. UNION ALL SELECT b ..`), where any
/// equality check fails immediately.
fn children_names_differ(schema: &Schema, n: usize) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut fields: Vec<Field> =
        schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let first = fields.remove(0);
    let renamed = first.clone().with_name(format!("renamed_{}", first.name()));
    fields.insert(0, renamed);
    let alt = Schema::new(fields);
    let mut children = vec![child(Arc::new(schema.clone()))];
    children.extend((1..n).map(|_| child(Arc::new(alt.clone()))));
    children
}

fn children_last_differs(schema: &Schema, n: usize) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut children = children_content_equal(schema, n - 1);
    children.push(child(Arc::new(divergent(schema))));
    children
}

fn bench_union_construction(c: &mut Criterion) {
    for (suffix, schema, sizes) in [
        ("", flat_schema(), &[100usize, 1000, 4000][..]),
        ("_nested", nested_schema(), &[1000, 4000][..]),
    ] {
        let mut group = c.benchmark_group(format!("union_exec_try_new{suffix}"));
        for &n in sizes {
            let shared = children_shared_arc(&schema, n);
            let content = children_content_equal(&schema, n);
            let differs = children_last_differs(&schema, n);

            group.bench_with_input(
                BenchmarkId::new("shared_arc", n),
                &shared,
                |b, ch| b.iter(|| UnionExec::try_new(ch.clone()).unwrap()),
            );
            group.bench_with_input(
                BenchmarkId::new("content_equal", n),
                &content,
                |b, ch| b.iter(|| UnionExec::try_new(ch.clone()).unwrap()),
            );
            group.bench_with_input(
                BenchmarkId::new("last_differs", n),
                &differs,
                |b, ch| b.iter(|| UnionExec::try_new(ch.clone()).unwrap()),
            );
            let names = children_names_differ(&schema, n);
            group.bench_with_input(
                BenchmarkId::new("names_differ", n),
                &names,
                |b, ch| b.iter(|| UnionExec::try_new(ch.clone()).unwrap()),
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_union_construction);
criterion_main!(benches);
