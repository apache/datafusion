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

//! Micro-benchmark for `LogicalPlan::map_expressions` on Extension nodes.
//!
//! Extension nodes can have many children but no expressions. When
//! `expressions()` returns empty, `map_expressions` should skip the
//! expensive clone-all-inputs + `with_exprs_and_inputs` rebuild.

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore, col};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Extension node with NO expressions (e.g. OneOf in view matching)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct NoExprExtension {
    children: Vec<LogicalPlan>,
    schema: DFSchemaRef,
}

impl PartialEq for NoExprExtension {
    fn eq(&self, other: &Self) -> bool {
        self.children.len() == other.children.len()
    }
}
impl Eq for NoExprExtension {}

impl Hash for NoExprExtension {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.children.len().hash(state);
    }
}

impl PartialOrd for NoExprExtension {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.children.len().cmp(&other.children.len()))
    }
}

impl UserDefinedLogicalNodeCore for NoExprExtension {
    fn name(&self) -> &str {
        "NoExprExtension"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.children.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![] // Key: no expressions
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NoExprExtension(children={})", self.children.len())
    }
    fn with_exprs_and_inputs(
        &self,
        _: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            children: inputs,
            schema: Arc::clone(&self.schema),
        })
    }
}

// ---------------------------------------------------------------------------
// Extension node WITH expressions (control group)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct WithExprExtension {
    children: Vec<LogicalPlan>,
    exprs: Vec<Expr>,
    schema: DFSchemaRef,
}

impl PartialEq for WithExprExtension {
    fn eq(&self, other: &Self) -> bool {
        self.children.len() == other.children.len()
            && self.exprs.len() == other.exprs.len()
    }
}
impl Eq for WithExprExtension {}

impl Hash for WithExprExtension {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.children.len().hash(state);
        self.exprs.len().hash(state);
    }
}

impl PartialOrd for WithExprExtension {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.children
                .len()
                .cmp(&other.children.len())
                .then(self.exprs.len().cmp(&other.exprs.len())),
        )
    }
}

impl UserDefinedLogicalNodeCore for WithExprExtension {
    fn name(&self) -> &str {
        "WithExprExtension"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.children.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WithExprExtension(children={}, exprs={})",
            self.children.len(),
            self.exprs.len()
        )
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            children: inputs,
            exprs,
            schema: Arc::clone(&self.schema),
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_schema(num_cols: usize) -> DFSchemaRef {
    let fields: Vec<Field> = (0..num_cols)
        .map(|i| Field::new(format!("col_{i}"), DataType::Utf8, true))
        .collect();
    Arc::new(DFSchema::try_from(Schema::new(fields)).unwrap())
}

fn build_child(schema: &DFSchemaRef) -> LogicalPlan {
    LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
        produce_one_row: false,
        schema: Arc::clone(schema),
    })
}

fn build_no_expr_plan(num_children: usize, num_cols: usize) -> LogicalPlan {
    let schema = build_schema(num_cols);
    let children: Vec<LogicalPlan> =
        (0..num_children).map(|_| build_child(&schema)).collect();
    LogicalPlan::Extension(Extension {
        node: Arc::new(NoExprExtension {
            children,
            schema: Arc::clone(&schema),
        }),
    })
}

fn build_with_expr_plan(num_children: usize, num_cols: usize) -> LogicalPlan {
    let schema = build_schema(num_cols);
    let children: Vec<LogicalPlan> =
        (0..num_children).map(|_| build_child(&schema)).collect();
    let exprs: Vec<Expr> = (0..3).map(|i| col(format!("col_{i}"))).collect();
    LogicalPlan::Extension(Extension {
        node: Arc::new(WithExprExtension {
            children,
            exprs,
            schema: Arc::clone(&schema),
        }),
    })
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn benchmark_map_expressions(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_expressions_extension");

    let num_cols = 40;

    for num_children in [1, 3, 5, 10] {
        let no_expr_plan = build_no_expr_plan(num_children, num_cols);
        let with_expr_plan = build_with_expr_plan(num_children, num_cols);

        group.bench_with_input(
            BenchmarkId::new("no_expr", num_children),
            &no_expr_plan,
            |b, plan| {
                b.iter(|| {
                    let result = plan
                        .clone()
                        .map_expressions(|expr| Ok(Transformed::no(expr)))
                        .unwrap();
                    std::hint::black_box(result);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("with_expr", num_children),
            &with_expr_plan,
            |b, plan| {
                b.iter(|| {
                    let result = plan
                        .clone()
                        .map_expressions(|expr| Ok(Transformed::no(expr)))
                        .unwrap();
                    std::hint::black_box(result);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_map_expressions);
criterion_main!(benches);
