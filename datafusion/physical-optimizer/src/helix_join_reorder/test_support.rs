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

//! Fixtures shared by the tests in this module.
//!
//! Two kinds of leaf relation are available. [`scan`] is an `EmptyExec` and
//! carries no useful statistics, which suits tests that only care about plan
//! structure. [`relation`] reports whatever statistics the test asks for, which
//! is what the cost-model paths need.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, JoinType, Result, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::joins::{HashJoinExecBuilder, JoinOn};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

/// One column of a test relation, with the statistics it should report.
#[derive(Debug, Clone)]
pub struct Col {
    name: String,
    distinct_count: Option<usize>,
    null_count: usize,
}

/// A column whose distinct count is unknown, forcing the NDV fallback.
pub fn col(name: &str) -> Col {
    Col {
        name: name.to_string(),
        distinct_count: None,
        null_count: 0,
    }
}

/// A column reporting an exact distinct count.
pub fn col_ndv(name: &str, distinct_count: usize) -> Col {
    Col {
        name: name.to_string(),
        distinct_count: Some(distinct_count),
        null_count: 0,
    }
}

impl Col {
    /// Set this column's null count. Nulls are not distinct values, so this
    /// lowers the NDV the fallback derives from the row count.
    pub fn nulls(mut self, null_count: usize) -> Self {
        self.null_count = null_count;
        self
    }
}

/// Build a schema of `Int32` columns with the given names.
pub fn schema(columns: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|name| Field::new(*name, DataType::Int32, false))
            .collect::<Vec<_>>(),
    ))
}

/// A leaf relation with no useful statistics, for structure-only tests.
pub fn scan(columns: &[&str]) -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(schema(columns)))
}

/// A leaf relation reporting exactly the statistics the test asks for.
pub fn relation(columns: Vec<Col>, rows: usize) -> Arc<dyn ExecutionPlan> {
    let names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    let schema = schema(&names);

    let column_statistics = columns
        .iter()
        .map(|column| ColumnStatistics {
            distinct_count: match column.distinct_count {
                Some(count) => Precision::Exact(count),
                None => Precision::Absent,
            },
            null_count: Precision::Exact(column.null_count),
            ..ColumnStatistics::new_unknown()
        })
        .collect();

    FakeRelation::build(
        Arc::clone(&schema),
        Statistics {
            num_rows: Precision::Exact(rows),
            total_byte_size: Precision::Absent,
            column_statistics,
        },
    )
}

/// A leaf relation reporting `Inexact` statistics, as an operator downstream
/// of a filter would.
pub fn inexact_relation(columns: Vec<Col>, rows: usize) -> Arc<dyn ExecutionPlan> {
    let names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    let schema = schema(&names);

    let column_statistics = columns
        .iter()
        .map(|column| ColumnStatistics {
            distinct_count: match column.distinct_count {
                Some(count) => Precision::Inexact(count),
                None => Precision::Absent,
            },
            null_count: Precision::Inexact(column.null_count),
            ..ColumnStatistics::new_unknown()
        })
        .collect();

    FakeRelation::build(
        Arc::clone(&schema),
        Statistics {
            num_rows: Precision::Inexact(rows),
            total_byte_size: Precision::Absent,
            column_statistics,
        },
    )
}

/// A leaf relation whose row count is unknown, so the rule must decline.
pub fn relation_without_row_count(columns: &[&str]) -> Arc<dyn ExecutionPlan> {
    let schema = schema(columns);
    let statistics = Statistics::new_unknown(&schema);
    FakeRelation::build(schema, statistics)
}

/// One equijoin key pair, naming each column from the position it reads.
pub fn key(
    left: &Arc<dyn ExecutionPlan>,
    left_index: usize,
    right: &Arc<dyn ExecutionPlan>,
    right_index: usize,
) -> (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>) {
    let left_schema = left.schema();
    let right_schema = right.schema();
    (
        Arc::new(Column::new(
            left_schema.field(left_index).name(),
            left_index,
        )),
        Arc::new(Column::new(
            right_schema.field(right_index).name(),
            right_index,
        )),
    )
}

/// Equijoin keys from `(left index, right index)` pairs.
pub fn keys(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
    pairs: &[(usize, usize)],
) -> JoinOn {
    pairs
        .iter()
        .map(|&(left_index, right_index)| key(left, left_index, right, right_index))
        .collect()
}

/// A builder for an inner join, so tests can attach a filter, projection or
/// fetch limit before building.
pub fn join_builder(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    pairs: &[(usize, usize)],
) -> HashJoinExecBuilder {
    let on = keys(&left, &right, pairs);
    HashJoinExecBuilder::new(left, right, on, JoinType::Inner)
}

/// A plain inner join on the given `(left index, right index)` pairs.
pub fn join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    pairs: &[(usize, usize)],
) -> Arc<dyn ExecutionPlan> {
    join_builder(left, right, pairs)
        .build_exec()
        .expect("valid inner join")
}

/// A join of the given type, for testing that non-inner joins are refused.
pub fn typed_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    pairs: &[(usize, usize)],
    join_type: JoinType,
) -> Arc<dyn ExecutionPlan> {
    let on = keys(&left, &right, pairs);
    HashJoinExecBuilder::new(left, right, on, join_type)
        .build_exec()
        .expect("valid join")
}

/// A projection selecting `columns` of `input` without renaming any of them:
/// the column-pruning projection the physical planner inserts between joins.
pub fn prune(input: Arc<dyn ExecutionPlan>, columns: &[usize]) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    let expr: Vec<(Arc<dyn PhysicalExpr>, String)> = columns
        .iter()
        .map(|&index| {
            let name = schema.field(index).name();
            (
                Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                name.to_string(),
            )
        })
        .collect();

    Arc::new(ProjectionExec::try_new(expr, input).expect("a valid projection"))
}

/// A single diamond, with no useful statistics.
///
/// ```text
///        a0(2)
///       /     \
///   p0(3)     p1(5)
///       \     /
///        b0(4)
/// ```
///
/// Relations flatten as `p0` 0, `a0` 1, `b0` 2, `p1` 3. Widths are deliberately
/// all different: with uniform widths a mistaken offset can land on a valid
/// column of the wrong relation and a test still passes, while uneven widths
/// make misalignment visible.
pub fn diamond() -> Arc<dyn ExecutionPlan> {
    diamond_from(
        scan(&["p0_ka", "p0_kb", "p0_x"]),
        scan(&["a0_p0", "a0_p1"]),
        scan(&["b0_p0", "b0_p1", "b0_y", "b0_z"]),
        scan(&["p1_a", "p1_b", "p1_c", "p1_d", "p1_e"]),
    )
}

/// The same diamond with statistics attached, chosen so the cheapest order is
/// not the one the plan is built in.
pub fn measured_diamond() -> Arc<dyn ExecutionPlan> {
    diamond_from(
        relation(vec![col("p0_ka"), col("p0_kb"), col("p0_x")], 1_000),
        relation(vec![col("a0_p0"), col("a0_p1")], 50),
        relation(
            vec![col("b0_p0"), col("b0_p1"), col("b0_y"), col("b0_z")],
            200,
        ),
        relation(
            vec![
                col("p1_a"),
                col("p1_b"),
                col("p1_c"),
                col("p1_d"),
                col("p1_e"),
            ],
            5_000,
        ),
    )
}

/// A diamond wired from caller-supplied relations. Widths must match
/// [`diamond`].
///
/// The last join carries two key pairs at once, which is how a cycle reaches
/// the rewrite: the second path closes at the same step the first does.
pub fn diamond_from(
    p0: Arc<dyn ExecutionPlan>,
    a0: Arc<dyn ExecutionPlan>,
    b0: Arc<dyn ExecutionPlan>,
    p1: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let left = join(p0, a0, &[(0, 0)]);
    let left = join(left, b0, &[(1, 0)]);
    // `a0`'s second column is global 4 and `b0`'s is global 6.
    join(left, p1, &[(4, 0), (6, 1)])
}

/// A helix of two diamonds, with statistics attached.
///
/// ```text
///      a0        a1
///     /  \      /  \
///   p0    p1  p1    p2
///     \  /      \  /
///      b0        b1
/// ```
///
/// Relations flatten as `p0` 0, `a0` 1, `b0` 2, `p1` 3, `a1` 4, `b1` 5,
/// `p2` 6, and widths differ throughout for the same reason as [`diamond`].
pub fn helix() -> Arc<dyn ExecutionPlan> {
    let p0 = relation(vec![col("p0_ka"), col("p0_kb"), col("p0_x")], 1_000);
    let a0 = relation(vec![col("a0_p0"), col("a0_p1")], 50);
    let b0 = relation(
        vec![col("b0_p0"), col("b0_p1"), col("b0_y"), col("b0_z")],
        200,
    );
    let p1 = relation(
        vec![col("p1_a"), col("p1_b"), col("p1_ka"), col("p1_kb")],
        5_000,
    );
    let a1 = relation(vec![col("a1_p1"), col("a1_p2"), col("a1_x")], 80);
    let b1 = relation(vec![col("b1_p1"), col("b1_p2")], 300);
    let p2 = relation(
        vec![
            col("p2_a"),
            col("p2_b"),
            col("p2_c"),
            col("p2_d"),
            col("p2_e"),
        ],
        2_000,
    );

    // The first diamond, closing on p1's first two columns.
    let left = join(p0, a0, &[(0, 0)]);
    let left = join(left, b0, &[(1, 0)]);
    let left = join(left, p1, &[(4, 0), (6, 1)]);
    // p1's key columns are globals 11 and 12.
    let left = join(left, a1, &[(11, 0)]);
    let left = join(left, b1, &[(12, 0)]);
    // a1's second column is global 14 and b1's is global 17.
    join(left, p2, &[(14, 0), (17, 1)])
}

/// A leaf relation reporting statistics fixed by the test.
///
/// Only used for planning: [`ExecutionPlan::execute`] is never called, since
/// these tests inspect plans rather than run them.
#[derive(Debug)]
struct FakeRelation {
    schema: SchemaRef,
    statistics: Arc<Statistics>,
    properties: Arc<PlanProperties>,
}

impl FakeRelation {
    fn build(schema: SchemaRef, statistics: Statistics) -> Arc<dyn ExecutionPlan> {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Arc::new(Self {
            schema,
            statistics: Arc::new(statistics),
            properties,
        })
    }
}

impl DisplayAs for FakeRelation {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FakeRelation")
    }
}

impl ExecutionPlan for FakeRelation {
    fn name(&self) -> &str {
        "FakeRelation"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _: usize,
        _: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("FakeRelation is only used for planning")
    }

    fn partition_statistics(&self, _: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&self.statistics))
    }
}
