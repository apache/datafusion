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

/// The canonical bowtie, with no useful statistics.
///
/// ```text
///   a1(1)  a2(4)              b1(2)
///       \  /                    |
///      hub_a(3) --- c(2) --- hub_b(5)
/// ```
///
/// Relation widths are deliberately all different. With uniform widths a
/// mistaken offset can land on a valid column of the wrong relation and a test
/// still passes; uneven widths make misalignment visible.
pub fn bowtie() -> Arc<dyn ExecutionPlan> {
    bowtie_from(
        scan(&["ha_k", "ha_s1", "ha_s2"]),
        scan(&["a1_k"]),
        scan(&["a2_k", "a2_x", "a2_y", "a2_z"]),
        scan(&["c_ka", "c_kb"]),
        scan(&["hb_k", "hb_s1", "hb_p", "hb_q", "hb_r"]),
        scan(&["b1_k", "b1_x"]),
    )
}

/// The same bowtie wired from caller-supplied relations, so a test can give
/// them statistics. Widths must match [`bowtie`].
pub fn bowtie_from(
    hub_a: Arc<dyn ExecutionPlan>,
    a1: Arc<dyn ExecutionPlan>,
    a2: Arc<dyn ExecutionPlan>,
    central: Arc<dyn ExecutionPlan>,
    hub_b: Arc<dyn ExecutionPlan>,
    b1: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let left = join(hub_a, a1, &[(1, 0)]);
    let left = join(left, a2, &[(2, 0)]);
    // hub_a's key column is 0; the central relation joins on its first column.
    let left = join(left, central, &[(0, 0)]);
    let right = join(hub_b, b1, &[(1, 0)]);
    // The central relation's second column is the last of `left`.
    join(left, right, &[(9, 0)])
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
