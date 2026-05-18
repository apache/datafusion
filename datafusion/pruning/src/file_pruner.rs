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

//! File-level pruning based on partition values and file-level statistics

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{FieldRef, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::{
    Column as LogColumn, Result, ScalarValue, internal_datafusion_err,
    pruning::{PrunableStatistics, PruningStatistics},
};
use datafusion_datasource::PartitionedFile;
use datafusion_expr_common::statistics::{
    SatisfiedStatistics, StatisticsRequest, StatisticsValue,
};
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, snapshot_generation};
use datafusion_physical_plan::metrics::Count;
use log::debug;

use crate::build_pruning_predicate;

/// Prune based on file-level statistics.
///
/// Note: Partition column pruning is handled earlier via `replace_columns_with_literals`
/// which substitutes partition column references with their literal values before
/// the predicate reaches this pruner.
pub struct FilePruner {
    predicate_generation: Option<u64>,
    predicate: Arc<dyn PhysicalExpr>,
    /// Schema used for pruning (the logical file schema).
    file_schema: SchemaRef,
    /// Pruning statistics adapter. Either a [`PrunableStatistics`] view
    /// of the file's dense `Statistics`, or a [`SparseFilePruningStats`]
    /// that hits the sparse [`PartitionedFile::satisfied_stats`] map
    /// directly. Boxed so we can dispatch between the two without forcing
    /// callers to densify a sparse map ahead of time.
    file_stats_pruning: Box<dyn PruningStatistics + Send + Sync>,
    predicate_creation_errors: Count,
}

impl FilePruner {
    #[deprecated(
        since = "52.0.0",
        note = "Use `try_new` instead which returns None if no statistics are available"
    )]
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(
        predicate: Arc<dyn PhysicalExpr>,
        logical_file_schema: &SchemaRef,
        _partition_fields: Vec<FieldRef>,
        partitioned_file: PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Result<Self> {
        Self::try_new(
            predicate,
            logical_file_schema,
            &partitioned_file,
            predicate_creation_errors,
        )
        .ok_or_else(|| {
            internal_datafusion_err!(
                "FilePruner::new called on a file without statistics: {:?}",
                partitioned_file
            )
        })
    }

    /// Create a new file pruner if statistics are available.
    ///
    /// Prefers the dense [`PartitionedFile::statistics`] when present, and
    /// falls back to the sparse [`PartitionedFile::satisfied_stats`] map.
    /// In the sparse case the accessors look up each
    /// [`StatisticsRequest`] by key and lazily materialize the
    /// single-row arrays the pruning predicate needs — no
    /// densify-then-throw-away. Returns `None` if neither source has
    /// any stats.
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        file_schema: &SchemaRef,
        partitioned_file: &PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Option<Self> {
        let file_stats_pruning: Box<dyn PruningStatistics + Send + Sync> =
            if let Some(s) = &partitioned_file.statistics {
                Box::new(PrunableStatistics::new(
                    vec![Arc::clone(s)],
                    Arc::clone(file_schema),
                ))
            } else if let Some(sparse) = &partitioned_file.satisfied_stats {
                Box::new(SparseFilePruningStats {
                    sparse: Arc::clone(sparse),
                    schema: Arc::clone(file_schema),
                })
            } else {
                return None;
            };
        Some(Self {
            predicate_generation: None,
            predicate,
            file_schema: Arc::clone(file_schema),
            file_stats_pruning,
            predicate_creation_errors,
        })
    }

    pub fn should_prune(&mut self) -> Result<bool> {
        // Check if the predicate has changed since last invocation by tracking
        // its "generation". Dynamic filter expressions can change their values
        // during query execution, so we use generation tracking to detect when
        // the predicate has been updated and needs to be rebuilt.
        //
        // If the generation hasn't changed, we can skip rebuilding the pruning
        // predicate, which is an expensive operation involving expression analysis.
        let new_generation = snapshot_generation(&self.predicate);
        if let Some(current_generation) = self.predicate_generation.as_mut() {
            if *current_generation == new_generation {
                return Ok(false);
            }
            *current_generation = new_generation;
        } else {
            self.predicate_generation = Some(new_generation);
        }
        let pruning_predicate = build_pruning_predicate(
            Arc::clone(&self.predicate),
            &self.file_schema,
            &self.predicate_creation_errors,
        );
        let Some(pruning_predicate) = pruning_predicate else {
            return Ok(false);
        };
        match pruning_predicate.prune(&*self.file_stats_pruning) {
            Ok(values) => {
                assert!(values.len() == 1);
                // We expect a single container -> if all containers are false skip this file
                if values.into_iter().all(|v| !v) {
                    return Ok(true);
                }
            }
            // Stats filter array could not be built, so we can't prune
            Err(e) => {
                debug!("Ignoring error building pruning predicate for file: {e}");
                self.predicate_creation_errors.add(1);
            }
        }

        Ok(false)
    }
}

/// `PruningStatistics` adapter backed by a sparse
/// `HashMap<StatisticsRequest, StatisticsValue>`.
///
/// Each accessor builds the corresponding [`StatisticsRequest`] for the
/// column it was asked about, looks it up in the map, and converts the
/// result into a single-element [`ArrayRef`]. The 1-row arrays are only
/// ever materialized for columns the pruning predicate actually touches —
/// the long-lived footprint is the sparse map itself, which only contains
/// entries the provider answered.
struct SparseFilePruningStats {
    sparse: Arc<SatisfiedStatistics>,
    schema: SchemaRef,
}

impl SparseFilePruningStats {
    fn lookup_scalar(&self, req: &StatisticsRequest) -> Option<ScalarValue> {
        match self.sparse.get(req)? {
            StatisticsValue::Scalar(Precision::Exact(v))
            | StatisticsValue::Scalar(Precision::Inexact(v)) => Some(v.clone()),
            _ => None,
        }
    }

    /// Single-row Min/Max array, with a typed-null fallback for absent
    /// entries so the array has a stable schema across calls.
    fn min_or_max(&self, field: &FieldRef, req: &StatisticsRequest) -> Option<ArrayRef> {
        let v = self.lookup_scalar(req)?;
        v.to_array_of_size(1).ok().or_else(|| {
            ScalarValue::try_from(field.data_type())
                .ok()
                .and_then(|n| n.to_array_of_size(1).ok())
        })
    }
}

impl PruningStatistics for SparseFilePruningStats {
    fn min_values(&self, column: &datafusion_common::Column) -> Option<ArrayRef> {
        let field = self.schema.field_with_name(&column.name).ok()?;
        let field = Arc::new(field.clone());
        self.min_or_max(
            &field,
            &StatisticsRequest::Min(LogColumn::new_unqualified(&column.name)),
        )
    }

    fn max_values(&self, column: &datafusion_common::Column) -> Option<ArrayRef> {
        let field = self.schema.field_with_name(&column.name).ok()?;
        let field = Arc::new(field.clone());
        self.min_or_max(
            &field,
            &StatisticsRequest::Max(LogColumn::new_unqualified(&column.name)),
        )
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, column: &datafusion_common::Column) -> Option<ArrayRef> {
        let v = self.lookup_scalar(&StatisticsRequest::NullCount(
            LogColumn::new_unqualified(&column.name),
        ))?;
        let n = scalar_to_u64(&v)?;
        ScalarValue::UInt64(Some(n)).to_array_of_size(1).ok()
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        let v = self.lookup_scalar(&StatisticsRequest::RowCount)?;
        let n = scalar_to_u64(&v)?;
        ScalarValue::UInt64(Some(n)).to_array_of_size(1).ok()
    }

    fn contained(
        &self,
        _column: &datafusion_common::Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        // Bloom-filter-style membership isn't representable in the
        // current StatisticsRequest set. A `Contained(column, values)`
        // request kind could be added later if a provider can answer it.
        None
    }
}

fn scalar_to_u64(v: &ScalarValue) -> Option<u64> {
    match v {
        ScalarValue::UInt64(Some(n)) => Some(*n),
        ScalarValue::Int64(Some(n)) if *n >= 0 => Some(*n as u64),
        ScalarValue::UInt32(Some(n)) => Some(*n as u64),
        ScalarValue::Int32(Some(n)) if *n >= 0 => Some(*n as u64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Column as LogColumn;
    use datafusion_physical_expr::expressions::lit;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column as PhysCol};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn make_file_with_sparse_stats(min: i64, max: i64) -> PartitionedFile {
        let mut map = HashMap::new();
        let col = LogColumn::new_unqualified("x");
        map.insert(
            StatisticsRequest::Min(col.clone()),
            StatisticsValue::Scalar(Precision::Exact(ScalarValue::Int64(Some(min)))),
        );
        map.insert(
            StatisticsRequest::Max(col),
            StatisticsValue::Scalar(Precision::Exact(ScalarValue::Int64(Some(max)))),
        );
        PartitionedFile::new("dummy", 100).with_satisfied_stats(Arc::new(map))
    }

    fn metric_count() -> Count {
        datafusion_physical_plan::metrics::MetricBuilder::new(
            &datafusion_physical_plan::metrics::ExecutionPlanMetricsSet::new(),
        )
        .global_counter("e")
    }

    #[test]
    fn file_pruner_uses_sparse_stats_when_dense_absent() {
        let schema = make_schema();
        // file with x in [10, 20]
        let file = make_file_with_sparse_stats(10, 20);

        // predicate: x > 100 — file can be pruned
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysCol::new("x", 0)),
            datafusion_expr::Operator::Gt,
            lit(ScalarValue::Int64(Some(100))),
        ));

        let mut pruner = FilePruner::try_new(predicate, &schema, &file, metric_count())
            .expect("should construct from satisfied_stats");

        assert!(
            pruner.should_prune().expect("prune ok"),
            "x > 100 should prune a file whose x in [10, 20]"
        );
    }

    #[test]
    fn file_pruner_does_not_prune_when_predicate_overlaps() {
        let schema = make_schema();
        let file = make_file_with_sparse_stats(10, 20);

        // predicate: x > 15 — overlap, should NOT prune
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysCol::new("x", 0)),
            datafusion_expr::Operator::Gt,
            lit(ScalarValue::Int64(Some(15))),
        ));

        let mut pruner = FilePruner::try_new(predicate, &schema, &file, metric_count())
            .expect("should construct");

        assert!(
            !pruner.should_prune().expect("prune ok"),
            "x > 15 should NOT prune — overlap with [10, 20]"
        );
    }

    #[test]
    fn file_pruner_returns_none_without_any_stats() {
        let schema = make_schema();
        let file = PartitionedFile::new("dummy", 100); // neither dense nor sparse

        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysCol::new("x", 0)),
            datafusion_expr::Operator::Gt,
            lit(ScalarValue::Int64(Some(0))),
        ));

        assert!(
            FilePruner::try_new(predicate, &schema, &file, metric_count()).is_none(),
            "no stats at all -> no pruner"
        );
    }
}
