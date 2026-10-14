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

//! Sources and sinks: `DataSourceExec` over memory and over files of each
//! format, `LazyMemoryExec`, `StreamingTableExec`, `DataSinkExec`, and the
//! plans of `DELETE` and `UPDATE` on a `MemTable`.
//!
//! A source is built from the rows of a generated input, a
//! `MockSourceExec`, rather than from data of its own, so that it is checked
//! with every partition layout, statistics precision, ordering and
//! partitioning of the profiles. Each source declares what the input
//! declares, as far as it can: its ordering, its hash partitioning, and, for
//! files, the statistics the input reports for each of its partitions,
//! which become the statistics of one file per partition. The files are not
//! written, so the file sources run the static checks only.

use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::{
    ArrowSource, CsvSource, JsonSource, ParquetSource,
};
use datafusion::prelude::SessionContext;
use datafusion_catalog::{MemTable, TableProvider};
use datafusion_common::{Result, ScalarValue, internal_datafusion_err};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::memory::{MemSink, MemorySourceConfig};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{PartitionedFile, TableSchema, compute_all_files_statistics};
use datafusion_execution::TaskContext;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{col as logical_col, lit};
use datafusion_physical_expr::equivalence::project_orderings;
use datafusion_physical_expr::expressions::{Literal, col};
use datafusion_physical_expr::{
    LexOrdering, LexRequirement, Partitioning, PhysicalExpr, PhysicalSortRequirement,
};
use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::{
    ExecutionPlan, SendableRecordBatchStream, StatisticsArgs, StatisticsContext,
};
use datafusion_property_tests::physical_plan::CheckKind;
use datafusion_property_tests::physical_plan::checks::all_checks;
use datafusion_property_tests::physical_plan::fixtures::MockSourceExec;
use datafusion_property_tests::physical_plan::harness::PlanFactory;
use futures::executor::block_on;
use parking_lot::RwLock;

use super::{FETCH, Plan, one_input, schema, spec};

/// The sources and sinks under test
pub(super) fn source_plans() -> Result<Vec<PlanFactory>> {
    Ok(vec![
        one_input("DataSourceExec MemorySourceConfig", spec(), |input| {
            let input = Input::of(&input)?;
            let config =
                MemorySourceConfig::try_new(input.partitions(), input.schema(), None)?
                    .try_with_sort_information(input.orderings())?;
            Ok(DataSourceExec::from_data_source(config))
        }),
        one_input(
            "DataSourceExec MemorySourceConfig with projection",
            spec(),
            |input| {
                let input = Input::of(&input)?;
                let projection = Some(input.projection());
                let config = MemorySourceConfig::try_new(
                    input.partitions(),
                    input.schema(),
                    projection,
                )?
                .try_with_sort_information(input.orderings())?;
                Ok(DataSourceExec::from_data_source(config))
            },
        ),
        one_input(
            "DataSourceExec MemorySourceConfig with limit",
            spec(),
            |input| {
                let input = Input::of(&input)?;
                let config = MemorySourceConfig::try_new(
                    input.partitions(),
                    input.schema(),
                    None,
                )?
                .try_with_sort_information(input.orderings())?
                .with_limit(Some(FETCH));
                Ok(DataSourceExec::from_data_source(config))
            },
        ),
        // VALUES (1, true, 'x'), (2, false, NULL)
        PlanFactory::new("DataSourceExec values", vec![], |_| {
            let literal = |value: ScalarValue| -> Arc<dyn PhysicalExpr> {
                Arc::new(Literal::new(value))
            };
            let rows = vec![
                vec![
                    literal(ScalarValue::Int32(Some(1))),
                    literal(ScalarValue::Boolean(Some(true))),
                    literal(ScalarValue::Utf8(Some("x".to_string()))),
                ],
                vec![
                    literal(ScalarValue::Int32(Some(2))),
                    literal(ScalarValue::Boolean(Some(false))),
                    literal(ScalarValue::Utf8(None)),
                ],
            ];
            Ok(MemorySourceConfig::try_new_as_values(schema(), rows)? as Plan)
        }),
        files("DataSourceExec Parquet", |schema| {
            Arc::new(ParquetSource::new(schema))
        }),
        files("DataSourceExec CSV", |schema| {
            Arc::new(CsvSource::new(schema))
        }),
        files("DataSourceExec JSON", |schema| {
            Arc::new(JsonSource::new(schema))
        }),
        files("DataSourceExec Arrow", |schema| {
            Arc::new(ArrowSource::new_file_source(schema))
        }),
        static_checks_only(one_input(
            "DataSourceExec Parquet with projection and limit",
            spec(),
            |input| {
                let input = Input::of(&input)?;
                let config = input
                    .file_scan(Arc::new(ParquetSource::new(input.schema())))?
                    .with_projection_indices(Some(input.projection()))?
                    .with_limit(Some(FETCH));
                Ok(DataSourceExec::from_data_source(config.build()))
            },
        )),
        one_input("LazyMemoryExec", spec(), |input| {
            let input = Input::of(&input)?;
            let mut exec = LazyMemoryExec::try_new(input.schema(), input.generators())?;
            for ordering in input.orderings() {
                exec.add_ordering(ordering);
            }
            if let Some(partitioning) = input.hash_partitioning() {
                exec.try_set_partitioning(partitioning)?;
            }
            Ok(Arc::new(exec))
        }),
        one_input("LazyMemoryExec with projection", spec(), |input| {
            let input = Input::of(&input)?;
            let exec = LazyMemoryExec::try_new(input.schema(), input.generators())?
                .with_projection(Some(input.projection()));
            Ok(Arc::new(exec))
        }),
        one_input("StreamingTableExec", spec(), |input| {
            let input = Input::of(&input)?;
            let exec = StreamingTableExec::try_new(
                input.schema(),
                input.partition_streams(),
                None,
                input.orderings(),
                false,
                None,
            )?;
            Ok(match input.hash_partitioning() {
                Some(partitioning) => {
                    Arc::new(exec.with_output_partitioning(partitioning)?)
                }
                None => Arc::new(exec),
            })
        }),
        one_input(
            "StreamingTableExec with projection and limit",
            spec(),
            |input| {
                let input = Input::of(&input)?;
                let projection = input.projection();
                let projected = Arc::new(input.schema().project(&projection)?);
                Ok(Arc::new(StreamingTableExec::try_new(
                    input.schema(),
                    input.partition_streams(),
                    Some(&projection),
                    project_orderings(&input.orderings(), &projected),
                    false,
                    Some(FETCH),
                )?))
            },
        ),
        one_input("StreamingTableExec infinite", spec(), |input| {
            let input = Input::of(&input)?;
            Ok(Arc::new(StreamingTableExec::try_new(
                input.schema(),
                input.partition_streams(),
                None,
                input.orderings(),
                true,
                None,
            )?))
        }),
        // INSERT INTO a `MemTable`
        one_input("DataSinkExec MemSink", spec(), |input| {
            let table = MemTable::try_new(input.schema(), vec![vec![]])?;
            let state = SessionContext::new().state();
            block_on(table.insert_into(&state, input, InsertOp::Append))
        }),
        // A sink that requires its input to be sorted
        one_input("DataSinkExec with a sort order", spec(), |input| {
            let schema = input.schema();
            let partition = Arc::new(tokio::sync::RwLock::new(vec![]));
            let sink = MemSink::try_new(vec![partition], Arc::clone(&schema))?;
            let sort_order = LexRequirement::new(vec![PhysicalSortRequirement::new(
                col("a", &schema)?,
                None,
            )]);
            Ok(Arc::new(DataSinkExec::new(
                input,
                Arc::new(sink),
                sort_order,
            )))
        }),
        // DELETE FROM t WHERE a > 5
        one_input("MemDeleteExec", spec(), |input| {
            let table = Input::of(&input)?.mem_table()?;
            let state = SessionContext::new().state();
            block_on(table.delete_from(&state, vec![logical_col("a").gt(lit(5))]))
        }),
        // UPDATE t SET a = a + 1 WHERE b
        one_input("MemUpdateExec", spec(), |input| {
            let table = Input::of(&input)?.mem_table()?;
            let state = SessionContext::new().state();
            let assignments = vec![("a".to_string(), logical_col("a") + lit(1))];
            block_on(table.update(&state, assignments, vec![logical_col("b")]))
        }),
    ])
}

/// A `DataSourceExec` over one file of the format of `source` per partition
/// of the input
fn files(name: &str, source: fn(TableSchema) -> Arc<dyn FileSource>) -> PlanFactory {
    static_checks_only(one_input(name, spec(), move |input| {
        let input = Input::of(&input)?;
        let config = input.file_scan(source(TableSchema::from(input.schema())))?;
        Ok(DataSourceExec::from_data_source(config.build()))
    }))
}

/// Allow every check that executes the plan of `factory`, a source over files
/// that are not written
fn static_checks_only(factory: PlanFactory) -> PlanFactory {
    all_checks()
        .into_iter()
        .filter(|check| check.kind != CheckKind::Static)
        .fold(factory, |factory, check| {
            factory.allow(
                check.name,
                "the files are not written, so the plan cannot run",
            )
        })
}

/// The generated input a source is built from
struct Input<'a> {
    source: &'a MockSourceExec,
}

impl<'a> Input<'a> {
    fn of(input: &'a Plan) -> Result<Self> {
        let source = input
            .downcast_ref::<MockSourceExec>()
            .ok_or_else(|| internal_datafusion_err!("the input is a MockSourceExec"))?;
        Ok(Self { source })
    }

    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    fn partitions(&self) -> &[Vec<RecordBatch>] {
        self.source.partitions()
    }

    /// The orderings the input declares
    fn orderings(&self) -> Vec<LexOrdering> {
        self.source
            .properties()
            .output_ordering()
            .cloned()
            .into_iter()
            .collect()
    }

    /// The hash partitioning the input declares, if any
    fn hash_partitioning(&self) -> Option<Partitioning> {
        let partitioning = self.source.properties().output_partitioning();
        matches!(partitioning, Partitioning::Hash(..)).then(|| partitioning.clone())
    }

    /// Every column but the second, so that a projection drops a column and
    /// moves the columns after it
    fn projection(&self) -> Vec<usize> {
        (0..self.schema().fields().len())
            .filter(|i| *i != 1)
            .collect()
    }

    /// A scan of one file per partition of the input, each with the
    /// statistics the input reports for the partition, and the ordering and
    /// hash partitioning the input declares
    fn file_scan(&self, source: Arc<dyn FileSource>) -> Result<FileScanConfigBuilder> {
        let extension = source.file_type();
        let groups = (0..self.partitions().len())
            .map(|p| {
                let statistics = StatisticsContext::new().compute(
                    self.source,
                    &StatisticsArgs::new().with_partition(Some(p)),
                )?;
                let file =
                    PartitionedFile::new(format!("data/part-{p}.{extension}"), 1024)
                        .with_statistics(statistics);
                Ok(FileGroup::new(vec![file]))
            })
            .collect::<Result<Vec<_>>>()?;
        let inexact = !matches!(
            StatisticsContext::new()
                .compute(self.source, &StatisticsArgs::new())?
                .num_rows,
            datafusion_common::stats::Precision::Exact(_)
        );
        let (groups, statistics) =
            compute_all_files_statistics(groups, self.schema(), true, inexact)?;
        Ok(
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
                .with_file_groups(groups)
                .with_statistics(statistics)
                .with_output_ordering(self.orderings())
                .with_output_partitioning(self.hash_partitioning()),
        )
    }

    /// One generator per partition of the input, serving its batches
    fn generators(&self) -> Vec<Arc<RwLock<dyn LazyBatchGenerator>>> {
        self.partitions()
            .iter()
            .map(|batches| {
                Arc::new(RwLock::new(Batches::new(batches.clone())))
                    as Arc<RwLock<dyn LazyBatchGenerator>>
            })
            .collect()
    }

    /// One stream per partition of the input, serving its batches
    fn partition_streams(&self) -> Vec<Arc<dyn PartitionStream>> {
        self.partitions()
            .iter()
            .map(|batches| {
                Arc::new(BatchStream {
                    schema: self.schema(),
                    batches: batches.clone(),
                }) as Arc<dyn PartitionStream>
            })
            .collect()
    }

    /// A `MemTable` with the partitions of the input
    fn mem_table(&self) -> Result<MemTable> {
        MemTable::try_new(self.schema(), self.partitions().to_vec())
    }
}

/// A [`LazyBatchGenerator`] that serves fixed batches
#[derive(Debug)]
struct Batches {
    batches: Vec<RecordBatch>,
    next: usize,
}

impl Batches {
    fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches, next: 0 }
    }
}

impl fmt::Display for Batches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Batches: {}", self.batches.len())
    }
}

impl LazyBatchGenerator for Batches {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let batch = self.batches.get(self.next).cloned();
        self.next += 1;
        Ok(batch)
    }

    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        Arc::new(RwLock::new(Batches::new(self.batches.clone())))
    }
}

/// A [`PartitionStream`] that serves fixed batches
#[derive(Debug)]
struct BatchStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl PartitionStream for BatchStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches = self.batches.clone().into_iter().map(Ok);
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(batches),
        ))
    }
}
