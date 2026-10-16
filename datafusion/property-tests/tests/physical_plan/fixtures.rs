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

//! Tests that generated sources match their spec and report true properties.
//! Every other test relies on this.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, AsArray, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{
    DataType, Field, Fields, Schema, SchemaRef, TimeUnit, UInt64Type,
};
use datafusion_common::ScalarValue;
use datafusion_common::stats::Precision;
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
use datafusion_physical_expr::{
    AcrossPartitions, ConstExpr, LexOrdering, Partitioning, PhysicalSortExpr,
};
use datafusion_physical_plan::execution_plan::Boundedness;
use datafusion_physical_plan::{
    ExecutionPlan, SendableRecordBatchStream, StatisticsArgs, StatisticsContext,
    displayable,
};
use datafusion_property_tests::physical_plan::fixtures::{
    BatchLayout, COPY_SUFFIX, ConstantValues, MockSourceExec, ROW_ID_COLUMN, SourceSpec,
    StatisticsPrecision, StreamBehavior,
};
use datafusion_property_tests::physical_plan::{PlanChecker, oracle};
use futures::StreamExt;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Float64, true),
    ]))
}

fn rows_per_partition(source: &MockSourceExec) -> Vec<usize> {
    source
        .partitions()
        .iter()
        .map(|batches| batches.iter().map(RecordBatch::num_rows).sum())
        .collect()
}

fn all_batches(source: &MockSourceExec) -> Vec<RecordBatch> {
    source.partitions().iter().flatten().cloned().collect()
}

fn null_count(source: &MockSourceExec, column: usize) -> usize {
    all_batches(source)
        .iter()
        .map(|batch| batch.column(column).null_count())
        .sum()
}

#[test]
fn generation_is_deterministic() {
    let spec = SourceSpec::new(schema()).with_partition_rows(&[20, 30]);
    let first = spec.clone().with_seed(7).build().unwrap();
    let second = spec.clone().with_seed(7).build().unwrap();
    let other = spec.with_seed(8).build().unwrap();
    assert_eq!(first.partitions(), second.partitions());
    assert_ne!(first.partitions(), other.partitions());
}

#[test]
fn partition_rows_and_batch_layout() {
    let source = SourceSpec::new(schema())
        .with_partition_rows(&[25, 0, 7])
        .with_batch_layout(BatchLayout::Fixed(10))
        .build()
        .unwrap();
    assert_eq!(rows_per_partition(&source), vec![25, 0, 7]);
    let sizes: Vec<Vec<usize>> = source
        .partitions()
        .iter()
        .map(|batches| batches.iter().map(RecordBatch::num_rows).collect())
        .collect();
    assert_eq!(sizes, vec![vec![10, 10, 5], vec![], vec![7]]);
    assert_eq!(
        source.properties().output_partitioning().partition_count(),
        3
    );
}

#[test]
fn random_batch_layout_with_empty_batches() {
    let source = SourceSpec::new(schema())
        .with_partition_rows(&[200, 0])
        .with_batch_layout(BatchLayout::Random { max_rows: 5 })
        .build()
        .unwrap();
    assert_eq!(rows_per_partition(&source), vec![200, 0]);
    let sizes: Vec<usize> = all_batches(&source)
        .iter()
        .map(RecordBatch::num_rows)
        .collect();
    assert!(sizes.iter().all(|n| *n <= 5), "{sizes:?}");
    assert!(sizes.contains(&0), "{sizes:?}");
}

#[test]
fn nulls_only_in_nullable_fields() {
    let spec = SourceSpec::new(schema()).with_partition_rows(&[500]);
    let source = spec.clone().with_null_fraction(0.5).build().unwrap();
    assert_eq!(null_count(&source, 0), 0);
    assert!(null_count(&source, 1) > 100);
    assert!(null_count(&source, 2) > 100);

    let source = spec.with_null_fraction(0.0).build().unwrap();
    assert_eq!(null_count(&source, 1), 0);
}

#[test]
fn distinct_values() {
    let source = SourceSpec::new(schema())
        .with_partition_rows(&[500])
        .with_distinct_values(3)
        .build()
        .unwrap();
    let stats =
        oracle::exact_statistics(&source.schema(), &all_batches(&source)).unwrap();
    assert_eq!(
        stats.column_statistics[0].distinct_count,
        Precision::Exact(3)
    );
}

#[test]
fn sums_of_integer_and_decimal_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int32, true),
        Field::new("d", DataType::Decimal128(10, 2), false),
        Field::new("f", DataType::Float64, false),
        Field::new("big", DataType::Int64, false),
        Field::new("nulls", DataType::Int32, true),
    ]));
    let batch = |i: Vec<Option<i32>>, d: Vec<i128>, f: Vec<f64>, big: Vec<i64>| {
        let rows = i.len();
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(i)),
                Arc::new(
                    Decimal128Array::from(d)
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(Float64Array::from(f)),
                Arc::new(Int64Array::from(big)),
                Arc::new(Int32Array::from(vec![None; rows])),
            ],
        )
        .unwrap()
    };
    let batches = [
        batch(
            vec![Some(1), None],
            vec![150, 250],
            vec![0.5, 1.5],
            vec![i64::MAX, 0],
        ),
        batch(vec![Some(2)], vec![100], vec![2.0], vec![1]),
    ];
    let stats = oracle::exact_statistics(&schema, &batches).unwrap();
    let sums: Vec<_> = stats
        .column_statistics
        .iter()
        .map(|column| column.sum_value.clone())
        .collect();
    // Integers are summed in the wider type of SQL `SUM`, and the precision of
    // a decimal sum grows with each addition. Floating point sums, sums that
    // overflow, and sums of columns without non-null values are not known.
    assert_eq!(sums[0], Precision::Exact(ScalarValue::Int64(Some(3))));
    assert!(
        matches!(
            sums[1],
            Precision::Exact(ScalarValue::Decimal128(Some(500), _, 2))
        ),
        "{sums:?}"
    );
    assert_eq!(sums[2..], vec![Precision::Absent; 3]);
}

#[test]
fn ordering_is_applied() {
    let schema = schema();
    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new(
            col("b", &schema).unwrap(),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        ),
        PhysicalSortExpr::new_default(col("a", &schema).unwrap()),
    ])
    .unwrap();
    let source = SourceSpec::new(Arc::clone(&schema))
        .with_partition_rows(&[100, 50])
        .with_distinct_values(4)
        .with_ordering(ordering.clone())
        .build()
        .unwrap();
    for batches in source.partitions() {
        assert_eq!(
            oracle::first_unsorted_row(batches, &ordering).unwrap(),
            None
        );
    }
    assert_eq!(source.properties().output_ordering(), Some(&ordering));
}

#[test]
fn hash_partitioning_is_applied() {
    let schema = schema();
    let exprs = vec![col("a", &schema).unwrap()];
    let source = SourceSpec::new(schema)
        .with_hash_partitioning(exprs.clone(), 4, 300)
        .build()
        .unwrap();
    assert_eq!(rows_per_partition(&source).iter().sum::<usize>(), 300);
    for (p, batches) in source.partitions().iter().enumerate() {
        assert_eq!(
            oracle::rows_outside_hash_partition(batches, &exprs, 4, p).unwrap(),
            0
        );
    }
    assert!(matches!(
        source.properties().output_partitioning(),
        Partitioning::Hash(_, 4)
    ));
}

#[test]
fn row_ids_are_unique_and_increasing() {
    let schema = schema();
    let spec = SourceSpec::new(Arc::clone(&schema))
        .with_partition_rows(&[20, 0, 15])
        .with_ordering(
            LexOrdering::new(vec![PhysicalSortExpr::new_default(
                col("a", &schema).unwrap(),
            )])
            .unwrap(),
        )
        .with_row_ids(1000);
    let source = spec.build().unwrap();
    assert_eq!(source.schema(), spec.schema());
    assert_eq!(source.schema().field(3).name(), ROW_ID_COLUMN);

    let ids: Vec<u64> = all_batches(&source)
        .iter()
        .flat_map(|batch| {
            batch
                .column(3)
                .as_primitive::<UInt64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, (1000..1035).collect::<Vec<_>>());
}

#[test]
fn row_id_ordering() {
    let schema = schema();
    let spec = SourceSpec::new(Arc::clone(&schema))
        .with_hash_partitioning(vec![col("a", &schema).unwrap()], 3, 100)
        .with_row_ids(1000)
        .with_row_id_ordering();
    let source = spec.build().unwrap();
    let ordering = source.properties().output_ordering().unwrap().clone();
    assert_eq!(ordering.to_string(), format!("{ROW_ID_COLUMN}@3 ASC"));
    for batches in source.partitions() {
        assert_eq!(
            oracle::first_unsorted_row(batches, &ordering).unwrap(),
            None
        );
    }

    // Another ordering is declared instead, and without row ids there is no
    // ordering
    let ordering_on_a = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("a", &schema).unwrap(),
    )])
    .unwrap();
    let source = spec.with_ordering(ordering_on_a.clone()).build().unwrap();
    assert_eq!(source.properties().output_ordering(), Some(&ordering_on_a));
    let source = SourceSpec::new(schema)
        .with_row_id_ordering()
        .build()
        .unwrap();
    assert_eq!(source.properties().output_ordering(), None);
}

#[test]
fn statistics_are_computed_from_the_data() {
    let spec = SourceSpec::new(schema()).with_partition_rows(&[40, 60]);
    let source = spec.build().unwrap();
    let context = StatisticsContext::new();
    let overall = context.compute(&source, &StatisticsArgs::new()).unwrap();
    let expected =
        oracle::exact_statistics(&source.schema(), &all_batches(&source)).unwrap();
    assert_eq!(overall.as_ref(), &expected);
    assert_eq!(overall.num_rows, Precision::Exact(100));

    let partition = context
        .compute(&source, &StatisticsArgs::new().with_partition(Some(1)))
        .unwrap();
    assert_eq!(partition.num_rows, Precision::Exact(60));

    let inexact = spec
        .clone()
        .with_statistics_precision(StatisticsPrecision::Inexact)
        .build()
        .unwrap();
    let stats = context.compute(&inexact, &StatisticsArgs::new()).unwrap();
    assert_eq!(stats.num_rows, Precision::Inexact(100));

    let absent = spec
        .with_statistics_precision(StatisticsPrecision::Absent)
        .build()
        .unwrap();
    let stats = context.compute(&absent, &StatisticsArgs::new()).unwrap();
    assert_eq!(stats.num_rows, Precision::Absent);
}

#[test]
fn generated_sources_pass_every_check() {
    let schema = schema();
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("a", &schema).unwrap(),
    )])
    .unwrap();
    let specs = [
        SourceSpec::new(Arc::clone(&schema)),
        SourceSpec::new(Arc::clone(&schema)).with_partition_rows(&[]),
        SourceSpec::new(Arc::clone(&schema)).with_partition_rows(&[0, 0]),
        SourceSpec::new(Arc::clone(&schema))
            .with_partition_rows(&[30, 5, 80])
            .with_batch_layout(BatchLayout::Random { max_rows: 3 })
            .with_ordering(ordering),
        SourceSpec::new(Arc::clone(&schema)).with_hash_partitioning(
            vec![col("b", &schema).unwrap()],
            3,
            100,
        ),
        SourceSpec::new(Arc::clone(&schema))
            .with_partition_rows(&[10, 10])
            .with_row_ids(0)
            .with_statistics_precision(StatisticsPrecision::Inexact),
        SourceSpec::new(Arc::clone(&schema))
            .with_partition_rows(&[10, 0, 20])
            .with_constant("a", ConstantValues::Uniform)
            .with_constant("c", ConstantValues::PerPartition),
        SourceSpec::new(Arc::clone(&schema))
            .with_hash_partitioning(vec![col("a", &schema).unwrap()], 3, 100)
            .with_constant("a", ConstantValues::PerPartition),
        SourceSpec::new(Arc::clone(&schema))
            .with_hash_partitioning(vec![col("a", &schema).unwrap()], 3, 100)
            .with_copy("a")
            .with_copy("c")
            .with_row_ids(0),
        SourceSpec::new(Arc::clone(&schema))
            .with_partition_rows(&[10, 0, 20])
            .with_row_ids(0)
            .with_row_id_ordering(),
    ];
    for spec in specs {
        let source = spec.build_arc().unwrap();
        PlanChecker::new().check(&source).unwrap().assert_clean();
    }
}

#[test]
fn every_supported_type_can_be_generated() {
    let types = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Date32,
        DataType::Date64,
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("+01:00".into())),
        DataType::Decimal128(10, 2),
        DataType::new_list(DataType::Int32, false),
        DataType::new_large_list(DataType::Utf8, true),
        DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::new_list(DataType::Utf8, true), true),
        ])),
    ];
    let fields: Vec<Field> = types
        .iter()
        .enumerate()
        .map(|(i, data_type)| Field::new(format!("c{i}"), data_type.clone(), true))
        .collect();
    let source = SourceSpec::new(Arc::new(Schema::new(fields)))
        .with_partition_rows(&[50])
        .build()
        .unwrap();
    for batch in all_batches(&source) {
        for (column, data_type) in batch.columns().iter().zip(&types) {
            assert_eq!(column.data_type(), data_type);
        }
    }
}

#[test]
fn nested_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l", DataType::new_list(DataType::Int32, false), true),
        Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Utf8, true),
            ])),
            true,
        ),
    ]));
    let spec = SourceSpec::new(Arc::clone(&schema)).with_partition_rows(&[30, 0, 20]);
    let source = spec.build().unwrap();

    // The list of index k has k % 4 elements, k, k + 1 and so on, and every
    // field of a struct has the value of the struct's index
    for batch in all_batches(&source) {
        let lists = batch.column(0).as_list::<i32>();
        let structs = batch.column(1).as_struct();
        for row in 0..batch.num_rows() {
            if lists.is_valid(row) {
                let list = lists.value(row);
                let values = list.as_primitive::<arrow::datatypes::Int32Type>();
                if let Some(first) = values.iter().next() {
                    let first = first.unwrap();
                    let expected: Vec<i32> =
                        (first..first + (first % 4)).collect::<Vec<_>>();
                    assert_eq!(values.values().to_vec(), expected);
                }
            }
            if structs.is_valid(row) {
                let x = structs
                    .column(0)
                    .as_primitive::<arrow::datatypes::Int32Type>();
                let y = structs.column(1).as_string::<i32>();
                assert_eq!(y.value(row), format!("s{:03}", x.value(row)));
            }
        }
    }
    let stats = oracle::exact_statistics(&schema, &all_batches(&source)).unwrap();
    assert_eq!(stats.column_statistics[0].min_value, Precision::Absent);

    // Nested columns can be constant, copied and checked
    for spec in [
        spec.clone()
            .with_constant("l", ConstantValues::Uniform)
            .with_constant("s", ConstantValues::PerPartition),
        spec.clone().with_copy("l").with_copy("s").with_row_ids(0),
    ] {
        let source = spec.build_arc().unwrap();
        PlanChecker::new().check(&source).unwrap().assert_clean();
    }
}

#[test]
fn unsupported_type_is_an_error() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Binary, false)]));
    let error = SourceSpec::new(schema).build().unwrap_err();
    assert!(error.to_string().contains("cannot generate"), "{error}");
}

#[test]
fn constant_columns() {
    let source = SourceSpec::new(schema())
        .with_partition_rows(&[10, 0, 20, 5])
        .with_constant("a", ConstantValues::Uniform)
        .with_constant("c", ConstantValues::PerPartition)
        .build()
        .unwrap();
    let a = col("a", &schema()).unwrap();
    let c = col("c", &schema()).unwrap();
    let values = |expr| {
        source
            .partitions()
            .iter()
            .map(|batches| oracle::distinct_values(batches, expr).unwrap())
            .collect::<Vec<_>>()
    };

    // `a` has one value in every partition with rows, and declares it
    let a_values = values(&a);
    let value = a_values[0][0].clone();
    assert_eq!(
        a_values,
        vec![
            vec![value.clone()],
            vec![],
            vec![value.clone()],
            vec![value.clone()]
        ]
    );
    // `c` is nullable, but has no nulls, and a different value in each
    // partition with rows
    let c_values = values(&c);
    assert!(
        c_values.iter().all(|values| values.len() <= 1),
        "{c_values:?}"
    );
    assert!(c_values.iter().flatten().all(|value| !value.is_null()));
    assert_ne!(c_values[0], c_values[2]);
    assert_ne!(c_values[2], c_values[3]);
    assert_eq!(
        source.properties().equivalence_properties().constants(),
        vec![
            ConstExpr::new(a, AcrossPartitions::Uniform(Some(value))),
            ConstExpr::new(c, AcrossPartitions::Heterogeneous),
        ]
    );

    // Hash partitioned on a constant column, every row is in one partition
    let source = SourceSpec::new(schema())
        .with_hash_partitioning(vec![col("a", &schema()).unwrap()], 3, 60)
        .with_constant("a", ConstantValues::PerPartition)
        .build()
        .unwrap();
    let mut rows = rows_per_partition(&source);
    rows.sort_unstable();
    assert_eq!(rows, vec![0, 0, 60]);

    // An unknown column is an error
    let error = SourceSpec::new(schema())
        .with_constant("x", ConstantValues::Uniform)
        .build()
        .unwrap_err();
    assert!(error.to_string().contains("\"x\""), "{error}");
}

#[test]
fn copied_columns() {
    let source = SourceSpec::new(schema())
        .with_partition_rows(&[10, 0, 20])
        .with_copy("c")
        .with_copy("a")
        .with_copy("a")
        .with_row_ids(0)
        .build()
        .unwrap();
    // Copies come after the columns of the schema, in the order they were
    // asked for, and before the row ids
    let names: Vec<String> = source
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["a", "b", "c", "c__copy", "a__copy", ROW_ID_COLUMN]);
    let source_schema = source.schema();
    assert_eq!(source_schema.field(3).data_type(), &DataType::Float64);
    assert!(source_schema.field(3).is_nullable());

    // Each copy has the values of its column, and is declared equal to it
    let column = |name: &str| col(name, &source_schema).unwrap();
    let eq_group = source
        .properties()
        .equivalence_properties()
        .eq_group()
        .clone();
    for name in ["a", "c"] {
        let copy = column(&format!("{name}{COPY_SUFFIX}"));
        for batches in source.partitions() {
            assert_eq!(
                oracle::first_unequal_row(batches, &column(name), &copy).unwrap(),
                None
            );
        }
        assert!(eq_group.exprs_equal(&column(name), &copy), "{eq_group}");
    }

    // An unknown column is an error
    let error = SourceSpec::new(schema())
        .with_copy("x")
        .build()
        .unwrap_err();
    assert!(error.to_string().contains("\"x\""), "{error}");
}

#[test]
fn false_claims_are_rejected() {
    let schema = schema();
    let unsorted = SourceSpec::new(Arc::clone(&schema))
        .with_partition_rows(&[100, 100])
        .build()
        .unwrap();

    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("a", &schema).unwrap(),
    )])
    .unwrap();
    let error = unsorted
        .clone()
        .try_with_output_ordering(ordering)
        .unwrap_err();
    assert!(error.to_string().contains("is not sorted"), "{error}");

    let hash = Partitioning::Hash(vec![col("a", &schema).unwrap()], 2);
    let error = unsorted.clone().try_with_partitioning(hash).unwrap_err();
    assert!(error.to_string().contains("do not belong"), "{error}");

    let error = unsorted
        .clone()
        .try_with_partitioning(Partitioning::UnknownPartitioning(3))
        .unwrap_err();
    assert!(error.to_string().contains("has 3"), "{error}");

    let constant =
        ConstExpr::new(col("a", &schema).unwrap(), AcrossPartitions::Heterogeneous);
    let error = unsorted
        .clone()
        .try_with_constants(vec![constant])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a@0 is constant, but partition 0 has"),
        "{error}"
    );

    let a = col("a", &schema).unwrap();
    let a_plus_one = Arc::new(BinaryExpr::new(Arc::clone(&a), Operator::Plus, lit(1)));
    let error = unsorted
        .try_with_equalities(vec![(a, a_plus_one)])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a@0 and a@0 + 1 are 11 and 12 in row 0 of partition 0"),
        "{error}"
    );

    let other_schema =
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch = RecordBatch::new_empty(other_schema);
    let error = MockSourceExec::try_new(schema, vec![vec![batch]]).unwrap_err();
    assert!(error.to_string().contains("expected"), "{error}");
}

/// Two partitions of 10 rows, in batches of 4 rows: 3 batches each
fn small_spec() -> SourceSpec {
    SourceSpec::new(schema())
        .with_partition_rows(&[10, 10])
        .with_batch_layout(BatchLayout::Fixed(4))
}

fn execute(source: &MockSourceExec, partition: usize) -> SendableRecordBatchStream {
    source
        .execute(partition, Arc::new(TaskContext::default()))
        .unwrap()
}

/// The next item of `stream`, or `None` if the stream does not produce one
/// within a short time
async fn next_item(
    stream: &mut SendableRecordBatchStream,
) -> Option<Option<datafusion_common::Result<RecordBatch>>> {
    tokio::time::timeout(Duration::from_millis(50), stream.next())
        .await
        .ok()
}

#[tokio::test]
async fn pending_after_serves_batches_then_stalls() {
    let source = small_spec().build().unwrap();
    let stalling = |batches| {
        source
            .clone()
            .try_with_stream_behavior(StreamBehavior::PendingAfter(batches))
            .unwrap()
    };
    let mut stream = execute(&stalling(2), 0);
    for _ in 0..2 {
        assert!(matches!(next_item(&mut stream).await, Some(Some(Ok(_)))));
    }
    assert!(
        next_item(&mut stream).await.is_none(),
        "should stay pending"
    );

    // Fewer batches than requested: all of them, then pending
    let mut stream = execute(&stalling(10), 1);
    for _ in 0..3 {
        assert!(matches!(next_item(&mut stream).await, Some(Some(Ok(_)))));
    }
    assert!(
        next_item(&mut stream).await.is_none(),
        "should stay pending"
    );
}

#[tokio::test]
async fn error_after_serves_batches_then_fails() {
    let source = small_spec()
        .build()
        .unwrap()
        .try_with_stream_behavior(StreamBehavior::ErrorAfter(1))
        .unwrap();
    let mut stream = execute(&source, 0);
    assert!(matches!(next_item(&mut stream).await, Some(Some(Ok(_)))));
    let Some(Some(Err(error))) = next_item(&mut stream).await else {
        panic!("expected an error");
    };
    assert!(
        error.to_string().contains("injected error after 1 batches"),
        "{error}"
    );
    assert!(matches!(next_item(&mut stream).await, Some(None)));
}

#[test]
fn stalling_and_failing_sources_keep_their_properties() {
    let source = small_spec().build().unwrap();
    for behavior in [
        StreamBehavior::PendingAfter(1),
        StreamBehavior::ErrorAfter(1),
    ] {
        let variant = source.clone().try_with_stream_behavior(behavior).unwrap();
        assert!(Arc::ptr_eq(source.properties(), variant.properties()));
        let stats = StatisticsContext::new()
            .compute(&variant, &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(20));
    }
}

#[tokio::test]
async fn unbounded_source_repeats_its_data() {
    let bounded = small_spec().build().unwrap();
    let source = bounded
        .clone()
        .try_with_stream_behavior(StreamBehavior::Unbounded { max_rows: 50 })
        .unwrap();
    assert_eq!(
        source.properties().boundedness,
        Boundedness::Unbounded {
            requires_infinite_memory: false
        }
    );
    let stats = StatisticsContext::new()
        .compute(&source, &StatisticsArgs::new())
        .unwrap();
    assert_eq!(stats.num_rows, Precision::Absent);
    let display = displayable(&source).one_line().to_string();
    assert!(
        display.contains("stream=Unbounded(max_rows=50)"),
        "{display}"
    );

    // Batches repeat until at least `max_rows` rows, then the stream stalls
    // without ending
    let mut stream = execute(&source, 0);
    let mut batches = vec![];
    while let Some(Some(batch)) = next_item(&mut stream).await {
        batches.push(batch.unwrap());
    }
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 50);
    assert_eq!(batches[3], bounded.partitions()[0][0]);
}

#[tokio::test]
async fn unbounded_sorted_source_stays_sorted() {
    let schema = schema();
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
        col("a", &schema).unwrap(),
    )])
    .unwrap();
    let source = SourceSpec::new(schema)
        .with_partition_rows(&[10, 0])
        .with_batch_layout(BatchLayout::Fixed(4))
        .with_ordering(ordering.clone())
        .build()
        .unwrap()
        .try_with_stream_behavior(StreamBehavior::Unbounded { max_rows: 40 })
        .unwrap();
    let mut stream = execute(&source, 0);
    let mut batches = vec![];
    while let Some(Some(batch)) = next_item(&mut stream).await {
        batches.push(batch.unwrap());
    }
    assert!(batches.iter().map(RecordBatch::num_rows).sum::<usize>() >= 40);
    assert_eq!(
        oracle::first_unsorted_row(&batches, &ordering).unwrap(),
        None
    );

    // A partition without rows ends at once
    let mut stream = execute(&source, 1);
    assert!(matches!(next_item(&mut stream).await, Some(None)));
}

#[test]
fn unbounded_source_needs_rows() {
    let error = SourceSpec::new(schema())
        .with_partition_rows(&[0, 0])
        .build()
        .unwrap()
        .try_with_stream_behavior(StreamBehavior::Unbounded { max_rows: 10 })
        .unwrap_err();
    assert!(error.to_string().contains("at least one row"), "{error}");
}
