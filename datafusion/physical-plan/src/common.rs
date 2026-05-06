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

//! Defines common code used in execution plans

use std::fs;
use std::fs::metadata;
use std::sync::Arc;

use super::SendableRecordBatchStream;
use crate::expressions::{CastExpr, Column};
use crate::projection::{ProjectionExec, ProjectionExpr};
use crate::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use crate::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, Statistics,
};

use arrow::array::Array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, plan_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};

use futures::{StreamExt, TryStreamExt};

/// [`MemoryReservation`] used across query execution streams
pub(crate) type SharedMemoryReservation = Arc<MemoryReservation>;

/// Create a vector of record batches from a stream
pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream.try_collect::<Vec<_>>().await
}

/// Recursively builds a list of files in a directory with a given extension
pub fn build_checked_file_list(dir: &str, ext: &str) -> Result<Vec<String>> {
    let mut filenames: Vec<String> = Vec::new();
    build_file_list_recurse(dir, &mut filenames, ext)?;
    if filenames.is_empty() {
        return plan_err!("No files found at {dir} with file extension {ext}");
    }
    Ok(filenames)
}

/// Recursively builds a list of files in a directory with a given extension
pub fn build_file_list(dir: &str, ext: &str) -> Result<Vec<String>> {
    let mut filenames: Vec<String> = Vec::new();
    build_file_list_recurse(dir, &mut filenames, ext)?;
    Ok(filenames)
}

/// Recursively build a list of files in a directory with a given extension with an accumulator list
fn build_file_list_recurse(
    dir: &str,
    filenames: &mut Vec<String>,
    ext: &str,
) -> Result<()> {
    let metadata = metadata(dir)?;
    if metadata.is_file() {
        if dir.ends_with(ext) {
            filenames.push(dir.to_string());
        }
    } else {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(path_name) = path.to_str() {
                if path.is_dir() {
                    build_file_list_recurse(path_name, filenames, ext)?;
                } else if path_name.ends_with(ext) {
                    filenames.push(path_name.to_string());
                }
            } else {
                return plan_err!("Invalid path");
            }
        }
    }
    Ok(())
}

/// Align `input`'s physical plan schema with `expected_schema`.
///
/// This helper is intended for operators that combine independently planned children but
/// expose a single declared output schema. It returns `input` unchanged when schemas already
/// match exactly. Otherwise, it validates positional compatibility and uses a plan-time
/// adapter whose advertised and emitted schema is exactly `expected_schema`.
///
/// Prefer this helper over rebinding batches inside a parent operator's stream. The alignment
/// is visible in the physical plan, while batch schema rebinding remains contained in the
/// adapter as the implementation detail required to uphold the plan-level schema contract.
///
/// This helper can align field names and nullability to the declared schema. It rejects
/// differences that would change values or silently lose schema information, such as column
/// count, data type, field metadata, or schema metadata mismatches.
///
/// When an adapter is required, it conservatively derives fresh equivalence properties from
/// `expected_schema` and drops child hash partitioning because field names/nullability may have
/// changed while the underlying partitioning expressions still refer to the child schema.
pub fn align_plan_to_schema(
    input: Arc<dyn ExecutionPlan>,
    expected_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    validate_schema_alignment(&input_schema, expected_schema, "align")?;

    if input_schema.as_ref() == expected_schema.as_ref() {
        return Ok(input);
    }

    if let Ok(projected) = project_plan_to_schema(Arc::clone(&input), expected_schema) {
        debug_assert_eq!(projected.schema().as_ref(), expected_schema.as_ref());
        return Ok(projected);
    }

    Ok(Arc::new(SchemaAlignExec::try_new(
        input,
        Arc::clone(expected_schema),
    )?))
}

/// Project `input` to `expected_schema` when [`ProjectionExec`] can produce that exact schema.
///
/// This is a narrower helper than [`align_plan_to_schema`]. It is useful when a positional
/// projection/alias is sufficient. It rejects requests where projection cannot advertise the
/// exact expected schema, such as nullability narrowing.
pub fn project_plan_to_schema(
    input: Arc<dyn ExecutionPlan>,
    expected_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    validate_schema_alignment(&input_schema, expected_schema, "project")?;

    if input_schema.as_ref() == expected_schema.as_ref() {
        return Ok(input);
    }

    if let Some((i, input_field, expected_field)) = input_schema
        .fields()
        .iter()
        .zip(expected_schema.fields().iter())
        .enumerate()
        .find_map(|(i, (input_field, expected_field))| {
            (input_field.is_nullable() && !expected_field.is_nullable()).then_some((
                i,
                input_field,
                expected_field,
            ))
        })
    {
        return plan_err!(
            "Cannot project plan column {i} ('{}') to expected output field '{}': \
             field nullability differs (input field: {:?}, expected field: {:?})",
            input_field.name(),
            expected_field.name(),
            input_field,
            expected_field
        );
    }

    let projection_exprs = expected_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, expected_field)| {
            let input_field = input_schema.field(i);
            let column = Arc::new(Column::new(input_field.name(), i));
            let expr = if !input_field.is_nullable() && expected_field.is_nullable() {
                Arc::new(CastExpr::new_with_target_field(
                    column,
                    Arc::clone(expected_field),
                    None,
                )) as _
            } else {
                column as _
            };
            ProjectionExpr {
                expr,
                alias: expected_field.name().clone(),
            }
        })
        .collect::<Vec<_>>();

    let projection = ProjectionExec::try_new(projection_exprs, input)?;
    debug_assert_eq!(projection.schema().as_ref(), expected_schema.as_ref());
    Ok(Arc::new(projection))
}

fn validate_schema_alignment(
    input_schema: &SchemaRef,
    expected_schema: &SchemaRef,
    operation: &str,
) -> Result<()> {
    if input_schema.fields().len() != expected_schema.fields().len() {
        return plan_err!(
            "Cannot {operation} plan to expected schema: expected {} column(s), got {}",
            expected_schema.fields().len(),
            input_schema.fields().len()
        );
    }

    if input_schema.metadata() != expected_schema.metadata() {
        return plan_err!(
            "Cannot {operation} plan to expected schema: schema metadata differ"
        );
    }

    if let Some((i, input_field, expected_field, mismatch)) = input_schema
        .fields()
        .iter()
        .zip(expected_schema.fields().iter())
        .enumerate()
        .find_map(|(i, (input_field, expected_field))| {
            if input_field.data_type() != expected_field.data_type() {
                Some((i, input_field, expected_field, "data type"))
            } else if input_field.metadata() != expected_field.metadata() {
                Some((i, input_field, expected_field, "metadata"))
            } else {
                None
            }
        })
    {
        return plan_err!(
            "Cannot {operation} plan column {i} ('{}') to expected output field '{}': \
             field {mismatch} differs (input field: {:?}, expected field: {:?})",
            input_field.name(),
            expected_field.name(),
            input_field,
            expected_field
        );
    }

    Ok(())
}

/// Plan-time schema adapter for positional schema alignment.
///
/// [`ProjectionExec`] cannot express every schema-only alignment. In particular, a column
/// expression remains nullable when its input field is nullable, so projection cannot advertise
/// a non-null expected field. This adapter is for cases where the operator-level contract has
/// already established that columns are positionally compatible and the child plan must expose
/// the declared schema exactly.
#[derive(Debug, Clone)]
pub struct SchemaAlignExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl SchemaAlignExec {
    /// Create a new schema alignment adapter.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Result<Self> {
        validate_schema_alignment(&input.schema(), &schema, "align")?;

        let input_properties = input.properties();
        let partitioning = match &input_properties.partitioning {
            Partitioning::RoundRobinBatch(partitions) => {
                Partitioning::RoundRobinBatch(*partitions)
            }
            partitioning => Partitioning::UnknownPartitioning(partitioning.partition_count()),
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            partitioning,
            input_properties.emission_type,
            input_properties.boundedness,
        )
        .with_evaluation_type(input_properties.evaluation_type)
        .with_scheduling_type(input_properties.scheduling_type);

        Ok(Self {
            input,
            schema,
            cache: Arc::new(properties),
        })
    }

    /// Input plan being aligned.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for SchemaAlignExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SchemaAlignExec")
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for SchemaAlignExec {
    fn name(&self) -> &'static str {
        "SchemaAlignExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.try_into().map_err(|children: Vec<_>| {
            datafusion_common::DataFusionError::Internal(format!(
                "SchemaAlignExec expected 1 child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::try_new(input, Arc::clone(&self.schema))?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let stream = self.input.execute(partition, context)?.map({
            let schema = Arc::clone(&schema);
            move |batch| {
                let batch = batch?;
                if batch.schema().as_ref() == schema.as_ref() {
                    Ok(batch)
                } else {
                    RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                        .map_err(Into::into)
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }
}

/// If running in a tokio context spawns the execution of `stream` to a separate task
/// allowing it to execute in parallel with an intermediate buffer of size `buffer`
pub fn spawn_buffered(
    mut input: SendableRecordBatchStream,
    buffer: usize,
) -> SendableRecordBatchStream {
    // Use tokio only if running from a multi-thread tokio context
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
        {
            let mut builder = RecordBatchReceiverStream::builder(input.schema(), buffer);

            let sender = builder.tx();

            builder.spawn(async move {
                while let Some(item) = input.next().await {
                    if sender.send(item).await.is_err() {
                        // Receiver dropped when query is shutdown early (e.g., limit) or error,
                        // no need to return propagate the send error.
                        return Ok(());
                    }
                }

                Ok(())
            });

            builder.build()
        }
        _ => input,
    }
}

/// Computes the statistics for an in-memory RecordBatch
///
/// Only computes statistics that are in arrows metadata (num rows, byte size and nulls)
/// and does not apply any kernel on the actual data.
pub fn compute_record_batch_statistics(
    batches: &[Vec<RecordBatch>],
    schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Statistics {
    let nb_rows = batches.iter().flatten().map(RecordBatch::num_rows).sum();

    let projection = match projection {
        Some(p) => p,
        None => (0..schema.fields().len()).collect(),
    };

    let total_byte_size = batches
        .iter()
        .flatten()
        .map(|b| {
            projection
                .iter()
                .map(|index| b.column(*index).get_array_memory_size())
                .sum::<usize>()
        })
        .sum();

    let mut null_counts = vec![0; projection.len()];

    for partition in batches.iter() {
        for batch in partition {
            for (stat_index, col_index) in projection.iter().enumerate() {
                null_counts[stat_index] += batch
                    .column(*col_index)
                    .logical_nulls()
                    .map(|nulls| nulls.null_count())
                    .unwrap_or_default();
            }
        }
    }
    let column_statistics = null_counts
        .into_iter()
        .map(|null_count| {
            let mut s = ColumnStatistics::new_unknown();
            s.null_count = Precision::Exact(null_count);
            s
        })
        .collect();

    Statistics {
        num_rows: Precision::Exact(nb_rows),
        total_byte_size: Precision::Exact(total_byte_size),
        column_statistics,
    }
}

/// Checks if the given projection is valid for the given schema.
pub fn can_project(schema: &SchemaRef, projection: Option<&[usize]>) -> Result<()> {
    match projection {
        Some(columns) => {
            if columns
                .iter()
                .max()
                .is_some_and(|&i| i >= schema.fields().len())
            {
                Err(arrow::error::ArrowError::SchemaError(format!(
                    "project index {} out of bounds, max field {}",
                    columns.iter().max().unwrap(),
                    schema.fields().len()
                ))
                .into())
            } else {
                Ok(())
            }
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::empty::EmptyExec;
    use crate::projection::ProjectionExec;

    use std::collections::HashMap;

    use arrow::{
        array::{Float32Array, Float64Array, UInt64Array},
        datatypes::{DataType, Field, Schema},
    };

    fn empty_exec(fields: Vec<Field>) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(fields))))
    }

    #[test]
    fn test_compute_record_batch_statistics_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let stats = compute_record_batch_statistics(&[], &schema, Some(vec![0, 1]));

        assert_eq!(stats.num_rows, Precision::Exact(0));
        assert_eq!(stats.total_byte_size, Precision::Exact(0));
        Ok(())
    }

    #[test]
    fn test_compute_record_batch_statistics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("u64", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float32Array::from(vec![1., 2., 3.])),
                Arc::new(Float64Array::from(vec![9., 8., 7.])),
                Arc::new(UInt64Array::from(vec![4, 5, 6])),
            ],
        )?;

        // Just select f32,f64
        let select_projection = Some(vec![0, 1]);
        let byte_size = batch
            .project(&select_projection.clone().unwrap())
            .unwrap()
            .get_array_memory_size();

        let actual =
            compute_record_batch_statistics(&[vec![batch]], &schema, select_projection);

        let expected = Statistics {
            num_rows: Precision::Exact(3),
            total_byte_size: Precision::Exact(byte_size),
            column_statistics: vec![
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    distinct_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                    null_count: Precision::Exact(0),
                    byte_size: Precision::Absent,
                },
            ],
        };

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn test_compute_record_batch_statistics_null() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("u64", DataType::UInt64, true)]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![Some(1), None, None]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![Some(1), Some(2), None]))],
        )?;
        let byte_size = batch1.get_array_memory_size() + batch2.get_array_memory_size();
        let actual =
            compute_record_batch_statistics(&[vec![batch1], vec![batch2]], &schema, None);

        let expected = Statistics {
            num_rows: Precision::Exact(6),
            total_byte_size: Precision::Exact(byte_size),
            column_statistics: vec![ColumnStatistics {
                distinct_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                sum_value: Precision::Absent,
                null_count: Precision::Exact(3),
                byte_size: Precision::Absent,
            }],
        };

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn project_plan_to_schema_returns_input_when_schema_matches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let result = project_plan_to_schema(Arc::clone(&input), &schema)?;

        assert!(Arc::ptr_eq(&input, &result));
        Ok(())
    }

    #[test]
    fn project_plan_to_schema_aliases_field_names_with_projection_exec() -> Result<()> {
        let input = empty_exec(vec![
            Field::new("recursive_a", DataType::Int32, false),
            Field::new("recursive_b", DataType::Utf8, true),
        ]);
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        let result = project_plan_to_schema(Arc::clone(&input), &expected_schema)?;

        let projection = result
            .downcast_ref::<ProjectionExec>()
            .expect("schema rename should use ProjectionExec");
        assert!(Arc::ptr_eq(projection.input(), &input));
        assert_eq!(projection.schema(), expected_schema);
        assert_eq!(projection.expr()[0].alias, "a");
        assert_eq!(projection.expr()[1].alias, "b");
        Ok(())
    }

    #[test]
    fn project_plan_to_schema_preserves_matching_metadata_while_renaming() -> Result<()> {
        let field_metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        let schema_metadata =
            HashMap::from([("schema-key".to_string(), "schema-value".to_string())]);
        let input_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("input", DataType::Int32, false)
                    .with_metadata(field_metadata.clone()),
            ],
            schema_metadata.clone(),
        ));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(input_schema));
        let expected_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("expected", DataType::Int32, false)
                    .with_metadata(field_metadata),
            ],
            schema_metadata,
        ));

        let result = project_plan_to_schema(input, &expected_schema)?;

        assert_eq!(result.schema(), expected_schema);
        Ok(())
    }

    #[test]
    fn project_plan_to_schema_errors_on_column_count_mismatch() {
        let input = empty_exec(vec![Field::new("a", DataType::Int32, false)]);
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let err = project_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("expected 2 column"));
    }

    #[test]
    fn project_plan_to_schema_errors_on_type_mismatch() {
        let input = empty_exec(vec![Field::new("a", DataType::Int32, false)]);
        let expected_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

        let err = project_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("field data type differs"));
    }

    #[test]
    fn project_plan_to_schema_widens_nullability() -> Result<()> {
        let input = empty_exec(vec![Field::new("a", DataType::Int32, false)]);
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "renamed",
            DataType::Int32,
            true,
        )]));

        let result = project_plan_to_schema(input, &expected_schema)?;

        assert_eq!(result.schema(), expected_schema);
        Ok(())
    }

    #[test]
    fn project_plan_to_schema_errors_on_nullability_narrowing() {
        let input = empty_exec(vec![Field::new("a", DataType::Int32, true)]);
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "renamed",
            DataType::Int32,
            false,
        )]));

        let err = project_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("field nullability differs"));
    }

    #[test]
    fn align_plan_to_schema_returns_input_when_schema_matches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let result = align_plan_to_schema(Arc::clone(&input), &schema)?;

        assert!(Arc::ptr_eq(&input, &result));
        Ok(())
    }

    #[test]
    fn align_plan_to_schema_uses_projection_for_rename_only() -> Result<()> {
        let input = empty_exec(vec![Field::new("recursive_a", DataType::Int32, false)]);
        let expected_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let result = align_plan_to_schema(Arc::clone(&input), &expected_schema)?;

        let projection = result
            .downcast_ref::<ProjectionExec>()
            .expect("rename-only alignment should use ProjectionExec");
        assert!(Arc::ptr_eq(projection.input(), &input));
        assert_eq!(projection.schema(), expected_schema);
        Ok(())
    }

    #[test]
    fn align_plan_to_schema_uses_adapter_for_nullability_narrowing() -> Result<()> {
        let input = empty_exec(vec![Field::new("a", DataType::Int32, true)]);
        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "renamed",
            DataType::Int32,
            false,
        )]));

        let result = align_plan_to_schema(Arc::clone(&input), &expected_schema)?;

        let aligned = result
            .downcast_ref::<SchemaAlignExec>()
            .expect("nullability narrowing should use SchemaAlignExec");
        assert!(Arc::ptr_eq(aligned.input(), &input));
        assert_eq!(aligned.schema(), expected_schema);
        Ok(())
    }

    #[test]
    fn align_plan_to_schema_errors_on_column_count_mismatch() {
        let input = empty_exec(vec![Field::new("a", DataType::Int32, false)]);
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let err = align_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("expected 2 column"));
    }

    #[test]
    fn align_plan_to_schema_errors_on_type_mismatch() {
        let input = empty_exec(vec![Field::new("a", DataType::Int32, false)]);
        let expected_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

        let err = align_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("field data type differs"));
    }

    #[test]
    fn align_plan_to_schema_errors_on_field_metadata_mismatch() {
        let input =
            empty_exec(vec![Field::new("a", DataType::Int32, false).with_metadata(
                HashMap::from([("source".to_string(), "input".to_string())]),
            )]);
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("renamed", DataType::Int32, false).with_metadata(HashMap::from([
                ("source".to_string(), "expected".to_string()),
            ])),
        ]));

        let err = align_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("field metadata differs"));
    }

    #[test]
    fn align_plan_to_schema_errors_on_schema_metadata_mismatch() {
        let input_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("a", DataType::Int32, false)],
            HashMap::from([("source".to_string(), "input".to_string())]),
        ));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(input_schema));
        let expected_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("renamed", DataType::Int32, false)],
            HashMap::from([("source".to_string(), "expected".to_string())]),
        ));

        let err = align_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("schema metadata differ"));
    }

    #[test]
    fn project_plan_to_schema_errors_on_field_metadata_mismatch() {
        let input =
            empty_exec(vec![Field::new("a", DataType::Int32, false).with_metadata(
                HashMap::from([("source".to_string(), "input".to_string())]),
            )]);
        let expected_schema = Arc::new(Schema::new(vec![
            Field::new("renamed", DataType::Int32, false).with_metadata(HashMap::from([
                ("source".to_string(), "expected".to_string()),
            ])),
        ]));

        let err = project_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("field metadata differs"));
    }

    #[test]
    fn project_plan_to_schema_errors_on_schema_metadata_mismatch() {
        let input_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("a", DataType::Int32, false)],
            HashMap::from([("source".to_string(), "input".to_string())]),
        ));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(input_schema));
        let expected_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("renamed", DataType::Int32, false)],
            HashMap::from([("source".to_string(), "expected".to_string())]),
        ));

        let err = project_plan_to_schema(input, &expected_schema).unwrap_err();
        assert!(err.to_string().contains("schema metadata differ"));
    }
}
