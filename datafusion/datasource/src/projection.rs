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

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result, ScalarValue,
};
use datafusion_physical_expr::{
    expressions::{Column, Literal},
    projection::{ProjectionExpr, ProjectionExprs},
};
use futures::{FutureExt, StreamExt};
use itertools::Itertools;

use crate::{
    file_stream::{FileOpenFuture, FileOpener},
    PartitionedFile, TableSchema,
};

/// A file opener that handles applying a projection on top of an inner opener.
///
/// This includes handling partition columns.
///
/// Any projection pushed down will be split up into:
/// - Simple column indices / column selection
/// - A remainder projection that this opener applies on top of it
///
/// This is meant to simplify projection pushdown for sources like CSV
/// that can only handle "simple" column selection.
pub struct ProjectionOpener {
    inner: Arc<dyn FileOpener>,
    projection: ProjectionExprs,
    input_schema: SchemaRef,
    partition_columns: Vec<PartitionColumnIndex>,
}

impl ProjectionOpener {
    pub fn try_new(
        projection: SplitProjection,
        inner: Arc<dyn FileOpener>,
        file_schema: &Schema,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(ProjectionOpener {
            inner,
            projection: projection.remapped_projection,
            input_schema: Arc::new(file_schema.project(&projection.file_indices)?),
            partition_columns: projection.partition_columns,
        }))
    }
}

impl FileOpener for ProjectionOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let partition_values = partitioned_file.partition_values.clone();
        // Modify any references to partition columns in the projection expressions
        // and substitute them with literal values from PartitionedFile.partition_values
        let projection = if self.partition_columns.is_empty() {
            self.projection.clone()
        } else {
            inject_partition_columns_into_projection(
                &self.projection,
                &self.partition_columns,
                partition_values,
            )
        };
        let projector = projection.make_projector(&self.input_schema)?;

        let inner = self.inner.open(partitioned_file)?;

        Ok(async move {
            let stream = inner.await?;
            let stream = stream.map(move |batch| {
                let batch = batch?;
                let batch = projector.project_batch(&batch)?;
                Ok(batch)
            });
            Ok(stream.boxed())
        }
        .boxed())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PartitionColumnIndex {
    /// The index of this partition column in the remainder projection (>= num_file_columns)
    pub in_remainder_projection: usize,
    /// The index of this partition column in the partition_values array
    pub in_partition_values: usize,
}

fn inject_partition_columns_into_projection(
    projection: &ProjectionExprs,
    partition_columns: &[PartitionColumnIndex],
    partition_values: Vec<ScalarValue>,
) -> ProjectionExprs {
    // Pre-create all literals for partition columns to avoid cloning ScalarValues multiple times.
    let partition_literals: Vec<Arc<Literal>> = partition_values
        .into_iter()
        .map(|value| Arc::new(Literal::new(value)))
        .collect();

    let projections = projection
        .iter()
        .map(|projection| {
            let expr = Arc::clone(&projection.expr)
                .transform(|expr| {
                    let original_expr = Arc::clone(&expr);
                    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                        // Check if this column index corresponds to a partition column
                        if let Some(pci) = partition_columns
                            .iter()
                            .find(|pci| pci.in_remainder_projection == column.index())
                        {
                            let literal =
                                Arc::clone(&partition_literals[pci.in_partition_values]);
                            return Ok(Transformed::yes(literal));
                        }
                    }
                    Ok(Transformed::no(original_expr))
                })
                .data()
                .expect("infallible transform");
            ProjectionExpr::new(expr, projection.alias.clone())
        })
        .collect_vec();
    ProjectionExprs::new(projections)
}

/// At a high level the goal of SplitProjection is to take a ProjectionExprs meant to be applied to the table schema
/// and split that into:
/// - The projection indices into the file schema (file_indices)
/// - The projection indices into the partition values (partition_value_indices), which pre-compute both the index into the table schema
///   and the index into the partition values array
/// - A remapped projection that can be applied after the file projection is applied
///   This remapped projection has the following properties:
///     - Column indices referring to file columns are remapped to [0..file_indices.len())
///     - Column indices referring to partition columns are remapped to [file_indices.len()..)
///
///   This allows the ProjectionOpener to easily identify which columns in the remapped projection
///   refer to partition columns and substitute them with literals from the partition values.
#[derive(Debug, Clone)]
pub struct SplitProjection {
    /// The original projection this [`SplitProjection`] was derived from
    pub source: ProjectionExprs,
    /// Column indices to read from file (public for file sources)
    pub file_indices: Vec<usize>,
    /// Pre-computed partition column mappings (internal, used by ProjectionOpener)
    pub(crate) partition_columns: Vec<PartitionColumnIndex>,
    /// The remapped projection (internal, used by ProjectionOpener)
    pub(crate) remapped_projection: ProjectionExprs,
}

impl SplitProjection {
    pub fn unprojected(table_schema: &TableSchema) -> Self {
        let projection = ProjectionExprs::from_indices(
            &(0..table_schema.table_schema().fields().len()).collect_vec(),
            table_schema.table_schema(),
        );
        Self::new(table_schema.file_schema(), &projection)
    }

    /// Creates a new [`SplitProjection`] by splitting a projection into
    /// simple file column indices and a remainder projection that is applied after reading the file.
    ///
    /// In other words: we get a `Vec<usize>` projection that is meant to be applied on top of `file_schema`
    /// and a remainder projection that is applied to the result of that first projection.
    ///
    /// Here `file_schema` is expected to be the *logical* schema of the file, that is the
    /// table schema minus any partition columns.
    /// Partition columns are always expected to be at the end of the table schema.
    /// Note that `file_schema` is *not* the physical schema of the file.
    pub fn new(logical_file_schema: &Schema, projection: &ProjectionExprs) -> Self {
        let num_file_schema_columns = logical_file_schema.fields().len();

        // Collect all unique columns and classify as file or partition
        let mut file_columns = Vec::new();
        let mut partition_columns = Vec::new();
        let mut all_columns = std::collections::HashMap::new();

        // Extract all unique column references (index -> name)
        for proj_expr in projection {
            proj_expr
                .expr
                .apply(|expr| {
                    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                        all_columns
                            .entry(column.index())
                            .or_insert_with(|| column.name().to_string());
                    }
                    Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
                })
                .expect("infallible apply");
        }

        // Sort by index and classify into file vs partition columns
        let mut sorted_columns: Vec<_> = all_columns
            .into_iter()
            .map(|(idx, name)| (name, idx))
            .collect();
        sorted_columns.sort_by_key(|(_, idx)| *idx);

        // Separate file and partition columns, assigning final indices
        // Pre-create all remapped columns to avoid duplicate Arc'd expressions
        let mut column_mapping = std::collections::HashMap::new();
        let mut file_idx = 0;
        let mut partition_idx = 0;

        for (name, original_index) in sorted_columns {
            let new_index = if original_index < num_file_schema_columns {
                // File column: gets index [0..num_file_columns)
                file_columns.push(original_index);
                let idx = file_idx;
                file_idx += 1;
                idx
            } else {
                // Partition column: gets index [num_file_columns..)
                partition_columns.push(original_index);
                let idx = file_idx + partition_idx;
                partition_idx += 1;
                idx
            };

            // Pre-create the remapped column so all references can share the same Arc
            let new_column: Arc<dyn datafusion_physical_plan::PhysicalExpr> =
                Arc::new(Column::new(&name, new_index));
            column_mapping.insert(original_index, new_column);
        }

        // Single tree transformation: remap all column references using pre-created columns
        let remapped_projection = projection
            .iter()
            .map(|proj_expr| {
                let expr = Arc::clone(&proj_expr.expr)
                    .transform(|expr| {
                        let original_expr = Arc::clone(&expr);
                        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                            if let Some(new_column) = column_mapping.get(&column.index())
                            {
                                return Ok(Transformed::yes(Arc::clone(new_column)));
                            }
                        }
                        Ok(Transformed::no(original_expr))
                    })
                    .data()
                    .expect("infallible transform");
                ProjectionExpr::new(expr, proj_expr.alias.clone())
            })
            .collect_vec();

        // Pre-compute partition column mappings for ProjectionOpener
        let num_file_columns = file_columns.len();
        let partition_column_mappings = partition_columns
            .iter()
            .enumerate()
            .map(|(partition_idx, &table_index)| PartitionColumnIndex {
                in_remainder_projection: num_file_columns + partition_idx,
                in_partition_values: table_index - num_file_schema_columns,
            })
            .collect_vec();

        Self {
            source: projection.clone(),
            file_indices: file_columns,
            partition_columns: partition_column_mappings,
            remapped_projection: ProjectionExprs::from(remapped_projection),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, SchemaRef};
    use datafusion_common::{record_batch, DFSchema, ScalarValue};
    use datafusion_expr::{col, execution_props::ExecutionProps, Expr};
    use datafusion_physical_expr::{create_physical_exprs, projection::ProjectionExpr};
    use itertools::Itertools;

    use super::*;

    fn create_projection_exprs<'a>(
        exprs: impl IntoIterator<Item = &'a Expr>,
        schema: &SchemaRef,
    ) -> ProjectionExprs {
        let df_schema = DFSchema::try_from(Arc::clone(schema)).unwrap();
        let physical_exprs =
            create_physical_exprs(exprs, &df_schema, &ExecutionProps::default()).unwrap();
        let projection_exprs = physical_exprs
            .into_iter()
            .enumerate()
            .map(|(i, e)| ProjectionExpr::new(Arc::clone(&e), format!("col{i}")))
            .collect_vec();
        ProjectionExprs::from(projection_exprs)
    }

    #[test]
    fn test_split_projection_with_partition_columns() {
        use arrow::array::AsArray;
        use arrow::datatypes::Field;
        // Simulate the avro_exec_with_partition test scenario:
        // file_schema has 3 fields
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("bool_col", DataType::Boolean, false),
            Field::new("tinyint_col", DataType::Int8, false),
        ]));

        // table_schema has 4 fields (3 file + 1 partition)
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("bool_col", DataType::Boolean, false),
            Field::new("tinyint_col", DataType::Int8, false),
            Field::new("date", DataType::Utf8, false), // partition column at index 3
        ]));

        // projection indices: [0, 1, 3, 2]
        // This should select: id (0), bool_col (1), date (3-partition), tinyint_col (2)
        let projection_indices = vec![0, 1, 3, 2];

        // Create projection expressions from indices using the table schema
        let projection =
            ProjectionExprs::from_indices(&projection_indices, &table_schema);

        // Call SplitProjection to separate file and partition columns
        let split = SplitProjection::new(&file_schema, &projection);

        // The file_indices should be [0, 1, 2] (all file columns needed)
        assert_eq!(split.file_indices, vec![0, 1, 2]);

        // Should have 1 partition column at in_partition_values index 0
        assert_eq!(split.partition_columns.len(), 1);
        assert_eq!(split.partition_columns[0].in_partition_values, 0);

        // Now create a batch with only the file columns
        let file_batch = record_batch!(
            ("id", Int32, vec![4]),
            ("bool_col", Boolean, vec![true]),
            ("tinyint_col", Int8, vec![0])
        )
        .unwrap();

        // After the fix, the remainder projection should have remapped indices:
        // - File columns: [0, 1, 2] (unchanged since they're already in order)
        // - Partition column: [3] (stays at index 3, which is >= num_file_columns)
        // So the remainder expects input columns [0, 1, 2] and references column [3] for partition

        // Verify that we can inject partition columns and apply the projection
        let partition_values = vec![ScalarValue::from("2021-10-26")];

        // Create partition column mapping
        let partition_columns = vec![PartitionColumnIndex {
            in_remainder_projection: 3, // partition column is at index 3 in remainder
            in_partition_values: 0,     // first partition value
        }];

        // Inject partition columns (replaces Column(3) with Literal)
        let injected_projection = inject_partition_columns_into_projection(
            &split.remapped_projection,
            &partition_columns,
            partition_values,
        );

        // Now the projection should work on the file batch
        let projector = injected_projection
            .make_projector(&file_batch.schema())
            .unwrap();
        let result = projector.project_batch(&file_batch).unwrap();

        // Verify the output has the correct column order: id, bool_col, date, tinyint_col
        assert_eq!(result.num_columns(), 4);
        assert_eq!(
            result
                .column(0)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(0),
            4
        );
        assert!(result.column(1).as_boolean().value(0));
        assert_eq!(result.column(2).as_string::<i32>().value(0), "2021-10-26");
        assert_eq!(
            result
                .column(3)
                .as_primitive::<arrow::datatypes::Int8Type>()
                .value(0),
            0
        );
    }

    // ========================================================================
    // Comprehensive Test Suite for SplitProjection
    // ========================================================================

    // Helper to create test schemas with file and partition columns
    fn create_test_schemas(
        file_cols: usize,
        partition_cols: usize,
    ) -> (SchemaRef, SchemaRef) {
        use arrow::datatypes::Field;

        let file_fields: Vec<_> = (0..file_cols)
            .map(|i| Field::new(format!("col_{i}"), DataType::Int32, false))
            .collect();

        let mut table_fields = file_fields.clone();
        table_fields.extend(
            (0..partition_cols)
                .map(|i| Field::new(format!("part_{i}"), DataType::Utf8, false)),
        );

        (
            Arc::new(Schema::new(file_fields)),
            Arc::new(Schema::new(table_fields)),
        )
    }

    // ========================================================================
    // Partition Column Handling Tests
    // ========================================================================

    #[test]
    fn test_split_projection_only_file_columns() {
        let (file_schema, table_schema) = create_test_schemas(3, 2);
        // Select only file columns [0, 1, 2]
        let projection = ProjectionExprs::from_indices(&[0, 1, 2], &table_schema);

        let split = SplitProjection::new(&file_schema, &projection);

        assert_eq!(split.file_indices, vec![0, 1, 2]);
        assert_eq!(split.partition_columns.len(), 0);
    }

    #[test]
    fn test_split_projection_only_partition_columns() {
        let (file_schema, table_schema) = create_test_schemas(3, 2);
        // Select only partition columns [3, 4]
        let projection = ProjectionExprs::from_indices(&[3, 4], &table_schema);

        let split = SplitProjection::new(&file_schema, &projection);

        assert_eq!(split.file_indices, Vec::<usize>::new());
        assert_eq!(split.partition_columns.len(), 2);
        assert_eq!(split.partition_columns[0].in_partition_values, 0);
        assert_eq!(split.partition_columns[1].in_partition_values, 1);
    }

    #[test]
    fn test_split_projection_multiple_partition_columns() {
        let (file_schema, table_schema) = create_test_schemas(2, 3);
        // File cols: 0, 1; Partition cols: 2, 3, 4
        // Select: [0, 2, 4, 1, 3] (mixed file and partition)
        let projection = ProjectionExprs::from_indices(&[0, 2, 4, 1, 3], &table_schema);

        let split = SplitProjection::new(&file_schema, &projection);

        assert_eq!(split.file_indices, vec![0, 1]);
        assert_eq!(split.partition_columns.len(), 3);
        assert_eq!(split.partition_columns[0].in_partition_values, 0);
        assert_eq!(split.partition_columns[1].in_partition_values, 1);
        assert_eq!(split.partition_columns[2].in_partition_values, 2);

        // Verify remapped projection has correct indices
        // File columns should be at [0, 1], partition columns at [2, 3, 4]
        assert_eq!(split.remapped_projection.iter().count(), 5);
    }

    #[test]
    fn test_split_projection_partition_columns_reverse_order() {
        let (file_schema, table_schema) = create_test_schemas(2, 2);
        // File cols: 0, 1; Partition cols: 2, 3
        // Select: [3, 2] (partitions in reverse)
        let projection = ProjectionExprs::from_indices(&[3, 2], &table_schema);

        let split = SplitProjection::new(&file_schema, &projection);

        assert_eq!(split.file_indices, Vec::<usize>::new());
        assert_eq!(split.partition_columns.len(), 2);
        assert_eq!(split.partition_columns[0].in_partition_values, 0);
        assert_eq!(split.partition_columns[1].in_partition_values, 1);
    }

    #[test]
    fn test_split_projection_interleaved_file_and_partition() {
        let (file_schema, table_schema) = create_test_schemas(3, 3);
        // File cols: 0, 1, 2; Partition cols: 3, 4, 5
        // Select: [0, 3, 1, 4, 2, 5] (alternating)
        let projection =
            ProjectionExprs::from_indices(&[0, 3, 1, 4, 2, 5], &table_schema);

        let split = SplitProjection::new(&file_schema, &projection);

        assert_eq!(split.file_indices, vec![0, 1, 2]);
        assert_eq!(split.partition_columns.len(), 3);
        assert_eq!(split.partition_columns[0].in_partition_values, 0);
        assert_eq!(split.partition_columns[1].in_partition_values, 1);
        assert_eq!(split.partition_columns[2].in_partition_values, 2);
    }

    #[test]
    fn test_split_projection_expression_with_file_and_partition_columns() {
        use arrow::datatypes::Field;

        // Create schemas: 2 file columns, 1 partition column
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("file_a", DataType::Int32, false),
            Field::new("file_b", DataType::Int32, false),
        ]));
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("file_a", DataType::Int32, false),
            Field::new("file_b", DataType::Int32, false),
            Field::new("part_c", DataType::Int32, false),
        ]));

        // Create expression: file_a + part_c
        let exprs = [col("file_a") + col("part_c")];
        let projection = create_projection_exprs(exprs.iter(), &table_schema);

        let split = SplitProjection::new(&file_schema, &projection);

        // Should extract both columns
        assert_eq!(split.file_indices, vec![0]);
        assert_eq!(split.partition_columns.len(), 1);
        assert_eq!(split.partition_columns[0].in_partition_values, 0);
    }

    // ========================================================================
    // Category 4: Boundary Conditions
    // ========================================================================

    #[test]
    fn test_split_projection_boundary_last_file_column() {
        let (file_schema, table_schema) = create_test_schemas(3, 2);
        // Last file column is index 2
        let projection = ProjectionExprs::from_indices(&[2], &table_schema);

        let split = SplitProjection::new(&file_schema, &projection);

        assert_eq!(split.file_indices, vec![2]);
        assert_eq!(split.partition_columns.len(), 0);
    }

    #[test]
    fn test_split_projection_boundary_first_partition_column() {
        let (file_schema, table_schema) = create_test_schemas(3, 2);
        // First partition column is index 3
        let projection = ProjectionExprs::from_indices(&[3], &table_schema);

        let split = SplitProjection::new(&file_schema, &projection);

        assert_eq!(split.file_indices, Vec::<usize>::new());
        assert_eq!(split.partition_columns.len(), 1);
        assert_eq!(split.partition_columns[0].in_partition_values, 0);
    }

    // ========================================================================
    // Category 6: Integration Tests
    // ========================================================================

    #[test]
    fn test_inject_partition_columns_multiple_partitions() {
        let data =
            record_batch!(("col_0", Int32, vec![1]), ("col_1", Int32, vec![2])).unwrap();

        // Create projection that references file columns and partition columns
        let (file_schema, table_schema) = create_test_schemas(2, 2);
        // Projection: [0, 2, 1, 3] = [file_0, part_0, file_1, part_1]
        let projection = ProjectionExprs::from_indices(&[0, 2, 1, 3], &table_schema);
        let split = SplitProjection::new(&file_schema, &projection);

        // Create partition column mappings
        let partition_columns = vec![
            PartitionColumnIndex {
                in_remainder_projection: 2, // First partition column at index 2
                in_partition_values: 0,
            },
            PartitionColumnIndex {
                in_remainder_projection: 3, // Second partition column at index 3
                in_partition_values: 1,
            },
        ];

        let partition_values =
            vec![ScalarValue::from("part_a"), ScalarValue::from("part_b")];

        let injected = inject_partition_columns_into_projection(
            &split.remapped_projection,
            &partition_columns,
            partition_values,
        );

        // Apply projection
        let projector = injected.make_projector(&data.schema()).unwrap();
        let result = projector.project_batch(&data).unwrap();

        assert_eq!(result.num_columns(), 4);
        assert_eq!(
            result
                .column(0)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(0),
            1
        );
        assert_eq!(result.column(1).as_string::<i32>().value(0), "part_a");
        assert_eq!(
            result
                .column(2)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(0),
            2
        );
        assert_eq!(result.column(3).as_string::<i32>().value(0), "part_b");
    }
}
