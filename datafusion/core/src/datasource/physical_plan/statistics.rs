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

/*!
 *
 * Use statistics to optimize physical planning.
 *
 * Currently, this module houses code to sort file groups if they are non-overlapping with
 * respect to the required sort order. See [`MinMaxStatistics`]
 *
*/

use std::sync::Arc;

use arrow::{
    compute::SortColumn,
    row::{Row, Rows},
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::{expressions::Column, PhysicalSortExpr};

use crate::datasource::listing::PartitionedFile;

/// A normalized representation of file min/max statistics that allows for efficient sorting & comparison.
/// The min/max values are ordered by [`Self::sort_order`].
/// Furthermore, any columns that are reversed in the sort order have their min/max values swapped.
pub(crate) struct MinMaxStatistics {
    min_by_sort_order: Rows,
    max_by_sort_order: Rows,
    sort_order: Vec<PhysicalSortExpr>,
}

impl MinMaxStatistics {
    /// Sort order used to sort the statistics
    #[allow(unused)]
    pub fn sort_order(&self) -> &[PhysicalSortExpr] {
        &self.sort_order
    }

    /// Min value at index
    #[allow(unused)]
    pub fn min(&self, idx: usize) -> Row {
        self.min_by_sort_order.row(idx)
    }

    /// Max value at index
    pub fn max(&self, idx: usize) -> Row {
        self.max_by_sort_order.row(idx)
    }

    pub fn new_from_files<'a>(
        projected_sort_order: &[PhysicalSortExpr], // Sort order with respect to projected schema
        projected_schema: &SchemaRef,              // Projected schema
        projection: Option<&[usize]>, // Indices of projection in full table schema (None = all columns)
        files: impl IntoIterator<Item = &'a PartitionedFile>,
    ) -> Result<Self> {
        use datafusion_common::ScalarValue;

        let statistics_and_partition_values = files
            .into_iter()
            .map(|file| {
                file.statistics
                    .as_ref()
                    .zip(Some(file.partition_values.as_slice()))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DataFusionError::Plan("Parquet file missing statistics".to_string())
            })?;

        // Helper function to get min/max statistics for a given column of projected_schema
        let get_min_max = |i: usize| -> Result<(Vec<ScalarValue>, Vec<ScalarValue>)> {
            Ok(statistics_and_partition_values
                .iter()
                .map(|(s, pv)| {
                    if i < s.column_statistics.len() {
                        s.column_statistics[i]
                            .min_value
                            .get_value()
                            .cloned()
                            .zip(s.column_statistics[i].max_value.get_value().cloned())
                            .ok_or_else(|| {
                                DataFusionError::Plan("statistics not found".to_string())
                            })
                    } else {
                        let partition_value = &pv[i - s.column_statistics.len()];
                        Ok((partition_value.clone(), partition_value.clone()))
                    }
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip())
        };

        let sort_columns = sort_columns_from_physical_sort_exprs(projected_sort_order)
            .ok_or(DataFusionError::Plan(
                "sort expression must be on column".to_string(),
            ))?;

        // Project the schema & sort order down to just the relevant columns
        let min_max_schema = Arc::new(
            projected_schema
                .project(&(sort_columns.iter().map(|c| c.index()).collect::<Vec<_>>()))?,
        );
        let min_max_sort_order = sort_columns
            .iter()
            .zip(projected_sort_order.iter())
            .enumerate()
            .map(|(i, (col, sort))| PhysicalSortExpr {
                expr: Arc::new(Column::new(col.name(), i)),
                options: sort.options,
            })
            .collect::<Vec<_>>();

        let (min_values, max_values): (Vec<_>, Vec<_>) = sort_columns
            .iter()
            .map(|c| {
                // Reverse the projection to get the index of the column in the full statistics
                // The file statistics contains _every_ column , but the sort column's index()
                // refers to the index in projected_schema
                let i = projection.map(|p| p[c.index()]).unwrap_or(c.index());

                let (min, max) = get_min_max(i).map_err(|e| {
                    e.context(format!("get min/max for column: '{}'", c.name()))
                })?;
                Ok((
                    ScalarValue::iter_to_array(min)?,
                    ScalarValue::iter_to_array(max)?,
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| e.context("collect min/max values"))?
            .into_iter()
            .unzip();

        Self::new(
            &min_max_sort_order,
            &min_max_schema,
            RecordBatch::try_new(Arc::clone(&min_max_schema), min_values).map_err(
                |e| {
                    DataFusionError::ArrowError(e, Some("\ncreate min batch".to_string()))
                },
            )?,
            RecordBatch::try_new(Arc::clone(&min_max_schema), max_values).map_err(
                |e| {
                    DataFusionError::ArrowError(e, Some("\ncreate max batch".to_string()))
                },
            )?,
        )
    }

    pub fn new(
        sort_order: &[PhysicalSortExpr],
        schema: &SchemaRef,
        min_values: RecordBatch,
        max_values: RecordBatch,
    ) -> Result<Self> {
        use arrow::row::*;

        let sort_fields = sort_order
            .iter()
            .map(|expr| {
                expr.expr
                    .data_type(schema)
                    .map(|data_type| SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| e.context("create sort fields"))?;
        let converter = RowConverter::new(sort_fields)?;

        let sort_columns = sort_columns_from_physical_sort_exprs(sort_order).ok_or(
            DataFusionError::Plan("sort expression must be on column".to_string()),
        )?;

        // swap min/max if they're reversed in the ordering
        let (new_min_cols, new_max_cols): (Vec<_>, Vec<_>) = sort_order
            .iter()
            .zip(sort_columns.iter().copied())
            .map(|(sort_expr, column)| {
                if sort_expr.options.descending {
                    max_values
                        .column_by_name(column.name())
                        .zip(min_values.column_by_name(column.name()))
                } else {
                    min_values
                        .column_by_name(column.name())
                        .zip(max_values.column_by_name(column.name()))
                }
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "missing column in MinMaxStatistics::new: '{}'",
                        column.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        let [min, max] = [new_min_cols, new_max_cols].map(|cols| {
            let values = RecordBatch::try_new(
                min_values.schema(),
                cols.into_iter().cloned().collect(),
            )?;
            let sorting_columns = sort_order
                .iter()
                .zip(sort_columns.iter().copied())
                .map(|(sort_expr, column)| {
                    let schema = values.schema();

                    let idx = schema.index_of(column.name())?;
                    let field = schema.field(idx);

                    // check that sort columns are non-nullable
                    if field.is_nullable() {
                        return Err(DataFusionError::Plan(
                            "cannot sort by nullable column".to_string(),
                        ));
                    }

                    Ok(SortColumn {
                        values: Arc::clone(values.column(idx)),
                        options: Some(sort_expr.options),
                    })
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|e| e.context("create sorting columns"))?;
            converter
                .convert_columns(
                    &sorting_columns
                        .into_iter()
                        .map(|c| c.values)
                        .collect::<Vec<_>>(),
                )
                .map_err(|e| {
                    DataFusionError::ArrowError(e, Some("convert columns".to_string()))
                })
        });

        Ok(Self {
            min_by_sort_order: min.map_err(|e| e.context("build min rows"))?,
            max_by_sort_order: max.map_err(|e| e.context("build max rows"))?,
            sort_order: sort_order.to_vec(),
        })
    }

    /// Return a sorted list of the min statistics together with the original indices
    pub fn min_values_sorted(&self) -> Vec<(usize, Row<'_>)> {
        let mut sort: Vec<_> = self.min_by_sort_order.iter().enumerate().collect();
        sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
        sort
    }

    /// Check if the min/max statistics are in order and non-overlapping
    pub fn is_sorted(&self) -> bool {
        self.max_by_sort_order
            .iter()
            .zip(self.min_by_sort_order.iter().skip(1))
            .all(|(max, next_min)| max < next_min)
    }
}

fn sort_columns_from_physical_sort_exprs(
    sort_order: &[PhysicalSortExpr],
) -> Option<Vec<&Column>> {
    sort_order
        .iter()
        .map(|expr| expr.expr.as_any().downcast_ref::<Column>())
        .collect::<Option<Vec<_>>>()
}
