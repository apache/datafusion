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

//! This file contains common subroutines for regular and symmetric hash join
//! related functionality, used both in join calculations and optimization rules.

use std::collections::HashMap;
use std::sync::Arc;
use std::{fmt, usize};

use arrow::datatypes::SchemaRef;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::intervals::Interval;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use hashbrown::raw::RawTable;
use smallvec::SmallVec;

use crate::physical_plan::joins::utils::{JoinFilter, JoinSide};
use datafusion_common::Result;

// Maps a `u64` hash value based on the build side ["on" values] to a list of indices with this key's value.
// By allocating a `HashMap` with capacity for *at least* the number of rows for entries at the build side,
// we make sure that we don't have to re-hash the hashmap, which needs access to the key (the hash in this case) value.
// E.g. 1 -> [3, 6, 8] indicates that the column values map to rows 3, 6 and 8 for hash value 1
// As the key is a hash value, we need to check possible hash collisions in the probe stage
// During this stage it might be the case that a row is contained the same hashmap value,
// but the values don't match. Those are checked in the [equal_rows] macro
// The indices (values) are stored in a separate chained list stored in the `Vec<u64>`.
// The first value (+1) is stored in the hashmap, whereas the next value is stored in array at the position value.
// The chain can be followed until the value "0" has been reached, meaning the end of the list.
// Also see chapter 5.3 of [Balancing vectorized query execution with bandwidth-optimized storage](https://dare.uva.nl/search?identifier=5ccbb60a-38b8-4eeb-858a-e7735dd37487)
// TODO: speed up collision checks
// https://github.com/apache/arrow-datafusion/issues/50
pub struct JoinHashMap {
    pub map: RawTable<(u64, u64)>,
    pub next: Vec<u64>,
}

/// SymmetricJoinHashMap is similar to JoinHashMap, except that it stores the indices inline, allowing it to mutate
/// and shrink the indices.
pub struct SymmetricJoinHashMap(pub RawTable<(u64, SmallVec<[u64; 1]>)>);

impl JoinHashMap {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        JoinHashMap {
            map: RawTable::with_capacity(capacity),
            next: vec![0; capacity],
        }
    }
}

impl SymmetricJoinHashMap {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self(RawTable::with_capacity(capacity))
    }

    /// In this implementation, the scale_factor variable determines how conservative the shrinking strategy is.
    /// The value of scale_factor is set to 4, which means the capacity will be reduced by 25%
    /// when necessary. You can adjust the scale_factor value to achieve the desired
    /// balance between memory usage and performance.
    //
    // If you increase the scale_factor, the capacity will shrink less aggressively,
    // leading to potentially higher memory usage but fewer resizes.
    // Conversely, if you decrease the scale_factor, the capacity will shrink more aggressively,
    // potentially leading to lower memory usage but more frequent resizing.
    pub(crate) fn shrink_if_necessary(&mut self, scale_factor: usize) {
        let capacity = self.0.capacity();
        let len = self.0.len();

        if capacity > scale_factor * len {
            let new_capacity = (capacity * (scale_factor - 1)) / scale_factor;
            self.0.shrink_to(new_capacity, |(hash, _)| *hash)
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.0.allocation_info().1.size()
    }
}

impl fmt::Debug for JoinHashMap {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

fn check_filter_expr_contains_sort_information(
    expr: &Arc<dyn PhysicalExpr>,
    reference: &Arc<dyn PhysicalExpr>,
) -> bool {
    expr.eq(reference)
        || expr
            .children()
            .iter()
            .any(|e| check_filter_expr_contains_sort_information(e, reference))
}

/// Create a one to one mapping from main columns to filter columns using
/// filter column indices. A column index looks like:
/// ```text
/// ColumnIndex {
///     index: 0, // field index in main schema
///     side: JoinSide::Left, // child side
/// }
/// ```
pub fn map_origin_col_to_filter_col(
    filter: &JoinFilter,
    schema: &SchemaRef,
    side: &JoinSide,
) -> Result<HashMap<Column, Column>> {
    let filter_schema = filter.schema();
    let mut col_to_col_map: HashMap<Column, Column> = HashMap::new();
    for (filter_schema_index, index) in filter.column_indices().iter().enumerate() {
        if index.side.eq(side) {
            // Get the main field from column index:
            let main_field = schema.field(index.index);
            // Create a column expression:
            let main_col = Column::new_with_schema(main_field.name(), schema.as_ref())?;
            // Since the order of by filter.column_indices() is the same with
            // that of intermediate schema fields, we can get the column directly.
            let filter_field = filter_schema.field(filter_schema_index);
            let filter_col = Column::new(filter_field.name(), filter_schema_index);
            // Insert mapping:
            col_to_col_map.insert(main_col, filter_col);
        }
    }
    Ok(col_to_col_map)
}

/// This function analyzes [`PhysicalSortExpr`] graphs with respect to monotonicity
/// (sorting) properties. This is necessary since monotonically increasing and/or
/// decreasing expressions are required when using join filter expressions for
/// data pruning purposes.
///
/// The method works as follows:
/// 1. Maps the original columns to the filter columns using the [`map_origin_col_to_filter_col`] function.
/// 2. Collects all columns in the sort expression using the [`collect_columns`] function.
/// 3. Checks if all columns are included in the map we obtain in the first step.
/// 4. If all columns are included, the sort expression is converted into a filter expression using
///    the [`convert_filter_columns`] function.
/// 5. Searches for the converted filter expression in the filter expression using the
///    [`check_filter_expr_contains_sort_information`] function.
/// 6. If an exact match is found, returns the converted filter expression as [`Some(Arc<dyn PhysicalExpr>)`].
/// 7. If all columns are not included or an exact match is not found, returns [`None`].
///
/// Examples:
/// Consider the filter expression "a + b > c + 10 AND a + b < c + 100".
/// 1. If the expression "a@ + d@" is sorted, it will not be accepted since the "d@" column is not part of the filter.
/// 2. If the expression "d@" is sorted, it will not be accepted since the "d@" column is not part of the filter.
/// 3. If the expression "a@ + b@ + c@" is sorted, all columns are represented in the filter expression. However,
///    there is no exact match, so this expression does not indicate pruning.
pub fn convert_sort_expr_with_filter_schema(
    side: &JoinSide,
    filter: &JoinFilter,
    schema: &SchemaRef,
    sort_expr: &PhysicalSortExpr,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let column_map = map_origin_col_to_filter_col(filter, schema, side)?;
    let expr = sort_expr.expr.clone();
    // Get main schema columns:
    let expr_columns = collect_columns(&expr);
    // Calculation is possible with `column_map` since sort exprs belong to a child.
    let all_columns_are_included =
        expr_columns.iter().all(|col| column_map.contains_key(col));
    if all_columns_are_included {
        // Since we are sure that one to one column mapping includes all columns, we convert
        // the sort expression into a filter expression.
        let converted_filter_expr = expr.transform_up(&|p| {
            convert_filter_columns(p.as_ref(), &column_map).map(|transformed| {
                match transformed {
                    Some(transformed) => Transformed::Yes(transformed),
                    None => Transformed::No(p),
                }
            })
        })?;
        // Search the converted `PhysicalExpr` in filter expression; if an exact
        // match is found, use this sorted expression in graph traversals.
        if check_filter_expr_contains_sort_information(
            filter.expression(),
            &converted_filter_expr,
        ) {
            return Ok(Some(converted_filter_expr));
        }
    }
    Ok(None)
}

/// This function is used to build the filter expression based on the sort order of input columns.
///
/// It first calls the [`convert_sort_expr_with_filter_schema`] method to determine if the sort
/// order of columns can be used in the filter expression. If it returns a [`Some`] value, the
/// method wraps the result in a [`SortedFilterExpr`] instance with the original sort expression and
/// the converted filter expression. Otherwise, this function returns an error.
///
/// The `SortedFilterExpr` instance contains information about the sort order of columns that can
/// be used in the filter expression, which can be used to optimize the query execution process.
pub fn build_filter_input_order(
    side: JoinSide,
    filter: &JoinFilter,
    schema: &SchemaRef,
    order: &PhysicalSortExpr,
) -> Result<Option<SortedFilterExpr>> {
    let opt_expr = convert_sort_expr_with_filter_schema(&side, filter, schema, order)?;
    Ok(opt_expr.map(|filter_expr| SortedFilterExpr::new(order.clone(), filter_expr)))
}

/// Convert a physical expression into a filter expression using the given
/// column mapping information.
fn convert_filter_columns(
    input: &dyn PhysicalExpr,
    column_map: &HashMap<Column, Column>,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // Attempt to downcast the input expression to a Column type.
    Ok(if let Some(col) = input.as_any().downcast_ref::<Column>() {
        // If the downcast is successful, retrieve the corresponding filter column.
        column_map.get(col).map(|c| Arc::new(c.clone()) as _)
    } else {
        // If the downcast fails, return the input expression as is.
        None
    })
}

/// The [SortedFilterExpr] object represents a sorted filter expression. It
/// contains the following information: The origin expression, the filter
/// expression, an interval encapsulating expression bounds, and a stable
/// index identifying the expression in the expression DAG.
///
/// Physical schema of a [JoinFilter]'s intermediate batch combines two sides
/// and uses new column names. In this process, a column exchange is done so
/// we can utilize sorting information while traversing the filter expression
/// DAG for interval calculations. When evaluating the inner buffer, we use
/// `origin_sorted_expr`.
#[derive(Debug, Clone)]
pub struct SortedFilterExpr {
    /// Sorted expression from a join side (i.e. a child of the join)
    origin_sorted_expr: PhysicalSortExpr,
    /// Expression adjusted for filter schema.
    filter_expr: Arc<dyn PhysicalExpr>,
    /// Interval containing expression bounds
    interval: Interval,
    /// Node index in the expression DAG
    node_index: usize,
}

impl SortedFilterExpr {
    /// Constructor
    pub fn new(
        origin_sorted_expr: PhysicalSortExpr,
        filter_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            origin_sorted_expr,
            filter_expr,
            interval: Interval::default(),
            node_index: 0,
        }
    }
    /// Get origin expr information
    pub fn origin_sorted_expr(&self) -> &PhysicalSortExpr {
        &self.origin_sorted_expr
    }
    /// Get filter expr information
    pub fn filter_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.filter_expr
    }
    /// Get interval information
    pub fn interval(&self) -> &Interval {
        &self.interval
    }
    /// Sets interval
    pub fn set_interval(&mut self, interval: Interval) {
        self.interval = interval;
    }
    /// Node index in ExprIntervalGraph
    pub fn node_index(&self) -> usize {
        self.node_index
    }
    /// Node index setter in ExprIntervalGraph
    pub fn set_node_index(&mut self, node_index: usize) {
        self.node_index = node_index;
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::physical_plan::{
        expressions::Column,
        expressions::PhysicalSortExpr,
        joins::utils::{ColumnIndex, JoinFilter, JoinSide},
    };
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, cast, col, lit};
    use smallvec::smallvec;
    use std::sync::Arc;

    /// Filter expr for a + b > c + 10 AND a + b < c + 100
    pub(crate) fn complicated_filter(
        filter_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let left_expr = binary(
            cast(
                binary(
                    col("0", filter_schema)?,
                    Operator::Plus,
                    col("1", filter_schema)?,
                    filter_schema,
                )?,
                filter_schema,
                DataType::Int64,
            )?,
            Operator::Gt,
            binary(
                cast(col("2", filter_schema)?, filter_schema, DataType::Int64)?,
                Operator::Plus,
                lit(ScalarValue::Int64(Some(10))),
                filter_schema,
            )?,
            filter_schema,
        )?;

        let right_expr = binary(
            cast(
                binary(
                    col("0", filter_schema)?,
                    Operator::Plus,
                    col("1", filter_schema)?,
                    filter_schema,
                )?,
                filter_schema,
                DataType::Int64,
            )?,
            Operator::Lt,
            binary(
                cast(col("2", filter_schema)?, filter_schema, DataType::Int64)?,
                Operator::Plus,
                lit(ScalarValue::Int64(Some(100))),
                filter_schema,
            )?,
            filter_schema,
        )?;
        binary(left_expr, Operator::And, right_expr, filter_schema)
    }

    #[test]
    fn test_column_exchange() -> Result<()> {
        let left_child_schema =
            Schema::new(vec![Field::new("left_1", DataType::Int32, true)]);
        // Sorting information for the left side:
        let left_child_sort_expr = PhysicalSortExpr {
            expr: col("left_1", &left_child_schema)?,
            options: SortOptions::default(),
        };

        let right_child_schema = Schema::new(vec![
            Field::new("right_1", DataType::Int32, true),
            Field::new("right_2", DataType::Int32, true),
        ]);
        // Sorting information for the right side:
        let right_child_sort_expr = PhysicalSortExpr {
            expr: binary(
                col("right_1", &right_child_schema)?,
                Operator::Plus,
                col("right_2", &right_child_schema)?,
                &right_child_schema,
            )?,
            options: SortOptions::default(),
        };

        let intermediate_schema = Schema::new(vec![
            Field::new("filter_1", DataType::Int32, true),
            Field::new("filter_2", DataType::Int32, true),
            Field::new("filter_3", DataType::Int32, true),
        ]);
        // Our filter expression is: left_1 > right_1 + right_2.
        let filter_left = col("filter_1", &intermediate_schema)?;
        let filter_right = binary(
            col("filter_2", &intermediate_schema)?,
            Operator::Plus,
            col("filter_3", &intermediate_schema)?,
            &intermediate_schema,
        )?;
        let filter_expr = binary(
            filter_left.clone(),
            Operator::Gt,
            filter_right.clone(),
            &intermediate_schema,
        )?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        let left_sort_filter_expr = build_filter_input_order(
            JoinSide::Left,
            &filter,
            &Arc::new(left_child_schema),
            &left_child_sort_expr,
        )?
        .unwrap();
        assert!(left_child_sort_expr.eq(left_sort_filter_expr.origin_sorted_expr()));

        let right_sort_filter_expr = build_filter_input_order(
            JoinSide::Right,
            &filter,
            &Arc::new(right_child_schema),
            &right_child_sort_expr,
        )?
        .unwrap();
        assert!(right_child_sort_expr.eq(right_sort_filter_expr.origin_sorted_expr()));

        // Assert that adjusted (left) filter expression matches with `left_child_sort_expr`:
        assert!(filter_left.eq(left_sort_filter_expr.filter_expr()));
        // Assert that adjusted (right) filter expression matches with `right_child_sort_expr`:
        assert!(filter_right.eq(right_sort_filter_expr.filter_expr()));
        Ok(())
    }

    #[test]
    fn test_column_collector() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&schema)?;
        let columns = collect_columns(&filter_expr);
        assert_eq!(columns.len(), 3);
        Ok(())
    }

    #[test]
    fn find_expr_inside_expr() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&schema)?;

        let expr_1 = Arc::new(Column::new("gnz", 0)) as _;
        assert!(!check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_1
        ));

        let expr_2 = col("1", &schema)? as _;

        assert!(check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_2
        ));

        let expr_3 = cast(
            binary(
                col("0", &schema)?,
                Operator::Plus,
                col("1", &schema)?,
                &schema,
            )?,
            &schema,
            DataType::Int64,
        )?;

        assert!(check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_3
        ));

        let expr_4 = Arc::new(Column::new("1", 42)) as _;

        assert!(!check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_4,
        ));
        Ok(())
    }

    #[test]
    fn build_sorted_expr() -> Result<()> {
        let left_schema = Schema::new(vec![
            Field::new("la1", DataType::Int32, false),
            Field::new("lb1", DataType::Int32, false),
            Field::new("lc1", DataType::Int32, false),
            Field::new("lt1", DataType::Int32, false),
            Field::new("la2", DataType::Int32, false),
            Field::new("la1_des", DataType::Int32, false),
        ]);

        let right_schema = Schema::new(vec![
            Field::new("ra1", DataType::Int32, false),
            Field::new("rb1", DataType::Int32, false),
            Field::new("rc1", DataType::Int32, false),
            Field::new("rt1", DataType::Int32, false),
            Field::new("ra2", DataType::Int32, false),
            Field::new("ra1_des", DataType::Int32, false),
        ]);

        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let filter_expr = complicated_filter(&intermediate_schema)?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 4,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        let left_schema = Arc::new(left_schema);
        let right_schema = Arc::new(right_schema);

        assert!(build_filter_input_order(
            JoinSide::Left,
            &filter,
            &left_schema,
            &PhysicalSortExpr {
                expr: col("la1", left_schema.as_ref())?,
                options: SortOptions::default(),
            }
        )?
        .is_some());
        assert!(build_filter_input_order(
            JoinSide::Left,
            &filter,
            &left_schema,
            &PhysicalSortExpr {
                expr: col("lt1", left_schema.as_ref())?,
                options: SortOptions::default(),
            }
        )?
        .is_none());
        assert!(build_filter_input_order(
            JoinSide::Right,
            &filter,
            &right_schema,
            &PhysicalSortExpr {
                expr: col("ra1", right_schema.as_ref())?,
                options: SortOptions::default(),
            }
        )?
        .is_some());
        assert!(build_filter_input_order(
            JoinSide::Right,
            &filter,
            &right_schema,
            &PhysicalSortExpr {
                expr: col("rb1", right_schema.as_ref())?,
                options: SortOptions::default(),
            }
        )?
        .is_none());

        Ok(())
    }

    // Test the case when we have an "ORDER BY a + b", and join filter condition includes "a - b".
    #[test]
    fn sorted_filter_expr_build() -> Result<()> {
        let intermediate_schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
        ]);
        let filter_expr = binary(
            col("0", &intermediate_schema)?,
            Operator::Minus,
            col("1", &intermediate_schema)?,
            &intermediate_schema,
        )?;
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
        ];
        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let sorted = PhysicalSortExpr {
            expr: binary(
                col("a", &schema)?,
                Operator::Plus,
                col("b", &schema)?,
                &schema,
            )?,
            options: SortOptions::default(),
        };

        let res = convert_sort_expr_with_filter_schema(
            &JoinSide::Left,
            &filter,
            &Arc::new(schema),
            &sorted,
        )?;
        assert!(res.is_none());
        Ok(())
    }

    #[test]
    fn test_shrink_if_necessary() {
        let scale_factor = 4;
        let mut join_hash_map = SymmetricJoinHashMap::with_capacity(100);
        let data_size = 2000;
        let deleted_part = 3 * data_size / 4;
        // Add elements to the JoinHashMap
        for hash_value in 0..data_size {
            join_hash_map.0.insert(
                hash_value,
                (hash_value, smallvec![hash_value]),
                |(hash, _)| *hash,
            );
        }

        assert_eq!(join_hash_map.0.len(), data_size as usize);
        assert!(join_hash_map.0.capacity() >= data_size as usize);

        // Remove some elements from the JoinHashMap
        for hash_value in 0..deleted_part {
            join_hash_map
                .0
                .remove_entry(hash_value, |(hash, _)| hash_value == *hash);
        }

        assert_eq!(join_hash_map.0.len(), (data_size - deleted_part) as usize);

        // Old capacity
        let old_capacity = join_hash_map.0.capacity();

        // Test shrink_if_necessary
        join_hash_map.shrink_if_necessary(scale_factor);

        // The capacity should be reduced by the scale factor
        let new_expected_capacity =
            join_hash_map.0.capacity() * (scale_factor - 1) / scale_factor;
        assert!(join_hash_map.0.capacity() >= new_expected_capacity);
        assert!(join_hash_map.0.capacity() <= old_capacity);
    }
}
