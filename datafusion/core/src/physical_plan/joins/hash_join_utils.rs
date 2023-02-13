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
use std::usize;

use arrow::datatypes::SchemaRef;

use datafusion_common::DataFusionError;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::intervals::Interval;
use datafusion_physical_expr::rewrite::TreeNodeRewritable;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

use crate::common::Result;
use crate::physical_plan::joins::utils::{JoinFilter, JoinSide};

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

fn collect_columns_recursive(expr: &Arc<dyn PhysicalExpr>, columns: &mut Vec<Column>) {
    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
        if !columns.iter().any(|c| c.eq(column)) {
            columns.push(column.clone())
        }
    }
    expr.children()
        .iter()
        .for_each(|e| collect_columns_recursive(e, columns))
}

fn collect_columns(expr: &Arc<dyn PhysicalExpr>) -> Vec<Column> {
    let mut columns = vec![];
    collect_columns_recursive(expr, &mut columns);
    columns
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

/// This function analyzes [PhysicalSortExpr] graphs with respect to monotonicity
/// (sorting) properties. This is necessary since monotonically increasing and/or
/// decreasing expressions are required when using join filter expressions for
/// data pruning purposes.
///
/// The method works as follows:
/// 1. Maps the original columns to the filter columns using the `map_origin_col_to_filter_col` function.
/// 2. Collects all columns in the sort expression using the `PhysicalExprColumnCollector` visitor.
/// 3. Checks if all columns are included in the `column_mapping_information` map.
/// 4. If all columns are included, the sort expression is converted into a filter expression using the `transform_up` and `convert_filter_columns` functions.
/// 5. Searches the converted filter expression in the filter expression using the `check_filter_expr_contains_sort_information`.
/// 6. If an exact match is encountered, returns the converted filter expression as `Some(Arc<dyn PhysicalExpr>)`.
/// 7. If all columns are not included or the exact match is not encountered, returns `None`.
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
        let converted_filter_expr =
            expr.transform_up(&|p| convert_filter_columns(p, &column_map))?;
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
/// It first calls the [convert_sort_expr_with_filter_schema] method to determine if the sort
/// order of columns can be used in the filter expression. If it returns a [Some] value, the
/// method wraps the result in a [SortedFilterExpr] instance with the original sort expression and
/// the converted filter expression. Otherwise, this function returns an error.
///
/// The [SortedFilterExpr] instance contains information about the sort order of columns that can
/// be used in the filter expression, which can be used to optimize the query execution process.
pub fn build_filter_input_order(
    side: JoinSide,
    filter: &JoinFilter,
    schema: &SchemaRef,
    order: &PhysicalSortExpr,
) -> Result<SortedFilterExpr> {
    if let Some(expr) =
        convert_sort_expr_with_filter_schema(&side, filter, schema, order)?
    {
        Ok(SortedFilterExpr::new(order.clone(), expr))
    } else {
        Err(DataFusionError::Plan(format!(
            "The {side} side of the join does not have an expression sorted."
        )))
    }
}

/// Convert a physical expression into a filter expression using the given
/// column mapping information.
fn convert_filter_columns(
    input: Arc<dyn PhysicalExpr>,
    column_map: &HashMap<Column, Column>,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // Attempt to downcast the input expression to a Column type.
    Ok(if let Some(col) = input.as_any().downcast_ref::<Column>() {
        // If the downcast is successful, retrieve the corresponding filter column.
        column_map.get(col).map(|c| Arc::new(c.clone()) as _)
    } else {
        // If the downcast fails, return the input expression as is.
        Some(input)
    })
}

/// The [SortedFilterExpr] object represents a sorted filter expression. It
/// contains the following information: The origin expression, the filter
/// expression, an interval encapsulating expression bounds, and a stable
/// index identifying the expression in the expression DAG.
///
/// Physical schema of intermediate batch of a JoinFilter combines two sides with new column names.
/// For sort information, we do the column exchange, to traverse the main filter expression.
///
/// For evaluate the inner buffer, we use origin_sorted_expr.
/// For interval traversing, we use filter_expr, adjusted with intermediate schema.
///
/// ```
/// use std::sync::Arc;
/// use arrow::compute::SortOptions;
/// use arrow::datatypes::{Field, Schema, DataType};
/// use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter, JoinSide};
/// use datafusion_expr::Operator;
/// use datafusion_physical_expr::expressions::{Column, BinaryExpr};
/// use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
/// use super::*;
///
/// let left_child_schema = Arc::new(Schema::new(vec![Field::new(
///             "left_1",
///             DataType::Int32,
///             true,
///  )]));
/// // Will be origin expr
/// let left_child_sort_expr = PhysicalSortExpr {
///             expr: Arc::new(Column::new("left_1", 0)),
///             options: SortOptions::default(),
///  };
///
///  let right_child_schema = Arc::new(Schema::new(vec![
///     Field::new("right_1", DataType::Int32, true),
///     Field::new("right_2", DataType::Int32, true),
///  ]));
///  // Will be origin expr
///  let right_child_sort_expr = PhysicalSortExpr {
///     expr: Arc::new(BinaryExpr::new(
///         Arc::new(Column::new("right_1", 0)),
///         Operator::Plus,
///         Arc::new(Column::new("right_2", 1)),
///  )),
///     options: SortOptions::default(),
///  };
///
///  let filter_col_1 = Arc::new(Column::new("filter_1", 0));
///  let filter_col_2 = Arc::new(Column::new("filter_2", 1));
///  let filter_col_3 = Arc::new(Column::new("filter_3", 2));
///
///  let column_indices = vec![
///     ColumnIndex {
///         index: 0,
///         side: JoinSide::Left,
///     },
///     ColumnIndex {
///         index: 0,
///         side: JoinSide::Right,
///     },
///     ColumnIndex {
///         index: 1,
///         side: JoinSide::Right,
///     },
///  ];
///  let intermediate_schema = Schema::new(vec![
///             Field::new(filter_col_1.name(), DataType::Int32, true),
///             Field::new(filter_col_2.name(), DataType::Int32, true),
///             Field::new(filter_col_3.name(), DataType::Int32, true),
///  ]);
///  // left_1 > right_1 + right_2
///  let filter_expr = Arc::new(BinaryExpr::new(
///     filter_col_1,
///     Operator::Gt,
///     Arc::new(BinaryExpr::new(filter_col_2, Operator::Plus, filter_col_3)),
///  ));
///  let filter = JoinFilter::new(
///     filter_expr,
///     column_indices.clone(),
///     intermediate_schema.clone(),
///  );
///
///  let left_sort_filter_expr = build_filter_input_order(
///     JoinSide::Left,
///     &filter,
///     left_child_schema,
///     &left_child_sort_expr,
///  )?;
///  let right_sort_filter_expr = build_filter_input_order(
///     JoinSide::Right,
///     &filter,
///     right_child_schema,
///     &right_child_sort_expr,
///  )?;
///
///  assert_eq!(
///     left_sort_filter_expr.origin_sorted_expr(),
///     left_child_sort_expr
///  );
/// // Matches with left_child_sort_expr expression.
///  let expected_filter_expr: Arc<dyn PhysicalExpr> =
///             Arc::new(Column::new("filter_1", 0));
///  assert!(expected_filter_expr.eq(left_sort_filter_expr.filter_expr()));
///  assert_eq!(
///     right_sort_filter_expr.origin_sorted_expr(),
///     right_child_sort_expr
///  );
///  // Matches with right_child_sort_expr expression.
///  let expected_filter_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
///     Arc::new(Column::new("filter_2", 1)),
///     Operator::Plus,
///     Arc::new(Column::new("filter_3", 2)),
///  ));
///  assert!(expected_filter_expr.eq(right_sort_filter_expr.filter_expr()));
///
/// ```
///
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
    use arrow::compute::{CastOptions, SortOptions};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, CastExpr, Literal};
    use std::sync::Arc;

    /// Filter expr for a + b > c + 10 AND a + b < c + 100
    pub(crate) fn complicated_filter() -> Arc<dyn PhysicalExpr> {
        let left_expr = BinaryExpr::new(
            Arc::new(CastExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("0", 0)),
                    Operator::Plus,
                    Arc::new(Column::new("1", 1)),
                )),
                DataType::Int64,
                CastOptions { safe: false },
            )),
            Operator::Gt,
            Arc::new(BinaryExpr::new(
                Arc::new(CastExpr::new(
                    Arc::new(Column::new("2", 2)),
                    DataType::Int64,
                    CastOptions { safe: false },
                )),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(10)))),
            )),
        );

        let right_expr = BinaryExpr::new(
            Arc::new(CastExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("0", 0)),
                    Operator::Plus,
                    Arc::new(Column::new("1", 1)),
                )),
                DataType::Int64,
                CastOptions { safe: false },
            )),
            Operator::Lt,
            Arc::new(BinaryExpr::new(
                Arc::new(CastExpr::new(
                    Arc::new(Column::new("2", 2)),
                    DataType::Int64,
                    CastOptions { safe: false },
                )),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int64(Some(100)))),
            )),
        );

        Arc::new(BinaryExpr::new(
            Arc::new(left_expr),
            Operator::And,
            Arc::new(right_expr),
        ))
    }

    #[test]
    fn test_column_collector() {
        let filter_expr = complicated_filter();
        let columns = collect_columns(&filter_expr);
        assert_eq!(columns.len(), 3)
    }

    #[test]
    fn find_expr_inside_expr() -> Result<()> {
        let filter_expr = complicated_filter();

        let expr_1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("gnz", 0));
        assert!(!check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_1
        ));

        let expr_2: Arc<dyn PhysicalExpr> = Arc::new(Column::new("1", 1));

        assert!(check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_2
        ));

        let expr_3: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("0", 0)),
                Operator::Plus,
                Arc::new(Column::new("1", 1)),
            )),
            DataType::Int64,
            CastOptions { safe: false },
        ));

        assert!(check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_3
        ));

        let expr_4: Arc<dyn PhysicalExpr> = Arc::new(Column::new("1", 42));

        assert!(!check_filter_expr_contains_sort_information(
            &filter_expr,
            &expr_4,
        ));
        Ok(())
    }

    #[test]
    fn build_sorted_expr() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("la1", DataType::Int32, false),
            Field::new("lb1", DataType::Int32, false),
            Field::new("lc1", DataType::Int32, false),
            Field::new("lt1", DataType::Int32, false),
            Field::new("la2", DataType::Int32, false),
            Field::new("la1_des", DataType::Int32, false),
        ]));

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("ra1", DataType::Int32, false),
            Field::new("rb1", DataType::Int32, false),
            Field::new("rc1", DataType::Int32, false),
            Field::new("rt1", DataType::Int32, false),
            Field::new("ra2", DataType::Int32, false),
            Field::new("ra1_des", DataType::Int32, false),
        ]));

        let filter_col_0 = Arc::new(Column::new("0", 0));
        let filter_col_1 = Arc::new(Column::new("1", 1));
        let filter_col_2 = Arc::new(Column::new("2", 2));

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
        let intermediate_schema = Schema::new(vec![
            Field::new(filter_col_0.name(), DataType::Int32, true),
            Field::new(filter_col_1.name(), DataType::Int32, true),
            Field::new(filter_col_2.name(), DataType::Int32, true),
        ]);

        let filter_expr = complicated_filter();

        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        assert!(build_filter_input_order(
            JoinSide::Left,
            &filter,
            &left_schema,
            &PhysicalSortExpr {
                expr: Arc::new(Column::new("la1", 0)),
                options: SortOptions::default(),
            }
        )
        .is_ok());
        assert!(build_filter_input_order(
            JoinSide::Left,
            &filter,
            &left_schema,
            &PhysicalSortExpr {
                expr: Arc::new(Column::new("lt1", 3)),
                options: SortOptions::default(),
            }
        )
        .is_err());
        assert!(build_filter_input_order(
            JoinSide::Right,
            &filter,
            &right_schema,
            &PhysicalSortExpr {
                expr: Arc::new(Column::new("ra1", 0)),
                options: SortOptions::default(),
            }
        )
        .is_ok());
        assert!(build_filter_input_order(
            JoinSide::Right,
            &filter,
            &right_schema,
            &PhysicalSortExpr {
                expr: Arc::new(Column::new("rb1", 1)),
                options: SortOptions::default(),
            }
        )
        .is_err());

        Ok(())
    }
    // if one side is sorted by ORDER BY (a+b), and join filter condition includes (a-b).
    #[test]
    fn sorted_filter_expr_build() -> Result<()> {
        let filter_col_0 = Arc::new(Column::new("0", 0));
        let filter_col_1 = Arc::new(Column::new("1", 1));

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
        let intermediate_schema = Schema::new(vec![
            Field::new(filter_col_0.name(), DataType::Int32, true),
            Field::new(filter_col_1.name(), DataType::Int32, true),
        ]);

        let filter_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("0", 0)),
            Operator::Minus,
            Arc::new(Column::new("1", 1)),
        ));

        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let sorted = PhysicalSortExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                Arc::new(Column::new("b", 1)),
            )),
            options: SortOptions::default(),
        };

        let res = convert_sort_expr_with_filter_schema(
            &JoinSide::Left,
            &filter,
            &schema,
            &sorted,
        )?;
        assert!(res.is_none());
        Ok(())
    }
}
