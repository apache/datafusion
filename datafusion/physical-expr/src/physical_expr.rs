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

use crate::expressions::{self, Column};
use crate::{LexOrdering, PhysicalSortExpr, create_physical_expr};

use arrow::compute::SortOptions;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DFSchema, HashMap};
use datafusion_common::{Result, plan_err};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{Expr, SortExpr};

use itertools::izip;
// Exports:
pub(crate) use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Adds the `offset` value to `Column` indices inside `expr`. This function is
/// generally used during the update of the right table schema in join operations.
pub fn add_offset_to_expr(
    expr: Arc<dyn PhysicalExpr>,
    offset: isize,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform_down(|e| match e.as_any().downcast_ref::<Column>() {
        Some(col) => {
            let Some(idx) = col.index().checked_add_signed(offset) else {
                return plan_err!("Column index overflow");
            };
            Ok(Transformed::yes(Arc::new(Column::new(col.name(), idx))))
        }
        None => Ok(Transformed::no(e)),
    })
    .data()
}

/// This function is similar to the `contains` method of `Vec`. It finds
/// whether `expr` is among `physical_exprs`.
pub fn physical_exprs_contains(
    physical_exprs: &[Arc<dyn PhysicalExpr>],
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    physical_exprs
        .iter()
        .any(|physical_expr| physical_expr.eq(expr))
}

/// Checks whether the given physical expression slices are equal.
pub fn physical_exprs_equal(
    lhs: &[Arc<dyn PhysicalExpr>],
    rhs: &[Arc<dyn PhysicalExpr>],
) -> bool {
    lhs.len() == rhs.len() && izip!(lhs, rhs).all(|(lhs, rhs)| lhs.eq(rhs))
}

/// Checks whether the given physical expression slices are equal in the sense
/// of bags (multi-sets), disregarding their orderings.
pub fn physical_exprs_bag_equal(
    lhs: &[Arc<dyn PhysicalExpr>],
    rhs: &[Arc<dyn PhysicalExpr>],
) -> bool {
    let mut multi_set_lhs: HashMap<_, usize> = HashMap::new();
    let mut multi_set_rhs: HashMap<_, usize> = HashMap::new();
    for expr in lhs {
        *multi_set_lhs.entry(expr).or_insert(0) += 1;
    }
    for expr in rhs {
        *multi_set_rhs.entry(expr).or_insert(0) += 1;
    }
    multi_set_lhs == multi_set_rhs
}

/// Converts logical sort expressions to physical sort expressions.
///
/// This function transforms a collection of logical sort expressions into their
/// physical representation that can be used during query execution.
///
/// # Arguments
///
/// * `schema` - The schema containing column definitions.
/// * `sort_order` - A collection of logical sort expressions grouped into
///   lexicographic orderings.
///
/// # Returns
///
/// A vector of lexicographic orderings for physical execution, or an error if
/// the transformation fails.
///
/// # Examples
///
/// ```
/// // Create orderings from columns "id" and "name"
/// # use arrow::datatypes::{Schema, Field, DataType};
/// # use datafusion_physical_expr::create_ordering;
/// # use datafusion_common::Column;
/// # use datafusion_expr::{Expr, SortExpr};
/// #
/// // Create a schema with two fields
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
///     Field::new("name", DataType::Utf8, false),
/// ]);
///
/// let sort_exprs = vec![
///     vec![SortExpr {
///         expr: Expr::Column(Column::new(Some("t"), "id")),
///         asc: true,
///         nulls_first: false,
///     }],
///     vec![SortExpr {
///         expr: Expr::Column(Column::new(Some("t"), "name")),
///         asc: false,
///         nulls_first: true,
///     }],
/// ];
/// let result = create_ordering(&schema, &sort_exprs).unwrap();
/// ```
pub fn create_ordering(
    schema: &Schema,
    sort_order: &[Vec<SortExpr>],
) -> Result<Vec<LexOrdering>> {
    let mut all_sort_orders = vec![];

    for (group_idx, exprs) in sort_order.iter().enumerate() {
        // Construct PhysicalSortExpr objects from Expr objects:
        let mut sort_exprs = vec![];
        for (expr_idx, sort) in exprs.iter().enumerate() {
            match &sort.expr {
                Expr::Column(col) => match expressions::col(&col.name, schema) {
                    Ok(expr) => {
                        let opts = SortOptions::new(!sort.asc, sort.nulls_first);
                        sort_exprs.push(PhysicalSortExpr::new(expr, opts));
                    }
                    // Cannot find expression in the projected_schema, stop iterating
                    // since rest of the orderings are violated
                    Err(_) => break,
                },
                expr => {
                    return plan_err!(
                        "Expected single column reference in sort_order[{}][{}], got {}",
                        group_idx,
                        expr_idx,
                        expr
                    );
                }
            }
        }
        all_sort_orders.extend(LexOrdering::new(sort_exprs));
    }
    Ok(all_sort_orders)
}

/// Creates a vector of [LexOrdering] from a vector of logical expression
pub fn create_lex_ordering(
    schema: &SchemaRef,
    sort_order: &[Vec<SortExpr>],
    execution_props: &ExecutionProps,
) -> Result<Vec<LexOrdering>> {
    // Try the fast path that only supports column references first
    // This avoids creating a DFSchema
    if let Ok(ordering) = create_ordering(schema, sort_order) {
        return Ok(ordering);
    }

    let df_schema = DFSchema::try_from(Arc::clone(schema))?;

    let mut all_sort_orders = vec![];

    for exprs in sort_order.iter() {
        all_sort_orders.extend(LexOrdering::new(create_physical_sort_exprs(
            exprs,
            &df_schema,
            execution_props,
        )?));
    }
    Ok(all_sort_orders)
}

/// Create a physical sort expression from a logical expression
pub fn create_physical_sort_expr(
    e: &SortExpr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<PhysicalSortExpr> {
    create_physical_expr(&e.expr, input_dfschema, execution_props).map(|expr| {
        let options = SortOptions::new(!e.asc, e.nulls_first);
        PhysicalSortExpr::new(expr, options)
    })
}

/// Create vector of physical sort expression from a vector of logical expression
pub fn create_physical_sort_exprs(
    exprs: &[SortExpr],
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Vec<PhysicalSortExpr>> {
    exprs
        .iter()
        .map(|e| create_physical_sort_expr(e, input_dfschema, execution_props))
        .collect()
}

pub fn add_offset_to_physical_sort_exprs(
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    offset: isize,
) -> Result<Vec<PhysicalSortExpr>> {
    sort_exprs
        .into_iter()
        .map(|mut sort_expr| {
            sort_expr.expr = add_offset_to_expr(sort_expr.expr, offset)?;
            Ok(sort_expr)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::{BinaryExpr, Column, Literal};
    use crate::physical_expr::{
        physical_exprs_bag_equal, physical_exprs_contains, physical_exprs_equal,
    };
    use datafusion_physical_expr_common::physical_expr::is_volatile;

    use arrow::datatypes::{DataType, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::ColumnarValue;
    use datafusion_expr::Operator;
    use std::any::Any;
    use std::fmt;

    #[test]
    fn test_physical_exprs_contains() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit4 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let col_a_expr = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let col_c_expr = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;

        // lit(true), lit(false), lit(4), lit(2), Col(a), Col(b)
        let physical_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::clone(&lit_true),
            Arc::clone(&lit_false),
            Arc::clone(&lit4),
            Arc::clone(&lit2),
            Arc::clone(&col_a_expr),
            Arc::clone(&col_b_expr),
        ];
        // below expressions are inside physical_exprs
        assert!(physical_exprs_contains(&physical_exprs, &lit_true));
        assert!(physical_exprs_contains(&physical_exprs, &lit2));
        assert!(physical_exprs_contains(&physical_exprs, &col_b_expr));

        // below expressions are not inside physical_exprs
        assert!(!physical_exprs_contains(&physical_exprs, &col_c_expr));
        assert!(!physical_exprs_contains(&physical_exprs, &lit1));
    }

    #[test]
    fn test_physical_exprs_equal() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let vec1 = vec![Arc::clone(&lit_true), Arc::clone(&lit_false)];
        let vec2 = vec![Arc::clone(&lit_true), Arc::clone(&col_b_expr)];
        let vec3 = vec![Arc::clone(&lit2), Arc::clone(&lit1)];
        let vec4 = vec![Arc::clone(&lit_true), Arc::clone(&lit_false)];

        // these vectors are same
        assert!(physical_exprs_equal(&vec1, &vec1));
        assert!(physical_exprs_equal(&vec1, &vec4));
        assert!(physical_exprs_bag_equal(&vec1, &vec1));
        assert!(physical_exprs_bag_equal(&vec1, &vec4));

        // these vectors are different
        assert!(!physical_exprs_equal(&vec1, &vec2));
        assert!(!physical_exprs_equal(&vec1, &vec3));
        assert!(!physical_exprs_bag_equal(&vec1, &vec2));
        assert!(!physical_exprs_bag_equal(&vec1, &vec3));
    }

    #[test]
    fn test_physical_exprs_set_equal() {
        let list1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(!physical_exprs_bag_equal(
            list1.as_slice(),
            list2.as_slice()
        ));
        assert!(!physical_exprs_bag_equal(
            list2.as_slice(),
            list1.as_slice()
        ));
        assert!(!physical_exprs_equal(list1.as_slice(), list2.as_slice()));
        assert!(!physical_exprs_equal(list2.as_slice(), list1.as_slice()));

        let list3: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list4: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(physical_exprs_bag_equal(list3.as_slice(), list4.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list3.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list4.as_slice()));
        assert!(!physical_exprs_equal(list3.as_slice(), list4.as_slice()));
        assert!(!physical_exprs_equal(list4.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list3.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list4.as_slice()));
    }

    #[test]
    fn test_is_volatile_default_behavior() {
        // Test that default PhysicalExpr implementations are not volatile
        let literal =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let column = Arc::new(Column::new("test", 0)) as Arc<dyn PhysicalExpr>;

        // Test is_volatile_node() - should return false by default
        assert!(!literal.is_volatile_node());
        assert!(!column.is_volatile_node());

        // Test is_volatile() - should return false for non-volatile expressions
        assert!(!is_volatile(&literal));
        assert!(!is_volatile(&column));
    }

    /// Mock volatile PhysicalExpr for testing purposes
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct MockVolatileExpr {
        volatile: bool,
    }

    impl MockVolatileExpr {
        fn new(volatile: bool) -> Self {
            Self { volatile }
        }
    }

    impl fmt::Display for MockVolatileExpr {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MockVolatile({})", self.volatile)
        }
    }

    impl PhysicalExpr for MockVolatileExpr {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            Ok(DataType::Boolean)
        }

        fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
            Ok(false)
        }

        fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                self.volatile,
            ))))
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }

        fn is_volatile_node(&self) -> bool {
            self.volatile
        }

        fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "mock_volatile({})", self.volatile)
        }
    }

    #[test]
    fn test_nested_expression_volatility() {
        // Test that is_volatile() recursively detects volatility in expression trees

        // Create a volatile mock expression
        let volatile_expr =
            Arc::new(MockVolatileExpr::new(true)) as Arc<dyn PhysicalExpr>;
        assert!(volatile_expr.is_volatile_node());
        assert!(is_volatile(&volatile_expr));

        // Create a non-volatile mock expression
        let stable_expr = Arc::new(MockVolatileExpr::new(false)) as Arc<dyn PhysicalExpr>;
        assert!(!stable_expr.is_volatile_node());
        assert!(!is_volatile(&stable_expr));

        // Create a literal (non-volatile)
        let literal =
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        assert!(!literal.is_volatile_node());
        assert!(!is_volatile(&literal));

        // Test composite expression: volatile_expr AND literal
        // The BinaryExpr itself is not volatile, but contains a volatile child
        let composite_expr = Arc::new(BinaryExpr::new(
            Arc::clone(&volatile_expr),
            Operator::And,
            Arc::clone(&literal),
        )) as Arc<dyn PhysicalExpr>;

        assert!(!composite_expr.is_volatile_node()); // BinaryExpr itself is not volatile
        assert!(is_volatile(&composite_expr)); // But it contains a volatile child

        // Test composite expression with all non-volatile children
        let stable_composite = Arc::new(BinaryExpr::new(
            Arc::clone(&stable_expr),
            Operator::And,
            Arc::clone(&literal),
        )) as Arc<dyn PhysicalExpr>;

        assert!(!stable_composite.is_volatile_node());
        assert!(!is_volatile(&stable_composite)); // No volatile children
    }
}
