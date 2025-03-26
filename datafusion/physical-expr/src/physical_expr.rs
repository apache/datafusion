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

use crate::create_physical_expr;
use datafusion_common::{DFSchema, HashMap};
use datafusion_expr::execution_props::ExecutionProps;
pub(crate) use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
pub use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;
use itertools::izip;

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

use crate::{expressions, LexOrdering, PhysicalSortExpr};
use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion_common::plan_err;
use datafusion_common::Result;
use datafusion_expr::{Expr, SortExpr};

/// Converts logical sort expressions to physical sort expressions
///
/// This function transforms a collection of logical sort expressions into their physical
/// representation that can be used during query execution.
///
/// # Arguments
///
/// * `schema` - The schema containing column definitions
/// * `sort_order` - A collection of logical sort expressions grouped into lexicographic orderings
///
/// # Returns
///
/// A vector of lexicographic orderings for physical execution, or an error if the transformation fails
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
///     vec![
///         SortExpr { expr: Expr::Column(Column::new(Some("t"), "id")), asc: true, nulls_first: false }
///     ],
///     vec![
///         SortExpr { expr: Expr::Column(Column::new(Some("t"), "name")), asc: false, nulls_first: true }
///     ]
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
        let mut sort_exprs = LexOrdering::default();
        for (expr_idx, sort) in exprs.iter().enumerate() {
            match &sort.expr {
                Expr::Column(col) => match expressions::col(&col.name, schema) {
                    Ok(expr) => {
                        sort_exprs.push(PhysicalSortExpr {
                            expr,
                            options: SortOptions {
                                descending: !sort.asc,
                                nulls_first: sort.nulls_first,
                            },
                        });
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
        if !sort_exprs.is_empty() {
            all_sort_orders.push(sort_exprs);
        }
    }
    Ok(all_sort_orders)
}

/// Create a physical sort expression from a logical expression
pub fn create_physical_sort_expr(
    e: &SortExpr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<PhysicalSortExpr> {
    let SortExpr {
        expr,
        asc,
        nulls_first,
    } = e;
    Ok(PhysicalSortExpr {
        expr: create_physical_expr(expr, input_dfschema, execution_props)?,
        options: SortOptions {
            descending: !asc,
            nulls_first: *nulls_first,
        },
    })
}

/// Create vector of physical sort expression from a vector of logical expression
pub fn create_physical_sort_exprs(
    exprs: &[SortExpr],
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<LexOrdering> {
    exprs
        .iter()
        .map(|expr| create_physical_sort_expr(expr, input_dfschema, execution_props))
        .collect::<Result<LexOrdering>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::{Column, Literal};
    use crate::physical_expr::{
        physical_exprs_bag_equal, physical_exprs_contains, physical_exprs_equal,
    };

    use datafusion_common::ScalarValue;

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
}
