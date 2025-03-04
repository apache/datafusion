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

//! DataFusion data sources: [`TableProvider`] and [`ListingTable`]
//!
//! [`ListingTable`]: crate::datasource::listing::ListingTable

pub mod avro_to_arrow;
pub mod cte_worktable;
pub mod default_table_source;
pub mod dynamic_file;
pub mod empty;
pub mod file_format;
pub mod listing;
pub mod listing_table_factory;
pub mod memory;
pub mod physical_plan;
pub mod provider;
pub mod schema_adapter;
mod statistics;
pub mod stream;
pub mod view;

pub use datafusion_datasource::source;

// backwards compatibility
pub use self::default_table_source::{
    provider_as_source, source_as_provider, DefaultTableSource,
};
pub use self::memory::MemTable;
pub use self::view::ViewTable;
pub use crate::catalog::TableProvider;
pub use crate::logical_expr::TableType;
pub use datafusion_execution::object_store;
pub use statistics::get_statistics_with_limit;

use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{Expr, SortExpr};
use datafusion_physical_expr::{expressions, LexOrdering, PhysicalSortExpr};

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
/// use arrow::datatypes::{Schema, Field, DataType};
/// use datafusion::datasource::create_ordering;
/// use datafusion_common::Column;
/// use datafusion_expr::{Expr, SortExpr};
///
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
