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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

//! Interfaces and default implementations of catalogs and schemas.
//!
//! Implementations
//! * Information schema: [`information_schema`]
//! * Simple memory based catalog: [`MemoryCatalogProviderList`], [`MemoryCatalogProvider`], [`MemorySchemaProvider`]
//! * Listing schema: [`listing_schema`]

pub mod memory;
pub use memory::{
    MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider,
};
mod r#async;
mod catalog;
mod dynamic_file;
pub mod information_schema;
pub mod listing_schema;
mod schema;
mod session;
mod table;
pub use catalog::*;
pub use dynamic_file::catalog::*;
pub use r#async::*;
pub use schema::*;
pub use session::*;
pub use table::*;
pub mod stream;
pub mod streaming;
pub mod view;

use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion_common::plan_err;
use datafusion_common::Result;
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
/// # use arrow::datatypes::{Schema, Field, DataType};
/// # use datafusion_catalog::create_ordering;
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
