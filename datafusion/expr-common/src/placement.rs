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

//! Expression placement classification for scalar functions.
//!
//! This module determines where in the query plan expressions should be placed
//! to optimize data flow:
//!
//! - **Leaf placement**: Cheap expressions (field accessors, column references)
//!   are pushed down to leaf nodes (near data sources) to reduce data volume early.
//! - **Root placement**: Expensive expressions (computations, aggregates) are kept
//!   at root nodes (after filtering) to operate on less data.

/// Classification of expression placement for scalar functions.
///
/// This enum is used by [`ScalarUDFImpl::placement`] to allow
/// functions to make context-dependent decisions about where they should
/// be placed in the query plan based on the nature of their arguments.
///
/// For example, `get_field(struct_col, 'field_name')` is
/// leaf-pushable (static field lookup), but `string_col like '%foo%'`
/// performs expensive per-row computation and should be placed
/// as further up the tree so that it can be run after filtering, sorting, etc.
///
/// # Why not pass in expressions directly to decide placement?
///
/// There are two reasons for using this enum instead of passing in the full expressions:
///
/// 1. **ScalarUDFImpl cannot reference PhysicalExpr**: The trait is defined in datafusion-expr,
///    which cannot reference datafusion-physical-expr since the latter depends on the former
///    (it would create a circular dependency).
/// 2. **Simplicity**: Without this enum abstracting away logical / physical distinctions,
///    we would need two distinct methods on ScalarUDFImpl: one for logical expression placement
///    and one for physical expression placement. This would require implementors to duplicate logic
///    and increases complexity for UDF authors.
///
/// [`ScalarUDFImpl::placement`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.ScalarUDFImpl.html#tymethod.placement
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionPlacement {
    /// Argument is a literal constant value or an expression that can be
    /// evaluated to a constant at planning time.
    Literal,
    /// Argument is a simple column reference.
    Column,
    /// Argument is a complex expression that can be safely placed at leaf nodes.
    /// For example, if `get_field(struct_col, 'field_name')` is implemented as a
    /// leaf-pushable expression, then it would return this variant.
    /// Then `other_leaf_function(get_field(...), 42)` could also be classified as
    /// leaf-pushable using the knowledge that `get_field(...)` is leaf-pushable.
    PlaceAtLeaves,
    /// Argument is a complex expression that should be placed at root nodes.
    /// For example, `min(col1 + col2)` is not leaf-pushable because it requires per-row computation.
    PlaceAtRoot,
}
