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

use crate::unparser::Unparser;
use crate::unparser::ast::{QueryBuilder, RelationBuilder, SelectBuilder};
use datafusion_expr::UserDefinedLogicalNode;
use sqlparser::ast::Statement;

/// This trait allows users to define custom unparser logic for their custom logical nodes.
pub trait UserDefinedLogicalNodeUnparser {
    /// Unparse the custom logical node to SQL within a statement.
    ///
    /// This method is called when the custom logical node is part of a statement.
    /// e.g. `SELECT * FROM custom_logical_node`
    ///
    /// The return value should be [UnparseWithinStatementResult::Modified] if the custom logical node was successfully unparsed.
    /// Otherwise, return [UnparseWithinStatementResult::Unmodified].
    fn unparse(
        &self,
        _node: &dyn UserDefinedLogicalNode,
        _unparser: &Unparser,
        _query: &mut Option<&mut QueryBuilder>,
        _select: &mut Option<&mut SelectBuilder>,
        _relation: &mut Option<&mut RelationBuilder>,
    ) -> datafusion_common::Result<UnparseWithinStatementResult> {
        Ok(UnparseWithinStatementResult::Unmodified)
    }

    /// Unparse the custom logical node to a statement.
    ///
    /// This method is called when the custom logical node is a custom statement.
    ///
    /// The return value should be [UnparseToStatementResult::Modified] if the custom logical node was successfully unparsed.
    /// Otherwise, return [UnparseToStatementResult::Unmodified].
    fn unparse_to_statement(
        &self,
        _node: &dyn UserDefinedLogicalNode,
        _unparser: &Unparser,
    ) -> datafusion_common::Result<UnparseToStatementResult> {
        Ok(UnparseToStatementResult::Unmodified)
    }
}

/// The result of unparsing a custom logical node within a statement.
pub enum UnparseWithinStatementResult {
    /// If the custom logical node was successfully unparsed within a statement.
    Modified,
    /// If the custom logical node wasn't unparsed.
    Unmodified,
}

/// The result of unparsing a custom logical node to a statement.
#[expect(clippy::large_enum_variant)]
pub enum UnparseToStatementResult {
    /// If the custom logical node was successfully unparsed to a statement.
    Modified(Statement),
    /// If the custom logical node wasn't unparsed.
    Unmodified,
}
