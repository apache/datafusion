// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
