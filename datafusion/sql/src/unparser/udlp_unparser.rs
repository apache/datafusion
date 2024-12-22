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

use crate::unparser::ast::{
    QueryBuilder, RelationBuilder, SelectBuilder,
};
use crate::unparser::Unparser;
use datafusion_expr::UserDefinedLogicalNode;
use sqlparser::ast::Statement;

/// This trait allows users to define custom unparser logic for their custom logical nodes.
pub trait UserDefinedLogicalNodeUnparser {
    /// Unparse the custom logical node to SQL within a statement.
    ///
    /// This method is called when the custom logical node is part of a statement.
    /// e.g. `SELECT * FROM custom_logical_node`
    fn unparse(
        &self,
        _node: &dyn UserDefinedLogicalNode,
        _unparser: &Unparser,
        _query: &mut Option<&mut QueryBuilder>,
        _select: &mut Option<&mut SelectBuilder>,
        _relation: &mut Option<&mut RelationBuilder>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }

    /// Unparse the custom logical node to a statement.
    ///
    /// This method is called when the custom logical node is a custom statement.
    fn unparse_to_statement(
        &self,
        _node: &dyn UserDefinedLogicalNode,
        _unparser: &Unparser,
    ) -> datafusion_common::Result<Option<Statement>> {
        Ok(None)
    }
}
