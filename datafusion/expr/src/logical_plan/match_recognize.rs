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

use crate::match_recognize::{AfterMatchSkip, Pattern, RowsPerMatch};
use crate::match_recognize::{MatchRecognizeOutputSpec, OutputSpecWithDFSchema};
use crate::{Expr, SortExpr};
use datafusion_common::tree_node::{Transformed, TreeNodeContainer, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, Result};
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

/// A MATCH_RECOGNIZE operation for pattern matching on ordered data
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchRecognize {
    /// The input logical plan
    pub input: Arc<crate::LogicalPlan>,
    /// The output schema
    pub schema: DFSchemaRef,

    /// The various clauses of the MATCH_RECOGNIZE expression
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortExpr>,
    pub after_skip: AfterMatchSkip,
    /// SQL-facing `ROWS PER MATCH` requested mode.
    pub rows_per_match: RowsPerMatch,
    pub pattern: Pattern,
    pub symbols: Vec<String>,

    /// DEFINE expressions
    /// One expression per symbol referenced in `pattern`.
    /// Physical planner will compute `__mr_symbol_*` columns from these expressions.
    pub defines: Vec<NamedExpr>,

    /// Output projection/pruning specification (passthrough, metadata, bitsets)
    pub output_spec: MatchRecognizeOutputSpec,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for MatchRecognize {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            cmp => return cmp,
        }
        match self.partition_by.partial_cmp(&other.partition_by) {
            Some(Ordering::Equal) => {}
            cmp => return cmp,
        }
        match self.order_by.partial_cmp(&other.order_by) {
            Some(Ordering::Equal) => {}
            cmp => return cmp,
        }
        match self.after_skip.partial_cmp(&other.after_skip) {
            Some(Ordering::Equal) => {}
            cmp => return cmp,
        }
        match self.rows_per_match.partial_cmp(&other.rows_per_match) {
            Some(Ordering::Equal) => {}
            cmp => return cmp,
        }
        match self.pattern.partial_cmp(&other.pattern) {
            Some(Ordering::Equal) => {}
            cmp => return cmp,
        }
        Some(Ordering::Equal)
    }
}

impl fmt::Display for MatchRecognize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MatchRecognize:")?;

        if !self.partition_by.is_empty() {
            write!(
                f,
                " partition_by=[{}]",
                self.partition_by
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }

        if !self.order_by.is_empty() {
            write!(
                f,
                " order_by=[{}]",
                self.order_by
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }

        write!(f, " after_skip=[{}]", &self.after_skip)?;
        write!(f, " pattern=[{}]", self.pattern)?;

        if !self.symbols.is_empty() {
            write!(f, " symbols=[{}]", self.symbols.join(","))?;
        }

        write!(f, " rows_per_match=[{}]", &self.rows_per_match)?;

        if !self.defines.is_empty() {
            let defs: Vec<String> = self
                .defines
                .iter()
                .map(|ne| format!("{}: {}", ne.name, ne.expr))
                .collect();
            write!(f, " defines=[{}]", defs.join(", "))?;
        }

        if !self.output_spec.is_empty() {
            write!(
                f,
                " {}",
                OutputSpecWithDFSchema::new(&self.output_spec, self.input.schema(),)
            )?;
        }
        Ok(())
    }
}

impl MatchRecognize {
    /// Create a new MatchRecognize operator
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<crate::LogicalPlan>,
        partition_by: Vec<Expr>,
        order_by: Vec<SortExpr>,
        after_skip: AfterMatchSkip,
        rows_per_match: RowsPerMatch,
        pattern: Pattern,
        symbols: Vec<String>,
        defines: Vec<NamedExpr>,
        output_spec: MatchRecognizeOutputSpec,
    ) -> Result<Self> {
        let schema = output_spec.build_df_schema(input.schema())?;

        Ok(Self {
            input,
            schema,
            partition_by,
            order_by,
            after_skip,
            rows_per_match,
            pattern,
            symbols,
            defines,
            output_spec,
        })
    }
}

/// NamedExpr pairs an expression with a stable output name used by MATCH_RECOGNIZE.
///
/// It represents DEFINE predicates and MEASURES labels without relying on
/// `Expr::Alias`, avoiding unintended alias stripping by optimizers while
/// keeping traversal and rewriting simple.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NamedExpr {
    pub expr: Expr,
    pub name: String,
}

impl From<(Expr, String)> for NamedExpr {
    fn from(value: (Expr, String)) -> Self {
        Self {
            expr: value.0,
            name: value.1,
        }
    }
}

impl From<NamedExpr> for (Expr, String) {
    fn from(value: NamedExpr) -> Self {
        (value.expr, value.name)
    }
}

// Adapter to treat NamedExpr as a container over Expr for tree traversal.
// Used by MATCH_RECOGNIZE (defines/measures) where each Expr is paired with a name.
// Co-located here to keep traversal logic with its consumer rather than
// in generic tree traversal utilities.
impl<'a> TreeNodeContainer<'a, Expr> for NamedExpr {
    fn apply_elements<F: FnMut(&'a Expr) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        f(&self.expr)
    }

    fn map_elements<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        let t = f(self.expr)?;
        Ok(Transformed::new(
            NamedExpr {
                expr: t.data,
                name: self.name,
            },
            t.transformed,
            t.tnr,
        ))
    }
}
