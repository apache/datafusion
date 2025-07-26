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

use crate::match_recognize::Pattern;
use crate::{Expr, SortExpr};
use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference};
use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

/// A MATCH_RECOGNIZE operation for pattern matching on ordered data
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchRecognizePattern {
    /// The input logical plan
    pub input: Arc<crate::LogicalPlan>,
    /// The output schema
    pub schema: DFSchemaRef,

    /// The various clauses of the MATCH_RECOGNIZE expression
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortExpr>,
    pub after_skip: Option<crate::match_recognize::AfterMatchSkip>,
    pub rows_per_match: Option<crate::match_recognize::RowsPerMatch>,
    pub pattern: Pattern,
    pub symbols: Vec<String>,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for MatchRecognizePattern {
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
        match self.pattern.partial_cmp(&other.pattern) {
            Some(Ordering::Equal) => {}
            cmp => return cmp,
        }
        Some(Ordering::Equal)
    }
}

impl fmt::Display for MatchRecognizePattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MatchRecognizePattern:")?;

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

        if let Some(after_skip) = &self.after_skip {
            write!(f, " after_skip={}", after_skip)?;
        }

        write!(f, " pattern=[{}]", self.pattern)?;

        Ok(())
    }
}

impl MatchRecognizePattern {
    /// Create a new MatchRecognize operator
    pub fn try_new(
        input: Arc<crate::LogicalPlan>,
        partition_by: Vec<Expr>,
        order_by: Vec<SortExpr>,
        after_skip: Option<crate::match_recognize::AfterMatchSkip>,
        rows_per_match: Option<crate::match_recognize::RowsPerMatch>,
        pattern: Pattern,
        symbols: Vec<String>,
    ) -> Result<Self> {
        let schema = pattern_schema(input.schema());

        Ok(Self {
            input,
            schema,
            partition_by,
            order_by,
            after_skip,
            rows_per_match,
            pattern,
            symbols,
        })
    }
}

/// Create a DFSchema with match metadata columns for MATCH operations
fn pattern_schema(table_schema: &DFSchema) -> DFSchemaRef {
    let mut fields: Vec<(Option<TableReference>, Arc<arrow::datatypes::Field>)> =
        table_schema
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .collect();

    // Add match metadata columns
    fields.push((
        None,
        Arc::new(arrow::datatypes::Field::new(
            "__mr_classifier",
            arrow::datatypes::DataType::Utf8,
            false,
        )),
    ));
    fields.push((
        None,
        Arc::new(arrow::datatypes::Field::new(
            "__mr_match_number",
            arrow::datatypes::DataType::UInt64,
            false,
        )),
    ));
    fields.push((
        None,
        Arc::new(arrow::datatypes::Field::new(
            "__mr_match_sequence_number",
            arrow::datatypes::DataType::UInt64,
            false,
        )),
    ));

    fields.push((
        None,
        Arc::new(arrow::datatypes::Field::new(
            "__mr_is_last_match_row",
            arrow::datatypes::DataType::Boolean,
            false,
        )),
    ));

    fields.push((
        None,
        Arc::new(arrow::datatypes::Field::new(
            "__mr_is_included_row",
            arrow::datatypes::DataType::Boolean,
            false,
        )),
    ));

    Arc::new(
        DFSchema::new_with_metadata(fields, table_schema.metadata().clone())
            .expect("Failed to create match schema"),
    )
}
