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

//! Shared constants and helpers for MATCH_RECOGNIZE virtual columns.
//!
//! These are used across SQL planning, logical planning, and physical
//! execution.

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion_common::TableReference;

// Prefix for DEFINE-derived boolean symbol columns
const MR_SYMBOL_PREFIX: &str = "__mr_symbol_";
// Prefix for classifier bitset columns
const MR_CLASSIFIER_BITS_PREFIX: &str = "__mr_classifier_";

// Core metadata column names
const MR_CLASSIFIER: &str = "__mr_classifier";
const MR_MATCH_NUMBER: &str = "__mr_match_number";
const MR_MATCH_SEQUENCE_NUMBER: &str = "__mr_match_sequence_number";
const MR_IS_EXCLUDED_ROW: &str = "__mr_is_excluded_row";

/// Enum representing MATCH_RECOGNIZE metadata columns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MrMetadataColumn {
    Classifier,
    MatchNumber,
    MatchSequenceNumber,
    IsExcludedRow,
}

impl MrMetadataColumn {
    /// Get the Arrow data type for the column
    pub const fn data_type(&self) -> arrow::datatypes::DataType {
        match self {
            Self::Classifier => arrow::datatypes::DataType::Utf8,
            Self::MatchNumber | Self::MatchSequenceNumber => {
                arrow::datatypes::DataType::UInt64
            }
            Self::IsExcludedRow => arrow::datatypes::DataType::Boolean,
        }
    }

    /// Check if the column is nullable
    pub const fn is_nullable(&self) -> bool {
        match self {
            // Unmatched rows must produce NULLs for these
            Self::Classifier | Self::MatchNumber | Self::MatchSequenceNumber => true,
            // Helper flag is always set for matched rows (true only for excluded rows),
            // and false for other rows; keep this non-nullable
            Self::IsExcludedRow => false,
        }
    }

    /// Return the canonical function name used in MEASURES for this
    /// metadata column (e.g. Classifier => "classifier()").
    pub const fn measure_function_name(&self) -> &'static str {
        match self {
            Self::Classifier => "classifier",
            Self::MatchNumber => "match_number",
            Self::MatchSequenceNumber => "match_sequence_number",
            Self::IsExcludedRow => "is_excluded_row",
        }
    }

    /// Get all metadata columns in canonical order
    pub const fn all() -> [Self; 4] {
        [
            Self::Classifier,
            Self::MatchNumber,
            Self::MatchSequenceNumber,
            Self::IsExcludedRow,
        ]
    }
}

impl AsRef<str> for MrMetadataColumn {
    fn as_ref(&self) -> &str {
        match self {
            Self::Classifier => MR_CLASSIFIER,
            Self::MatchNumber => MR_MATCH_NUMBER,
            Self::MatchSequenceNumber => MR_MATCH_SEQUENCE_NUMBER,
            Self::IsExcludedRow => MR_IS_EXCLUDED_ROW,
        }
    }
}

impl FromStr for MrMetadataColumn {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            MR_CLASSIFIER => Ok(Self::Classifier),
            MR_MATCH_NUMBER => Ok(Self::MatchNumber),
            MR_MATCH_SEQUENCE_NUMBER => Ok(Self::MatchSequenceNumber),
            MR_IS_EXCLUDED_ROW => Ok(Self::IsExcludedRow),
            _ => Err(()),
        }
    }
}

impl Display for MrMetadataColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

/// Build `__mr_symbol_<symbol>` column name
#[inline]
pub fn symbol_col_name(symbol: &str) -> String {
    format!("{MR_SYMBOL_PREFIX}{}", symbol.to_ascii_lowercase())
}

/// Build `__mr_classifier_<symbol>` column name
#[inline]
pub fn classifier_bits_col_name(symbol: &str) -> String {
    format!("{MR_CLASSIFIER_BITS_PREFIX}{}", symbol.to_ascii_lowercase())
}

/// Arrow fields for fixed MR metadata columns (non-nullable)
pub fn mr_metadata_arrow_fields() -> Vec<Arc<Field>> {
    MrMetadataColumn::all()
        .iter()
        .map(|col| Arc::new(Field::new(col.as_ref(), col.data_type(), col.is_nullable())))
        .collect()
}

/// DF (qualified) fields for MR metadata to append to a DFSchema
pub fn mr_metadata_df_fields() -> Vec<(Option<TableReference>, Arc<Field>)> {
    mr_metadata_arrow_fields()
        .into_iter()
        .map(|f| (None, f))
        .collect()
}

/// Arrow fields for a subset of MR metadata columns, preserving canonical order
pub fn mr_metadata_arrow_fields_subset(names: &[String]) -> Vec<Arc<Field>> {
    let name_set: std::collections::BTreeSet<&str> =
        names.iter().map(|s| s.as_str()).collect();
    MrMetadataColumn::all()
        .iter()
        .filter(|col| name_set.contains(col.as_ref()))
        .map(|col| Arc::new(Field::new(col.as_ref(), col.data_type(), col.is_nullable())))
        .collect()
}

/// DF fields for a subset of MR metadata columns, preserving canonical order
pub fn mr_metadata_df_fields_subset(
    names: &[String],
) -> Vec<(Option<TableReference>, Arc<Field>)> {
    mr_metadata_arrow_fields_subset(names)
        .into_iter()
        .map(|f| (None, f))
        .collect()
}

/// Arrow fields for a subset of MR metadata columns using enum variants
pub fn mr_metadata_arrow_fields_subset_enum(
    columns: &[MrMetadataColumn],
) -> Vec<Arc<Field>> {
    columns
        .iter()
        .map(|col| Arc::new(Field::new(col.as_ref(), col.data_type(), col.is_nullable())))
        .collect()
}

/// DF fields for a subset of MR metadata columns using enum variants
pub fn mr_metadata_df_fields_subset_enum(
    columns: &[MrMetadataColumn],
) -> Vec<(Option<TableReference>, Arc<Field>)> {
    mr_metadata_arrow_fields_subset_enum(columns)
        .into_iter()
        .map(|f| (None, f))
        .collect()
}

/// DF fields for DEFINE-derived symbol columns
pub fn symbol_df_fields(
    symbols: &[String],
    nullable: bool,
) -> Vec<(Option<TableReference>, Arc<Field>)> {
    symbols
        .iter()
        .map(|sym| {
            (
                None,
                Arc::new(Field::new(
                    symbol_col_name(sym),
                    arrow::datatypes::DataType::Boolean,
                    nullable,
                )),
            )
        })
        .collect()
}

/// DF fields for classifier bitset columns `__mr_classifier_<SYM>` (non-nullable booleans)
pub fn classifier_bits_df_fields(
    symbols: &[String],
) -> Vec<(Option<TableReference>, Arc<Field>)> {
    symbols
        .iter()
        .map(|sym| {
            (
                None,
                Arc::new(Field::new(
                    classifier_bits_col_name(sym),
                    arrow::datatypes::DataType::Boolean,
                    false,
                )),
            )
        })
        .collect()
}

/// Convert a slice of string names to enum variants, filtering out non-MR metadata columns
pub fn filter_mr_metadata_columns(names: &[String]) -> Vec<MrMetadataColumn> {
    names
        .iter()
        .filter_map(|name| name.parse::<MrMetadataColumn>().ok())
        .collect()
}

/// Check if a column name is an MR metadata column
pub fn is_mr_metadata_column(name: &str) -> bool {
    name.parse::<MrMetadataColumn>().is_ok()
}
