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

//! Typed wrapper for parquet virtual columns.
//!
//! arrow-rs identifies virtual columns via arrow extension types carried on
//! the `FieldRef`. [`ParquetVirtualColumn`] lifts that contract into the type
//! system so callers validate at the boundary (via `TryFrom<&FieldRef>`)
//! rather than string-comparing extension-type names deep inside the reader.

use arrow::datatypes::FieldRef;
use arrow_schema::extension::ExtensionType;
use datafusion_common::{DataFusionError, Result, not_impl_err};
use parquet::arrow::RowNumber;
use std::sync::Arc;

/// A parquet virtual column validated to have a supported arrow extension
/// type.
///
/// Construct via [`TryFrom<&FieldRef>`]; add a new variant (and update the
/// `TryFrom` impl) when DataFusion gains support for another arrow-rs virtual
/// extension type.
#[derive(Debug, Clone)]
pub enum ParquetVirtualColumn {
    /// Absolute row number within the parquet file. Backed by arrow-rs's
    /// [`RowNumber`] extension type.
    RowNumber(FieldRef),
}

impl ParquetVirtualColumn {
    pub fn field(&self) -> &FieldRef {
        match self {
            Self::RowNumber(field) => field,
        }
    }
}

impl From<ParquetVirtualColumn> for FieldRef {
    fn from(col: ParquetVirtualColumn) -> Self {
        match col {
            ParquetVirtualColumn::RowNumber(field) => field,
        }
    }
}

impl TryFrom<&FieldRef> for ParquetVirtualColumn {
    type Error = DataFusionError;

    fn try_from(field: &FieldRef) -> Result<Self> {
        let Some(name) = field.extension_type_name() else {
            return not_impl_err!(
                "Virtual column '{}' is missing an Arrow extension type; \
                 supported extension types: [{}]",
                field.name(),
                RowNumber::NAME
            );
        };
        match name {
            n if n == RowNumber::NAME => Ok(Self::RowNumber(Arc::clone(field))),
            other => not_impl_err!(
                "Virtual column '{}' uses unsupported Arrow extension type '{}'; \
                 supported types: [{}]. Add a ParquetVirtualColumn variant and \
                 a test for this type before wiring it through.",
                field.name(),
                other,
                RowNumber::NAME
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn row_number_field_converts() {
        let field: FieldRef = Arc::new(
            Field::new("row_number", DataType::Int64, false)
                .with_extension_type(RowNumber),
        );
        let col = ParquetVirtualColumn::try_from(&field).expect("valid row_number");
        assert!(matches!(col, ParquetVirtualColumn::RowNumber(_)));
        assert_eq!(col.field().name(), "row_number");
    }

    #[test]
    fn missing_extension_type_rejected() {
        let field: FieldRef = Arc::new(Field::new("plain", DataType::Int64, false));
        let err = ParquetVirtualColumn::try_from(&field).unwrap_err();
        assert!(
            err.to_string().contains("missing an Arrow extension type"),
            "got: {err}"
        );
    }

    #[test]
    fn unsupported_extension_type_rejected() {
        // RowGroupIndex is a real arrow-rs virtual type not yet in our enum.
        let field: FieldRef = Arc::new(
            Field::new("row_group_index", DataType::Int64, false)
                .with_extension_type(parquet::arrow::RowGroupIndex),
        );
        let err = ParquetVirtualColumn::try_from(&field).unwrap_err();
        assert!(
            err.to_string().contains("parquet.virtual.row_group_index"),
            "error should name the offending extension type, got: {err}"
        );
    }
}
