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

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions_nested::arrays_zip::{
    arrays_zip_inner_with_names, arrays_zip_return_type,
};
use std::sync::Arc;

/// Spark-compatible `arrays_zip` — struct field names follow the Spark naming
/// policy rather than the native [`ArraysZip`]'s 1-based ordinals.
///
/// Names reach the struct one of two ways, then go straight into the native
/// [`arrays_zip_return_type`] / [`arrays_zip_inner_with_names`]:
/// * [`SparkArraysZip::with_field_names`] — explicit names. For SQL, the
///   [`SparkArraysZipRewrite`] analyzer rule derives them from the argument
///   expressions (column / alias names, else 0-based ordinals) and pins them
///   here before optimizer passes can rename `arg_fields`.
/// * [`SparkArraysZip::new`] — nothing pinned; falls back to 0-based ordinals.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArraysZip {
    signature: Signature,
    field_names: Option<Vec<String>>,
}

impl Default for SparkArraysZip {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArraysZip {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            field_names: None,
        }
    }

    pub fn with_field_names(field_names: Vec<String>) -> Self {
        Self {
            field_names: Some(field_names),
            ..Self::new()
        }
    }

    /// Used by the analyzer to rewrite and to read back the baked-in names.
    pub(crate) fn field_names(&self) -> Option<&[String]> {
        self.field_names.as_deref()
    }

    fn resolve_names(&self, arg_fields: &[FieldRef]) -> Result<Vec<String>> {
        match &self.field_names {
            Some(names) => {
                if names.len() != arg_fields.len() {
                    return exec_err!(
                        "arrays_zip configured with {} field names but called with {} arguments",
                        names.len(),
                        arg_fields.len()
                    );
                }
                Ok(names.clone())
            }
            None => Ok((0..arg_fields.len()).map(|i| i.to_string()).collect()),
        }
    }
}

impl ScalarUDFImpl for SparkArraysZip {
    fn name(&self) -> &str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.is_empty() {
            return exec_err!("arrays_zip expects at least one array argument");
        }
        let names = self.resolve_names(args.arg_fields)?;
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let dt = arrays_zip_return_type(&arg_types, &names)?;
        Ok(Arc::new(Field::new(self.name(), dt, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("arrays_zip expects at least one array argument");
        }
        let names = self.resolve_names(&args.arg_fields)?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let result = arrays_zip_inner_with_names(&arrays, &names)?;
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{DataFusionError, assert_contains};

    fn int_field(name: &str) -> FieldRef {
        Arc::new(Field::new(name, DataType::Int64, true))
    }

    #[test]
    fn resolve_names_some_used_when_length_matches() {
        let udf = SparkArraysZip::with_field_names(vec!["a".into(), "b".into()]);
        let names = udf.resolve_names(&[int_field("x"), int_field("y")]).unwrap();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn resolve_names_some_length_mismatch_errors() {
        let udf = SparkArraysZip::with_field_names(vec!["a".into(), "b".into()]);
        let err = udf.resolve_names(&[int_field("x")]).unwrap_err();

        assert!(
            matches!(err, DataFusionError::Execution(_)),
            "unexpected error kind: {err}"
        );
        assert_contains!(
            err.to_string(),
            "arrays_zip configured with 2 field names but called with 1 arguments"
        );
    }

    #[test]
    fn resolve_names_none_uses_zero_based_ordinals() {
        let udf = SparkArraysZip::new();
        // Field names are ignored — positional ordinals stay stable under the
        // optimizer renames that make `arg_fields`-based naming unsafe.
        let names = udf
            .resolve_names(&[int_field("t.a"), int_field("b"), int_field("c")])
            .unwrap();
        assert_eq!(names, vec!["0", "1", "2"]);
    }
}
