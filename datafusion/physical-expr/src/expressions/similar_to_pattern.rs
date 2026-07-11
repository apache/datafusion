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

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::PhysicalExpr;

use arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{DataType, FieldRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::ColumnarValue;
use datafusion_expr_common::placement::ExpressionPlacement;
use datafusion_expr_common::sort_properties::ExprProperties;
use datafusion_physical_expr_common::physical_expr::fmt_sql;

pub fn sql_similar_to_regex(pattern: &str) -> String {
    let mut result = String::with_capacity(pattern.len() + 10);
    result.push_str("^(?:");
    let mut in_bracket = false;
    for ch in pattern.chars() {
        match (ch, in_bracket) {
            ('%', false) => result.push_str("(?s:.*)"),
            ('_', false) => result.push_str("(?s:.)"),
            ('[', false) => {
                result.push('[');
                in_bracket = true;
            }
            (']', true) => {
                result.push(']');
                in_bracket = false;
            }
            // `. ^ $` are SQL literals but regex metachars when not inside a
            // bracket expression; `\` is a regex escape character in all
            // positions, so it always needs escaping.
            ('.' | '^' | '$', false) | ('\\', _) => {
                result.push('\\');
                result.push(ch);
            }
            (c, _) => result.push(c),
        }
    }
    result.push_str(")$");
    result
}

#[derive(Debug, Eq)]
pub struct SqlSimilarToPattern {
    expr: Arc<dyn PhysicalExpr>,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for SqlSimilarToPattern {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
    }
}

impl Hash for SqlSimilarToPattern {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
    }
}

impl SqlSimilarToPattern {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }

    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }
}

impl fmt::Display for SqlSimilarToPattern {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "sql_similar_to_regex({})", self.expr)
    }
}

impl PhysicalExpr for SqlSimilarToPattern {
    fn data_type(&self, input_schema: &arrow::datatypes::Schema) -> Result<DataType> {
        self.expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &arrow::datatypes::Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.expr.evaluate(batch)? {
            ColumnarValue::Array(array) => translate_array(&array),
            ColumnarValue::Scalar(scalar) => {
                Ok(ColumnarValue::Scalar(translate_scalar(&scalar)?))
            }
        }
    }

    fn return_field(&self, input_schema: &arrow::datatypes::Schema) -> Result<FieldRef> {
        self.expr.return_field(input_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(SqlSimilarToPattern::new(Arc::clone(&children[0]))))
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(children[0].clone())
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sql_similar_to_regex({})", fmt_sql(self.expr.as_ref()))
    }

    fn placement(&self) -> ExpressionPlacement {
        ExpressionPlacement::KeepInPlace
    }
}

pub fn translate_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => {
            Ok(ScalarValue::Utf8(Some(sql_similar_to_regex(s))))
        }
        ScalarValue::LargeUtf8(Some(s)) => {
            Ok(ScalarValue::LargeUtf8(Some(sql_similar_to_regex(s))))
        }
        ScalarValue::Utf8View(Some(s)) => {
            Ok(ScalarValue::Utf8View(Some(sql_similar_to_regex(s))))
        }
        ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::Null => Ok(ScalarValue::Utf8(None)),
        other => internal_err!("SIMILAR TO pattern must be a string type, got {other:?}"),
    }
}

fn translate_array(array: &ArrayRef) -> Result<ColumnarValue> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let translated: StringArray = arr
                .iter()
                .map(|opt| opt.map(sql_similar_to_regex))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(translated)))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let translated: LargeStringArray = arr
                .iter()
                .map(|opt| opt.map(sql_similar_to_regex))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(translated)))
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            let translated: StringViewArray = arr
                .iter()
                .map(|opt| opt.map(sql_similar_to_regex))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(translated)))
        }
        other => internal_err!("SIMILAR TO pattern must be a string type, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit};
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::cast::as_string_array;

    #[test]
    fn test_translate_scalar() -> Result<()> {
        assert_eq!(
            translate_scalar(&ScalarValue::Utf8(Some("a%".to_string())))?,
            ScalarValue::Utf8(Some(r"^(?:a(?s:.*))$".to_string()))
        );
        assert_eq!(
            translate_scalar(&ScalarValue::Utf8(Some("a.b".to_string())))?,
            ScalarValue::Utf8(Some(r"^(?:a\.b)$".to_string()))
        );
        assert!(
            translate_scalar(&ScalarValue::Null).is_ok(),
            "NULL pattern should pass through"
        );
        assert_eq!(
            translate_scalar(&ScalarValue::Null).unwrap(),
            ScalarValue::Utf8(None)
        );
        assert!(translate_scalar(&ScalarValue::Int32(Some(1))).is_err());
        Ok(())
    }

    #[test]
    fn test_translate_array() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let input = StringArray::from(vec!["a%", "a.b", "a_b"]);
        let expr = SqlSimilarToPattern::new(col("a", &schema)?);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(input)])?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = as_string_array(&result)?;
        assert_eq!(result.value(0), r"^(?:a(?s:.*))$");
        assert_eq!(result.value(1), r"^(?:a\.b)$");
        assert_eq!(result.value(2), r"^(?:a(?s:.)b)$");
        Ok(())
    }

    #[test]
    fn test_display_and_sql() -> Result<()> {
        let expr = SqlSimilarToPattern::new(lit("a%"));
        assert_eq!("sql_similar_to_regex(a%)", format!("{expr}"));
        Ok(())
    }
}
