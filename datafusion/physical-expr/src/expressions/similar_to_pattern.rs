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
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::ColumnarValue;
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

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sql_similar_to_regex({})", fmt_sql(self.expr.as_ref()))
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::SqlSimilarToPattern(
                Box::new(protobuf::PhysicalSqlSimilarToPatternNode {
                    expr: Some(Box::new(ctx.encode_child(&self.expr)?)),
                }),
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl SqlSimilarToPattern {
    /// Reconstruct a [`SqlSimilarToPattern`] from its protobuf representation.
    ///
    /// Takes the whole [`PhysicalExprNode`] so the decode signature matches
    /// other migrated expressions and can inspect outer-node metadata if
    /// needed in the future.
    ///
    /// [`PhysicalExprNode`]: datafusion_proto_models::protobuf::PhysicalExprNode
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_proto_models::protobuf;

        let pattern = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::SqlSimilarToPattern,
            "SqlSimilarToPattern",
        );

        Ok(Arc::new(SqlSimilarToPattern::new(
            ctx.decode_required_expression(
                pattern.expr.as_deref(),
                "SqlSimilarToPattern",
                "expr",
            )?,
        )))
    }
}

pub(crate) fn translate_scalar(scalar: &ScalarValue) -> Result<ScalarValue> {
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
        // A NULL pattern makes the whole predicate NULL, but the string variant
        // still has to survive the translation: the regex kernel dispatches on
        // the left-hand type and downcasts the pattern to the same array type.
        ScalarValue::Utf8(None) => Ok(ScalarValue::Utf8(None)),
        ScalarValue::LargeUtf8(None) => Ok(ScalarValue::LargeUtf8(None)),
        ScalarValue::Utf8View(None) => Ok(ScalarValue::Utf8View(None)),
        ScalarValue::Null => Ok(ScalarValue::Utf8(None)),
        other => exec_err!("SIMILAR TO pattern must be a string type, got {other:?}"),
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
        other => exec_err!("SIMILAR TO pattern must be a string type, got {other:?}"),
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
        // A NULL pattern keeps its string variant: the regex kernel dispatches
        // on the left-hand type and downcasts the pattern to the same type.
        assert_eq!(
            translate_scalar(&ScalarValue::LargeUtf8(None))?,
            ScalarValue::LargeUtf8(None)
        );
        assert_eq!(
            translate_scalar(&ScalarValue::Utf8View(None))?,
            ScalarValue::Utf8View(None)
        );
        // Non-`Utf8` patterns keep their variant when translated too.
        assert_eq!(
            translate_scalar(&ScalarValue::LargeUtf8(Some("a%".to_string())))?,
            ScalarValue::LargeUtf8(Some(r"^(?:a(?s:.*))$".to_string()))
        );
        assert_eq!(
            translate_scalar(&ScalarValue::Utf8View(Some("a%".to_string())))?,
            ScalarValue::Utf8View(Some(r"^(?:a(?s:.*))$".to_string()))
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

/// Tests for the `try_to_proto` / `try_from_proto` hooks.
#[cfg(all(test, feature = "proto"))]
mod proto_tests {
    use super::*;
    use crate::expressions::{Column, col};
    use crate::proto_test_util::{
        StubDecoder, StubEncoder, UnreachableDecoder, column_node,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DataFusionError;
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_proto_models::protobuf::{
        PhysicalExprNode, PhysicalSqlSimilarToPatternNode, physical_expr_node,
    };

    /// Build a `SqlSimilarToPattern` proto node with the given child.
    fn pattern_node(expr: Option<Box<PhysicalExprNode>>) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_id: None,
            expr_type: Some(physical_expr_node::ExprType::SqlSimilarToPattern(Box::new(
                PhysicalSqlSimilarToPatternNode { expr },
            ))),
        }
    }

    /// A `SqlSimilarToPattern` over a `Utf8` column.
    fn pattern_fixture() -> SqlSimilarToPattern {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        SqlSimilarToPattern::new(col("a", &schema).unwrap())
    }

    #[test]
    fn try_to_proto_encodes_sql_similar_to_pattern() {
        let pattern = pattern_fixture();
        let encoder = StubEncoder::ok();
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let node = pattern
            .try_to_proto(&ctx)
            .unwrap()
            .expect("SqlSimilarToPattern should encode to Some(node)");

        // Built-in exprs never set expr_id; only dynamic filters do.
        assert!(node.expr_id.is_none());
        let pattern_node = match node.expr_type {
            Some(physical_expr_node::ExprType::SqlSimilarToPattern(boxed)) => *boxed,
            other => panic!("expected a SqlSimilarToPattern node, got {other:?}"),
        };
        assert!(pattern_node.expr.is_some());
    }

    #[test]
    fn try_to_proto_propagates_expr_encode_error() {
        let pattern = pattern_fixture();
        let encoder = StubEncoder::failing_on(1);
        let ctx = PhysicalExprEncodeCtx::new(&encoder);
        let err = pattern.try_to_proto(&ctx).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(msg) if msg.contains("call 1")));
    }

    #[test]
    fn try_from_proto_decodes_sql_similar_to_pattern() {
        let node = pattern_node(Some(Box::new(column_node("a"))));
        let schema = Schema::empty();
        let decoder = StubDecoder::ok();
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = SqlSimilarToPattern::try_from_proto(&node, &ctx).unwrap();
        let pattern = decoded
            .downcast_ref::<SqlSimilarToPattern>()
            .expect("decoded expr should be a SqlSimilarToPattern");
        assert!(pattern.expr().downcast_ref::<Column>().is_some());
    }

    #[test]
    fn try_from_proto_rejects_non_sql_similar_to_pattern_node() {
        let node = column_node("a");
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = SqlSimilarToPattern::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg)
                if msg.contains("PhysicalExprNode is not a SqlSimilarToPattern")
        ));
    }

    #[test]
    fn try_from_proto_rejects_missing_expr() {
        let node = pattern_node(None);
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = SqlSimilarToPattern::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg)
                if msg.contains("SqlSimilarToPattern is missing required field 'expr'")
        ));
    }

    #[test]
    fn try_from_proto_propagates_expr_decode_error() {
        let node = pattern_node(Some(Box::new(column_node("a"))));
        let schema = Schema::empty();
        let decoder = StubDecoder::failing_on(1);
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);
        let err = SqlSimilarToPattern::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(msg) if msg.contains("call 1")));
    }
}
