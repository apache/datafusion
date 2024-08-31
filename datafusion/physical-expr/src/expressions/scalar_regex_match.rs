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

use super::Literal;
use arrow::array::ArrayData;
use arrow_array::{
    Array, ArrayAccessor, BooleanArray, LargeStringArray, StringArray, StringViewArray,
};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::{DataType, Schema};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::{down_cast_any_ref, PhysicalExpr};
use regex::Regex;
use std::{any::Any, hash::Hash, sync::Arc};

/// ScalarRegexMatchExpr
/// Only used when evaluating regexp matching with literal pattern.
/// Example regex expression: c1 ~ '^a' / c1 !~ '^a' / c1 ~* '^a' / c1 !~* '^a'.
/// Literal regexp pattern will be compiled once and cached to be reused in execution.
/// It's will save compile time of pre execution and speed up execution.
#[derive(Clone)]
pub struct ScalarRegexMatchExpr {
    negated: bool,
    case_insensitive: bool,
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
    compiled: Option<Regex>,
}

impl ScalarRegexMatchExpr {
    pub fn new(
        negated: bool,
        case_insensitive: bool,
        expr: Arc<dyn PhysicalExpr>,
        pattern: Arc<dyn PhysicalExpr>,
    ) -> Self {
        let mut res = Self {
            negated,
            case_insensitive,
            expr,
            pattern,
            compiled: None,
        };
        res.compile().unwrap();
        res
    }

    /// Is negated
    pub fn negated(&self) -> bool {
        self.negated
    }

    /// Is case insensitive
    pub fn case_insensitive(&self) -> bool {
        self.case_insensitive
    }

    /// Input expression
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Pattern expression
    pub fn pattern(&self) -> &Arc<dyn PhysicalExpr> {
        &self.pattern
    }

    /// Compile regex pattern
    fn compile(&mut self) -> datafusion_common::Result<()> {
        let scalar_pattern =
            self.pattern
                .as_any()
                .downcast_ref::<Literal>()
                .and_then(|pattern| match pattern.value() {
                    ScalarValue::Null
                    | ScalarValue::Utf8(None)
                    | ScalarValue::Utf8View(None)
                    | ScalarValue::LargeUtf8(None) => Some(None),
                    ScalarValue::Utf8(Some(pattern))
                    | ScalarValue::Utf8View(Some(pattern))
                    | ScalarValue::LargeUtf8(Some(pattern)) => {
                        let mut pattern = pattern.to_string();
                        if self.case_insensitive {
                            pattern = format!("(?i){}", pattern);
                        }
                        Some(Some(pattern))
                    }
                    _ => None,
                });
        match scalar_pattern {
            Some(Some(scalar_pattern)) => Regex::new(scalar_pattern.as_str())
                .map(|compiled| {
                    self.compiled = Some(compiled);
                })
                .map_err(|err| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "Failed to compile regex: {}",
                        err
                    ))
                }),
            Some(None) => {
                self.compiled = None;
                Ok(())
            }
            None => Err(datafusion_common::DataFusionError::Internal(format!(
                "Regex pattern({}) isn't literal string",
                self.pattern
            ))),
        }
    }

    /// Operator name
    fn op_name(&self) -> &str {
        match (self.negated, self.case_insensitive) {
            (false, false) => "MATCH",
            (true, false) => "NOT MATCH",
            (false, true) => "IMATCH",
            (true, true) => "NOT IMATCH",
        }
    }
}

impl ScalarRegexMatchExpr {
    /// Evaluate the scalar regex match expression match array value
    fn evaluate_array(
        &self,
        array: &Arc<dyn Array>,
    ) -> datafusion_common::Result<ColumnarValue> {
        macro_rules! downcast_string_array {
            ($ARRAY:expr, $ARRAY_TYPE:ident, $ERR_MSG:expr) => {
                &($ARRAY
                    .as_any()
                    .downcast_ref::<$ARRAY_TYPE>()
                    .expect($ERR_MSG))
            };
        }
        match array.data_type() {
            DataType::Null => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            },
            DataType::Utf8 => array_regexp_match(
                downcast_string_array!(array, StringArray, "Failed to downcast StringArray"), 
                self.compiled.as_ref().unwrap(),
                self.negated,
            ),
            DataType::Utf8View => array_regexp_match(
                downcast_string_array!(array, StringViewArray, "Failed to downcast StringViewArray"),
                self.compiled.as_ref().unwrap(),
                self.negated,
            ),
            DataType::LargeUtf8 => array_regexp_match(
                downcast_string_array!(array, LargeStringArray, "Failed to downcast LargeStringArray"),
                self.compiled.as_ref().unwrap(),
                self.negated,
            ),
            other=> datafusion_common::internal_err!(
                "Data type {:?} not supported for ScalarRegexMatchExpr, expect Utf8|Utf8View|LargeUtf8", other
            ),
        }
    }

    /// Evaluate the scalar regex match expression match scalar value
    fn evaluate_scalar(
        &self,
        scalar: &ScalarValue,
    ) -> datafusion_common::Result<ColumnarValue> {
        match scalar {
            ScalarValue::Null
            | ScalarValue::Utf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::LargeUtf8(None) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None))),
            ScalarValue::Utf8(Some(scalar))
            | ScalarValue::Utf8View(Some(scalar))
            | ScalarValue::LargeUtf8(Some(scalar)) => {
                let mut result = self.compiled.as_ref().unwrap().is_match(scalar);
                if self.negated {
                    result = !result;
                }
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
            },
            other=> datafusion_common::internal_err!(
                "Data type {:?} not supported for ScalarRegexMatchExpr, expect Utf8|Utf8View|LargeUtf8", other
            ),
        }
    }
}

impl std::hash::Hash for ScalarRegexMatchExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.negated.hash(state);
        self.case_insensitive.hash(state);
        self.expr.hash(state);
        self.pattern.hash(state);
    }
}

impl std::cmp::PartialEq for ScalarRegexMatchExpr {
    fn eq(&self, other: &Self) -> bool {
        self.negated.eq(&other.negated)
            && self.case_insensitive.eq(&self.case_insensitive)
            && self.expr.eq(&other.expr)
            && self.pattern.eq(&other.pattern)
    }
}

impl std::fmt::Debug for ScalarRegexMatchExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScalarRegexMatchExpr")
            .field("negated", &self.negated)
            .field("case_insensitive", &self.case_insensitive)
            .field("expr", &self.expr)
            .field("pattern", &self.pattern)
            .finish()
    }
}

impl std::fmt::Display for ScalarRegexMatchExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {} {}", self.expr, self.op_name(), self.pattern)
    }
}

impl PhysicalExpr for ScalarRegexMatchExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _: &arrow_schema::Schema,
    ) -> datafusion_common::Result<arrow_schema::DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(
        &self,
        input_schema: &arrow_schema::Schema,
    ) -> datafusion_common::Result<bool> {
        Ok(self.expr.nullable(input_schema)? || self.pattern.nullable(input_schema)?)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion_common::Result<ColumnarValue> {
        self.expr
            .evaluate(batch)
            .and_then(|lhs| {
                if self.compiled.is_some() {
                    match &lhs {
                        ColumnarValue::Array(array) => self.evaluate_array(array),
                        ColumnarValue::Scalar(scalar) => self.evaluate_scalar(scalar),
                    }
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
                }
            })
            .and_then(|result| result.into_array(batch.num_rows()))
            .map(ColumnarValue::Array)
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn PhysicalExpr>> {
        vec![&self.expr, &self.pattern]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<std::sync::Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarRegexMatchExpr::new(
            self.negated,
            self.case_insensitive,
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for ScalarRegexMatchExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

/// It is used for scalar regexp matching and copy from arrow-rs
fn array_regexp_match(
    array: &dyn ArrayAccessor<Item = &str>,
    regex: &Regex,
    negated: bool,
) -> datafusion_common::Result<ColumnarValue> {
    let null_bit_buffer = array.nulls().map(|x| x.inner().sliced());
    let mut buffer_builder = BooleanBufferBuilder::new(array.len());

    if regex.as_str().is_empty() {
        buffer_builder.append_n(array.len(), true);
    } else {
        for i in 0..array.len() {
            let value = array.value(i);
            buffer_builder.append(regex.is_match(value));
        }
    }

    let buffer = buffer_builder.into();
    let bool_array = BooleanArray::from(unsafe {
        ArrayData::new_unchecked(
            DataType::Boolean,
            array.len(),
            None,
            null_bit_buffer,
            0,
            vec![buffer],
            vec![],
        )
    });

    let bool_array = if negated {
        arrow::compute::kernels::boolean::not(&bool_array)
    } else {
        Ok(bool_array)
    };

    bool_array
        .map_err(|err| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to evaluate regex: {}",
                err
            ))
        })
        .map(|bool_array| ColumnarValue::Array(Arc::new(bool_array)))
}

/// Create a scalar regex match expression, erroring if the argument types are not compatible.
pub fn scalar_regex_match(
    negated: bool,
    case_insensitive: bool,
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
    let valid_data_type = |data_type: &DataType| {
        if !matches!(
            data_type,
            DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        ) {
            return datafusion_common::internal_err!(
                "The type {data_type} not supported for scalar_regex_match, expect Null|Utf8|Utf8View|LargeUtf8"
            );
        }
        Ok(())
    };

    for arg_expr in [&expr, &pattern] {
        arg_expr
            .data_type(input_schema)
            .and_then(|data_type| valid_data_type(&data_type))?;
    }

    Ok(Arc::new(ScalarRegexMatchExpr::new(
        negated,
        case_insensitive,
        expr,
        pattern,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit};
    use arrow::record_batch::RecordBatch;
    use arrow_array::ArrayRef;
    use arrow_array::NullArray;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use rstest::rstest;
    use std::sync::Arc;

    fn test_schema(typ: DataType) -> Schema {
        Schema::new(vec![Field::new("c1", typ, false)])
    }

    #[rstest(
        negated, case_insensitive, typ, a_vec, b_lit, c_vec,
        case(
            false, false, DataType::Utf8,
            Arc::new(StringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            "^a",
            Arc::new(BooleanArray::from(vec![true, false, false, false, false])),
        ),
        case(
            false, true, DataType::Utf8,
            Arc::new(StringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            "^a",
            Arc::new(BooleanArray::from(vec![true, false, true, false, false])),
        ),
        case(
            true, false, DataType::Utf8,
            Arc::new(StringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            "^a",
            Arc::new(BooleanArray::from(vec![false, true, true, true, true])),
        ),
        case(
            true, true, DataType::Utf8,
            Arc::new(StringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            "^a",
            Arc::new(BooleanArray::from(vec![false, true, false, true, true])),
        ),
        case(
            true, true, DataType::Utf8,
            Arc::new(StringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::Utf8(None),
            Arc::new(BooleanArray::from(vec![None, None, None, None, None])),
        ),
        case(
            false, false, DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::LargeUtf8(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![true, false, false, false, false])),
        ),
        case(
            false, true, DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::LargeUtf8(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![true, false, true, false, false])),
        ),
        case(
            true, false, DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::LargeUtf8(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![false, true, true, true, true])),
        ),
        case(
            true, true, DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::LargeUtf8(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![false, true, false, true, true])),
        ),
        case(
            true, true, DataType::LargeUtf8,
            Arc::new(LargeStringArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::LargeUtf8(None),
            Arc::new(BooleanArray::from(vec![None, None, None, None, None])),
        ),
        case(
            false, false, DataType::Utf8View,
            Arc::new(StringViewArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::Utf8View(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![true, false, false, false, false])),
        ),
        case(
            false, true, DataType::Utf8View,
            Arc::new(StringViewArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::Utf8View(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![true, false, true, false, false])),
        ),
        case(
            true, false, DataType::Utf8View,
            Arc::new(StringViewArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::Utf8View(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![false, true, true, true, true])),
        ),
        case(
            true, true, DataType::Utf8View,
            Arc::new(StringViewArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::Utf8View(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![false, true, false, true, true])),
        ),
        case(
            true, true, DataType::Utf8View,
            Arc::new(StringViewArray::from(vec!["abc", "bbb", "ABC", "ba", "cba"])),
            ScalarValue::Utf8View(None),
            Arc::new(BooleanArray::from(vec![None, None, None, None, None])),
        ),
        case(
            true, true, DataType::Null,
            Arc::new(NullArray::new(5)),
            ScalarValue::Utf8View(Some("^a".to_string())),
            Arc::new(BooleanArray::from(vec![None, None, None, None, None])),
        ),
    )]
    fn test_scalar_regex_match_array(
        negated: bool,
        case_insensitive: bool,
        typ: DataType,
        a_vec: ArrayRef,
        b_lit: impl datafusion_expr::Literal,
        c_vec: ArrayRef,
    ) {
        let schema = test_schema(typ);
        let left = col("c1", &schema).unwrap();
        let right = lit(b_lit);

        // verify that we can construct the expression
        let expression =
            scalar_regex_match(negated, case_insensitive, left, right, &schema).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a_vec]).unwrap();

        // verify that the expression's type is correct
        assert_eq!(expression.data_type(&schema).unwrap(), DataType::Boolean);

        // compute
        let result = expression
            .evaluate(&batch)
            .expect("Error evaluating expression");

        if let ColumnarValue::Array(array) = result {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("failed to downcast to BooleanArray");

            let c_vec = c_vec
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("failed to downcast to BooleanArray");
            // verify that the result is correct
            assert_eq!(array, c_vec);
        } else {
            panic!("result was not an array");
        }
    }

    #[rstest(
        negated, case_insensitive, typ, a_lit, b_lit, flag,
        case(
            false, false, DataType::Utf8, "abc", "^a", Some(true),
        ),
        case(
            false, true, DataType::Utf8, "Abc", "^a", Some(true),
        ),
        case(
            true, false, DataType::Utf8, "abc", "^a", Some(false),
        ),
        case(
            true, true, DataType::Utf8, "Abc", "^a", Some(false),
        ),
        case(
            true, true, DataType::Utf8,
            ScalarValue::Utf8(Some("Abc".to_string())),
            ScalarValue::Utf8(None),
            None,
        ),
        case(
            false, false, DataType::Utf8,
            ScalarValue::Utf8(Some("abc".to_string())),
            ScalarValue::LargeUtf8(Some("^a".to_string())),
            Some(true),
        ),
        case(
            false, true, DataType::Utf8,
            ScalarValue::Utf8(Some("Abc".to_string())),
            ScalarValue::LargeUtf8(Some("^a".to_string())),
            Some(true),
        ),
        case(
            true, false, DataType::Utf8,
            ScalarValue::Utf8(Some("abc".to_string())),
            ScalarValue::LargeUtf8(Some("^a".to_string())),
            Some(false),
        ),
        case(
            true, true, DataType::Utf8,
            ScalarValue::Utf8(Some("Abc".to_string())),
            ScalarValue::LargeUtf8(Some("^a".to_string())),
            Some(false),
        ),
        case(
            true, true, DataType::Utf8,
            ScalarValue::Utf8(Some("Abc".to_string())),
            ScalarValue::LargeUtf8(None),
            None,
        ),
    )]
    fn test_scalar_regex_match_scalar(
        negated: bool,
        case_insensitive: bool,
        typ: DataType,
        a_lit: impl datafusion_expr::Literal,
        b_lit: impl datafusion_expr::Literal,
        flag: Option<bool>,
    ) {
        let left = lit(a_lit);
        let right = lit(b_lit);
        let schema = test_schema(typ);
        let expression =
            scalar_regex_match(negated, case_insensitive, left, right, &schema).unwrap();
        let num_rows: usize = 3;
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from([""].repeat(num_rows)))],
        )
        .unwrap();

        // verify that the expression's type is correct
        assert_eq!(expression.data_type(&schema).unwrap(), DataType::Boolean);

        // compute
        let result = expression
            .evaluate(&batch)
            .expect("Error evaluating expression");

        if let ColumnarValue::Array(array) = result {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("failed to downcast to BooleanArray");

            // verify that the result is correct
            let c_vec = [flag].repeat(batch.num_rows());
            assert_eq!(array, &BooleanArray::from(c_vec));
        } else {
            panic!("result was not an array");
        }
    }

    #[rstest(
        expr, pattern,
        case(
            col("c1", &test_schema(DataType::Utf8)).unwrap(),
            lit(1),
        ),
        case(
            lit(1),
            col("c1", &test_schema(DataType::Utf8)).unwrap(),
        ),
    )]
    #[should_panic]
    fn test_scalar_regex_match_panic(
        expr: Arc<dyn PhysicalExpr>,
        pattern: Arc<dyn PhysicalExpr>,
    ) {
        let _ =
            scalar_regex_match(false, false, expr, pattern, &test_schema(DataType::Utf8))
                .unwrap();
    }

    #[rstest(
        pattern,
        case(col("c1", &test_schema(DataType::Utf8)).unwrap()), // not literal 
        case(lit(1)), // not literal string
        case(lit("\\x{202e")), // wrong regex pattern
    )]
    #[should_panic]
    fn test_scalar_regex_match_compile_error(pattern: Arc<dyn PhysicalExpr>) {
        let _ = ScalarRegexMatchExpr::new(false, false, lit("a"), pattern);
    }
}
