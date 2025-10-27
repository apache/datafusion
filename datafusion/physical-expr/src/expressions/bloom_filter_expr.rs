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

//! Bloom filter physical expression

use crate::bloom_filter::Sbbf;
use crate::PhysicalExpr;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;
use std::any::Any;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A progressive builder for creating bloom filters
///
/// This builder allows incremental insertion of values from record batches
/// and produces a static `BloomFilterExpr` when finished.
///
/// # Example
/// ```ignore
/// let mut builder = BloomFilterBuilder::new(1000, 0.01)?;
/// builder.insert_scalar(&ScalarValue::Int32(Some(42)))?;
/// builder.insert_array(&int_array)?;
/// let expr = builder.finish(col_expr)?;
/// ```
pub struct BloomFilterBuilder {
    /// The underlying bloom filter
    sbbf: Sbbf,
}

impl BloomFilterBuilder {
    /// Create a new bloom filter builder
    ///
    /// # Arguments
    /// * `ndv` - Expected number of distinct values
    /// * `fpp` - Desired false positive probability (0.0 to 1.0)
    pub fn new(ndv: u64, fpp: f64) -> Result<Self> {
        let sbbf = Sbbf::new_with_ndv_fpp(ndv, fpp)?;
        Ok(Self { sbbf })
    }

    /// Insert a single scalar value into the bloom filter
    pub fn insert_scalar(&mut self, value: &ScalarValue) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        match value {
            ScalarValue::Boolean(Some(v)) => self.sbbf.insert(v),
            ScalarValue::Int32(Some(v)) => self.sbbf.insert(v),
            ScalarValue::Int64(Some(v)) => self.sbbf.insert(v),
            ScalarValue::UInt32(Some(v)) => self.sbbf.insert(v),
            ScalarValue::UInt64(Some(v)) => self.sbbf.insert(v),
            ScalarValue::Float32(Some(v)) => self.sbbf.insert(v),
            ScalarValue::Float64(Some(v)) => self.sbbf.insert(v),
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                self.sbbf.insert(v.as_str())
            }
            ScalarValue::Utf8View(Some(v)) => self.sbbf.insert(v.as_str()),
            ScalarValue::Binary(Some(v))
            | ScalarValue::LargeBinary(Some(v))
            | ScalarValue::FixedSizeBinary(_, Some(v)) => self.sbbf.insert(v.as_slice()),
            ScalarValue::BinaryView(Some(v)) => self.sbbf.insert(v.as_slice()),
            _ => {
                return exec_err!(
                    "Unsupported data type for bloom filter: {}",
                    value.data_type()
                )
            }
        }
        Ok(())
    }

    /// Insert all non-null values from an array into the bloom filter
    pub fn insert_array(&mut self, array: &ArrayRef) -> Result<()> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        match array.data_type() {
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(&array.value(i));
                    }
                }
            }
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(&array.value(i));
                    }
                }
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(&array.value(i));
                    }
                }
            }
            DataType::UInt32 => {
                let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(&array.value(i));
                    }
                }
            }
            DataType::UInt64 => {
                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(&array.value(i));
                    }
                }
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(&array.value(i));
                    }
                }
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(&array.value(i));
                    }
                }
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(array.value(i));
                    }
                }
            }
            DataType::LargeUtf8 => {
                let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(array.value(i));
                    }
                }
            }
            DataType::Utf8View => {
                let array = array.as_any().downcast_ref::<StringViewArray>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(array.value(i));
                    }
                }
            }
            DataType::Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(array.value(i));
                    }
                }
            }
            DataType::LargeBinary => {
                let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(array.value(i));
                    }
                }
            }
            DataType::BinaryView => {
                let array = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(array.value(i));
                    }
                }
            }
            DataType::FixedSizeBinary(_) => {
                let array = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        self.sbbf.insert(array.value(i));
                    }
                }
            }
            _ => {
                return exec_err!(
                    "Unsupported data type for bloom filter: {}",
                    array.data_type()
                )
            }
        }
        Ok(())
    }

    /// Finish building and create a `BloomFilterExpr`
    ///
    /// This consumes the builder and returns a static PhysicalExpr
    /// that checks values against the constructed bloom filter.
    ///
    /// # Arguments
    /// * `expr` - The expression to evaluate and check against the bloom filter
    pub fn finish(self, expr: Arc<dyn PhysicalExpr>) -> BloomFilterExpr {
        BloomFilterExpr::new(expr, self.sbbf)
    }
}

/// Physical expression that checks values against a bloom filter
///
/// This is a static expression (similar to `InListExpr`) that evaluates
/// a child expression and checks each value against a pre-built bloom filter.
/// Returns a boolean array indicating whether each value might be present
/// (true) or is definitely absent (false).
///
/// Note: Bloom filters can produce false positives but never false negatives.
#[derive(Debug, Clone)]
pub struct BloomFilterExpr {
    /// The expression to evaluate
    expr: Arc<dyn PhysicalExpr>,
    /// The bloom filter to check against
    bloom_filter: Arc<Sbbf>,
}

impl BloomFilterExpr {
    /// Create a new bloom filter expression
    pub fn new(expr: Arc<dyn PhysicalExpr>, bloom_filter: Sbbf) -> Self {
        Self {
            expr,
            bloom_filter: Arc::new(bloom_filter),
        }
    }

    /// Check a scalar value against the bloom filter
    fn check_scalar(&self, value: &ScalarValue) -> bool {
        if value.is_null() {
            return false;
        }

        match value {
            ScalarValue::Boolean(Some(v)) => self.bloom_filter.check(v),
            ScalarValue::Int32(Some(v)) => self.bloom_filter.check(v),
            ScalarValue::Int64(Some(v)) => self.bloom_filter.check(v),
            ScalarValue::UInt32(Some(v)) => self.bloom_filter.check(v),
            ScalarValue::UInt64(Some(v)) => self.bloom_filter.check(v),
            ScalarValue::Float32(Some(v)) => self.bloom_filter.check(v),
            ScalarValue::Float64(Some(v)) => self.bloom_filter.check(v),
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                self.bloom_filter.check(v.as_str())
            }
            ScalarValue::Utf8View(Some(v)) => self.bloom_filter.check(v.as_str()),
            ScalarValue::Binary(Some(v))
            | ScalarValue::LargeBinary(Some(v))
            | ScalarValue::FixedSizeBinary(_, Some(v)) => {
                self.bloom_filter.check(v.as_slice())
            }
            ScalarValue::BinaryView(Some(v)) => self.bloom_filter.check(v.as_slice()),
            _ => true, // Unsupported types default to "might be present"
        }
    }

    /// Check an array against the bloom filter
    fn check_array(&self, array: &ArrayRef) -> Result<BooleanArray> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        let len = array.len();
        let mut builder = BooleanArray::builder(len);

        match array.data_type() {
            DataType::Boolean => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(&array.value(i)));
                    }
                }
            }
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(&array.value(i)));
                    }
                }
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(&array.value(i)));
                    }
                }
            }
            DataType::UInt32 => {
                let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(&array.value(i)));
                    }
                }
            }
            DataType::UInt64 => {
                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(&array.value(i)));
                    }
                }
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(&array.value(i)));
                    }
                }
            }
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(&array.value(i)));
                    }
                }
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(array.value(i)));
                    }
                }
            }
            DataType::LargeUtf8 => {
                let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(array.value(i)));
                    }
                }
            }
            DataType::Utf8View => {
                let array = array.as_any().downcast_ref::<StringViewArray>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(array.value(i)));
                    }
                }
            }
            DataType::Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(array.value(i)));
                    }
                }
            }
            DataType::LargeBinary => {
                let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(array.value(i)));
                    }
                }
            }
            DataType::BinaryView => {
                let array = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(array.value(i)));
                    }
                }
            }
            DataType::FixedSizeBinary(_) => {
                let array = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                for i in 0..len {
                    if array.is_null(i) {
                        builder.append_value(false);
                    } else {
                        builder.append_value(self.bloom_filter.check(array.value(i)));
                    }
                }
            }
            _ => {
                return internal_err!(
                    "Unsupported data type for bloom filter check: {}",
                    array.data_type()
                )
            }
        }

        Ok(builder.finish())
    }
}

impl fmt::Display for BloomFilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} IN BLOOM_FILTER", self.expr)
    }
}

impl PartialEq for BloomFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        // Two bloom filter expressions are equal if they have the same child expression
        // We can't compare bloom filters directly, so we use pointer equality
        self.expr.eq(&other.expr) && Arc::ptr_eq(&self.bloom_filter, &other.bloom_filter)
    }
}

impl Eq for BloomFilterExpr {}

impl Hash for BloomFilterExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        // Hash the pointer to the bloom filter
        Arc::as_ptr(&self.bloom_filter).hash(state);
    }
}

impl PhysicalExpr for BloomFilterExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        match value {
            ColumnarValue::Array(array) => {
                let result = self.check_array(&array)?;
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                let result = self.check_scalar(&scalar);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return internal_err!("BloomFilterExpr should have exactly 1 child");
        }
        Ok(Arc::new(BloomFilterExpr {
            expr: Arc::clone(&children[0]),
            bloom_filter: Arc::clone(&self.bloom_filter),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} IN BLOOM_FILTER", self.expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn test_bloom_filter_builder() -> Result<()> {
        let mut builder = BloomFilterBuilder::new(100, 0.01)?;

        // Insert some values
        builder.insert_scalar(&ScalarValue::Int32(Some(1)))?;
        builder.insert_scalar(&ScalarValue::Int32(Some(2)))?;
        builder.insert_scalar(&ScalarValue::Int32(Some(3)))?;

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let expr = col("a", &schema)?;
        let bloom_expr = builder.finish(expr);

        // Check that inserted values are found
        assert!(bloom_expr.check_scalar(&ScalarValue::Int32(Some(1))));
        assert!(bloom_expr.check_scalar(&ScalarValue::Int32(Some(2))));
        assert!(bloom_expr.check_scalar(&ScalarValue::Int32(Some(3))));

        // A value that wasn't inserted might not be found
        // (but could be a false positive, so we can't assert false)

        Ok(())
    }

    #[test]
    fn test_bloom_filter_expr_evaluation() -> Result<()> {
        use arrow::array::Int32Array;

        // Build a bloom filter with values 1, 2, 3
        let mut builder = BloomFilterBuilder::new(100, 0.01)?;
        let training_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        builder.insert_array(&training_array)?;

        // Create the expression
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let expr = col("a", &schema)?;
        let bloom_expr = Arc::new(builder.finish(expr));

        // Create a test batch with values [1, 2, 4, 5]
        let test_array = Arc::new(Int32Array::from(vec![1, 2, 4, 5])) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![test_array])?;

        // Evaluate the expression
        let result = bloom_expr.evaluate(&batch)?;
        let result_array = result.into_array(4)?;
        let result_bool = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        // Values 1 and 2 should definitely be found
        assert!(result_bool.value(0)); // 1 is in the filter
        assert!(result_bool.value(1)); // 2 is in the filter

        // Values 4 and 5 were not inserted, but might be false positives
        // We can't assert they're false without making the test flaky

        Ok(())
    }

    #[test]
    fn test_bloom_filter_with_strings() -> Result<()> {
        use arrow::array::StringArray;

        let mut builder = BloomFilterBuilder::new(100, 0.01)?;
        builder.insert_scalar(&ScalarValue::Utf8(Some("hello".to_string())))?;
        builder.insert_scalar(&ScalarValue::Utf8(Some("world".to_string())))?;

        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let expr = col("s", &schema)?;
        let bloom_expr = Arc::new(builder.finish(expr));

        let test_array =
            Arc::new(StringArray::from(vec!["hello", "world", "foo"])) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![test_array])?;

        let result = bloom_expr.evaluate(&batch)?;
        let result_array = result.into_array(3)?;
        let result_bool = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert!(result_bool.value(0)); // "hello" is in the filter
        assert!(result_bool.value(1)); // "world" is in the filter

        Ok(())
    }
}
