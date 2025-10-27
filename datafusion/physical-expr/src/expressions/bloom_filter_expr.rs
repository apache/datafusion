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
use ahash::RandomState;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{internal_err, Result, ScalarValue};
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
/// let random_state = RandomState::with_seeds(0, 0, 0, 0);
/// let mut builder = BloomFilterBuilder::new(1000, 0.01, random_state)?;
/// builder.insert_hashes(&hashes)?;
/// let expr = builder.build(col_expr);  // Consumes builder
/// ```
#[derive(Debug)]
pub struct BloomFilterBuilder {
    /// The underlying bloom filter
    sbbf: Sbbf,
    /// Random state for consistent hashing
    random_state: RandomState,
}

impl BloomFilterBuilder {
    /// Create a new bloom filter builder
    ///
    /// # Arguments
    /// * `ndv` - Expected number of distinct values
    /// * `fpp` - Desired false positive probability (0.0 to 1.0)
    /// * `random_state` - Random state for consistent hashing across build and probe phases
    pub fn new(ndv: u64, fpp: f64, random_state: RandomState) -> Result<Self> {
        let sbbf = Sbbf::new_with_ndv_fpp(ndv, fpp)?;
        Ok(Self { sbbf, random_state })
    }

    /// Insert pre-computed hash values into the bloom filter
    ///
    /// This method allows reusing hash values that were already computed
    /// for other purposes (e.g., hash table insertion), avoiding redundant
    /// hash computation.
    ///
    /// # Arguments
    /// * `hashes` - Pre-computed hash values to insert
    pub fn insert_hashes(&mut self, hashes: &[u64]) {
        for &hash in hashes {
            self.sbbf.insert_hash(hash);
        }
    }

    /// Build a `BloomFilterExpr` from this builder, consuming the builder.
    ///
    /// This consumes the builder and moves the bloom filter data into the expression,
    /// avoiding any clones of the (potentially large) bloom filter.
    ///
    /// # Arguments
    /// * `expr` - The expression to evaluate and check against the bloom filter
    pub fn build(self, expr: Arc<dyn PhysicalExpr>) -> BloomFilterExpr {
        BloomFilterExpr::new(expr, self.sbbf, self.random_state)
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
    /// Random state for consistent hashing
    random_state: RandomState,
}

impl BloomFilterExpr {
    /// Create a new bloom filter expression (internal use only)
    ///
    /// Users should create bloom filter expressions through `BloomFilterBuilder::build()`
    pub(crate) fn new(
        expr: Arc<dyn PhysicalExpr>,
        bloom_filter: Sbbf,
        random_state: RandomState,
    ) -> Self {
        Self {
            expr,
            bloom_filter: Arc::new(bloom_filter),
            random_state,
        }
    }

    /// Check a scalar value against the bloom filter
    fn check_scalar(&self, value: &ScalarValue) -> Result<bool> {
        let array = value.to_array()?;
        let result = self.check_array(&array)?;
        // Since the array has length 1, return the first value
        #[cfg(debug_assertions)]
        {
            assert_eq!(result.len(), 1);
        }
        Ok(result.value(0))
    }

    /// Check an array against the bloom filter
    fn check_array(&self, array: &ArrayRef) -> Result<BooleanArray> {
        // Use create_hashes to compute hash values for all array types
        // This handles Dictionary, Struct, Null, and all other types uniformly
        let mut hashes = vec![0u64; array.len()];
        create_hashes(&[Arc::clone(array)], &self.random_state, &mut hashes)?;

        // Check each hash against the bloom filter
        let mut builder = BooleanArray::builder(array.len());
        for hash in hashes {
            builder.append_value(self.bloom_filter.check_hash(hash));
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
                let result = self.check_scalar(&scalar)?;
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
            random_state: self.random_state.clone(),
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

    // Helper trait to add insert_scalar for tests
    trait BloomFilterBuilderTestExt {
        fn insert_scalar(&mut self, value: &ScalarValue) -> Result<()>;
    }

    impl BloomFilterBuilderTestExt for BloomFilterBuilder {
        /// Insert a single scalar value by converting to array and computing hashes
        /// This is less efficient but sufficient for tests
        fn insert_scalar(&mut self, value: &ScalarValue) -> Result<()> {
            let array = value.to_array()?;
            let mut hashes = vec![0u64; array.len()];
            create_hashes(&[array], &self.random_state, &mut hashes)?;
            self.insert_hashes(&hashes);
            Ok(())
        }
    }

    #[test]
    fn test_bloom_filter_builder() -> Result<()> {
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut builder = BloomFilterBuilder::new(100, 0.01, random_state)?;

        // Insert some values
        builder.insert_scalar(&ScalarValue::Int32(Some(1)))?;
        builder.insert_scalar(&ScalarValue::Int32(Some(2)))?;
        builder.insert_scalar(&ScalarValue::Int32(Some(3)))?;

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let expr = col("a", &schema)?;
        let bloom_expr = builder.build(expr);

        // Check that inserted values are found
        assert!(bloom_expr.check_scalar(&ScalarValue::Int32(Some(1)))?);
        assert!(bloom_expr.check_scalar(&ScalarValue::Int32(Some(2)))?);
        assert!(bloom_expr.check_scalar(&ScalarValue::Int32(Some(3)))?);

        // A value that wasn't inserted might not be found
        // (but could be a false positive, so we can't assert false)

        Ok(())
    }

    #[test]
    fn test_bloom_filter_expr_evaluation() -> Result<()> {
        use arrow::array::Int32Array;

        // Build a bloom filter with values 1, 2, 3
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut builder = BloomFilterBuilder::new(100, 0.01, random_state)?;
        let training_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut hashes = vec![0u64; training_array.len()];
        create_hashes(
            &[Arc::clone(&training_array)],
            &builder.random_state,
            &mut hashes,
        )?;
        builder.insert_hashes(&hashes);

        // Create the expression
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let expr = col("a", &schema)?;
        let bloom_expr = Arc::new(builder.build(expr));

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

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut builder = BloomFilterBuilder::new(100, 0.01, random_state)?;
        builder.insert_scalar(&ScalarValue::Utf8(Some("hello".to_string())))?;
        builder.insert_scalar(&ScalarValue::Utf8(Some("world".to_string())))?;

        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let expr = col("s", &schema)?;
        let bloom_expr = Arc::new(builder.build(expr));

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

    #[test]
    fn test_bloom_filter_with_decimals() -> Result<()> {
        use arrow::array::Decimal128Array;

        // Build a bloom filter with decimal values
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut builder = BloomFilterBuilder::new(100, 0.01, random_state)?;
        builder.insert_scalar(&ScalarValue::Decimal128(Some(12345), 10, 2))?;
        builder.insert_scalar(&ScalarValue::Decimal128(Some(67890), 10, 2))?;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let expr = col("d", &schema)?;
        let bloom_expr = Arc::new(builder.build(expr));

        // Create test array with decimal values
        let test_array = Arc::new(
            Decimal128Array::from(vec![12345, 67890, 11111])
                .with_precision_and_scale(10, 2)?,
        ) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![test_array])?;

        // Evaluate the expression
        let result = bloom_expr.evaluate(&batch)?;
        let result_array = result.into_array(3)?;
        let result_bool = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        // Values that were inserted should be found
        assert!(result_bool.value(0)); // 12345 is in the filter
        assert!(result_bool.value(1)); // 67890 is in the filter

        // Value 11111 was not inserted, but might be a false positive
        // We can't assert it's false without making the test flaky

        Ok(())
    }

    #[test]
    fn test_bloom_filter_negative_lookups() -> Result<()> {
        use arrow::array::{Float64Array, Int32Array, StringArray};

        // Test Int32: Use extremely low FPP (0.00001) to make false positives negligible
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut builder = BloomFilterBuilder::new(10, 0.00001, random_state.clone())?;
        for i in 1..=10 {
            builder.insert_scalar(&ScalarValue::Int32(Some(i)))?;
        }
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let expr = col("a", &schema)?;
        let bloom_expr = Arc::new(builder.build(expr));

        // Test values far outside the inserted range (1000-1099)
        let test_values: Vec<i32> = (1000..1100).collect();
        let test_array = Arc::new(Int32Array::from(test_values)) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![test_array])?;
        let result = bloom_expr.evaluate(&batch)?;
        let result_array = result.into_array(100)?;
        let result_bool = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..100 {
            assert!(
                !result_bool.value(i),
                "Int32 value {} should not match",
                i + 1000
            );
        }

        // Test Float64
        let mut builder = BloomFilterBuilder::new(10, 0.00001, random_state.clone())?;
        for i in 0..10 {
            builder.insert_scalar(&ScalarValue::Float64(Some(i as f64 * 0.5)))?;
        }
        let schema =
            Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)]));
        let expr = col("f", &schema)?;
        let bloom_expr = Arc::new(builder.build(expr));

        let test_values: Vec<f64> = (100..200).map(|i| i as f64 * 10.0).collect();
        let test_array = Arc::new(Float64Array::from(test_values)) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![test_array])?;
        let result = bloom_expr.evaluate(&batch)?;
        let result_array = result.into_array(100)?;
        let result_bool = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..100 {
            assert!(
                !result_bool.value(i),
                "Float64 value {} should not match",
                (i + 100) as f64 * 10.0
            );
        }

        // Test Strings
        let mut builder = BloomFilterBuilder::new(5, 0.00001, random_state)?;
        builder.insert_scalar(&ScalarValue::Utf8(Some("apple".to_string())))?;
        builder.insert_scalar(&ScalarValue::Utf8(Some("banana".to_string())))?;
        builder.insert_scalar(&ScalarValue::Utf8(Some("cherry".to_string())))?;
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let expr = col("s", &schema)?;
        let bloom_expr = Arc::new(builder.build(expr));

        let test_strings = vec![
            "zebra",
            "yak",
            "xylophone",
            "walrus",
            "vulture",
            "umbrella",
            "tiger",
            "snake",
        ];
        let test_array = Arc::new(StringArray::from(test_strings.clone())) as ArrayRef;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![test_array])?;
        let result = bloom_expr.evaluate(&batch)?;
        let result_array = result.into_array(8)?;
        let result_bool = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for (i, s) in test_strings.iter().enumerate() {
            assert!(!result_bool.value(i), "String '{s}' should not match");
        }

        Ok(())
    }
}
