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

//! Hash utilities for repartitioning data

use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

use ahash::RandomState;

/// RandomState used for consistent hash partitioning
const REPARTITION_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

/// Internal hash function used for repartitioning inputs.
/// This is used for partitioned HashJoinExec and partitioned GroupByExec.
/// Currently we use AHash with fixed seeds, but this is subject to change.
/// We make no promises about stability of this function across versions.
/// Currently this is *not* stable across machines since AHash is not stable across platforms,
/// thus this should only be used in a single node context.
#[derive(Debug)]
pub(crate) struct RepartitionHash {
    signature: datafusion_expr::Signature,
    /// RandomState for consistent hashing - using the same seed as hash joins
    random_state: RandomState,
}

impl PartialEq for RepartitionHash {
    fn eq(&self, other: &Self) -> bool {
        // RandomState doesn't implement PartialEq, so we just compare signatures
        self.signature == other.signature
    }
}

impl Eq for RepartitionHash {}

impl std::hash::Hash for RepartitionHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Only hash the signature since RandomState doesn't implement Hash
        self.signature.hash(state);
    }
}

impl RepartitionHash {
    /// Create a new RepartitionHash
    pub(crate) fn new() -> Self {
        Self {
            signature: datafusion_expr::Signature::one_of(
                vec![datafusion_expr::TypeSignature::VariadicAny],
                datafusion_expr::Volatility::Immutable,
            ),
            random_state: REPARTITION_RANDOM_STATE,
        }
    }
}

impl ScalarUDFImpl for RepartitionHash {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "repartition_hash"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Always return UInt64Array regardless of input types
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        use arrow::array::{Array, UInt64Array};
        use datafusion_common::hash_utils::create_hashes;
        use std::sync::Arc;

        if args.args.is_empty() {
            return datafusion_common::plan_err!("repartition_hash requires at least one argument");
        }

        // Convert all arguments to arrays
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;

        // Check that all arrays have the same length
        let array_len = arrays[0].len();
        for (i, array) in arrays.iter().enumerate() {
            if array.len() != array_len {
                return datafusion_common::plan_err!(
                    "All input arrays must have the same length. Array 0 has length {}, but array {} has length {}",
                    array_len, i, array.len()
                );
            }
        }

        // If no rows, return an empty UInt64Array
        if array_len == 0 {
            return Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(
                Vec::<u64>::new(),
            ))));
        }

        // Create hash buffer and compute hashes using DataFusion's internal algorithm
        let mut hashes_buffer = vec![0u64; array_len];
        create_hashes(&arrays, &self.random_state, &mut hashes_buffer)?;

        // Return the hash values as a UInt64Array
        Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(
            hashes_buffer,
        ))))
    }

    fn documentation(&self) -> Option<&datafusion_expr::Documentation> {
        None
    }
}
