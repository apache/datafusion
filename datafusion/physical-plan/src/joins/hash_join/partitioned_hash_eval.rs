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

//! Hash computation and hash table lookup expressions for dynamic filtering

use std::{any::Any, fmt::Display, hash::Hash, sync::Arc};

use ahash::RandomState;
use arrow::{
    array::UInt64Array,
    datatypes::{DataType, Schema},
};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::{
    DynHash, PhysicalExpr, PhysicalExprRef,
};

use crate::hash_utils::create_hashes;

/// Physical expression that computes hash values for a set of columns
///
/// This expression computes the hash of join key columns using a specific RandomState.
/// It returns a UInt64Array containing the hash values.
///
/// This is used for:
/// - Computing routing hashes (with RepartitionExec's 0,0,0,0 seeds)
/// - Computing lookup hashes (with HashJoin's 'J','O','I','N' seeds)
pub(super) struct HashExpr {
    /// Columns to hash
    on_columns: Vec<PhysicalExprRef>,
    /// Random state for hashing
    random_state: RandomState,
    /// Description for display
    description: String,
}

impl HashExpr {
    /// Create a new HashExpr
    ///
    /// # Arguments
    /// * `on_columns` - Columns to hash
    /// * `random_state` - RandomState for hashing
    /// * `description` - Description for debugging (e.g., "hash_repartition", "hash_join")
    pub(super) fn new(
        on_columns: Vec<PhysicalExprRef>,
        random_state: RandomState,
        description: String,
    ) -> Self {
        Self {
            on_columns,
            random_state,
            description,
        }
    }
}

impl std::fmt::Debug for HashExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .on_columns
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}({})", self.description, cols)
    }
}

impl Hash for HashExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.on_columns.dyn_hash(state);
        self.description.hash(state);
    }
}

impl PartialEq for HashExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_columns == other.on_columns && self.description == other.description
    }
}

impl Eq for HashExpr {}

impl Display for HashExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl PhysicalExpr for HashExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.on_columns.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(HashExpr::new(
            children,
            self.random_state.clone(),
            self.description.clone(),
        )))
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();

        // Evaluate columns
        let keys_values = self
            .on_columns
            .iter()
            .map(|c| c.evaluate(batch)?.into_array(num_rows))
            .collect::<Result<Vec<_>>>()?;

        // Compute hashes
        let mut hashes_buffer = vec![0; num_rows];
        create_hashes(&keys_values, &self.random_state, &mut hashes_buffer)?;

        Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(
            hashes_buffer,
        ))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}
