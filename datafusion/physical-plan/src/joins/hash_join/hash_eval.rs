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

use ahash::RandomState;
use datafusion_physical_expr::expressions::Column;
use std::{any::Any, fmt::Display, hash::Hash, sync::Arc};

use arrow::{
    array::BooleanArray,
    buffer::MutableBuffer,
    datatypes::{DataType, Schema},
    util::bit_util,
};
use datafusion_common::{
    hash_utils::create_hashes, internal_datafusion_err, NullEquality, Result,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::{
    DynHash, PhysicalExpr, PhysicalExprRef,
};

use crate::joins::utils::NoHashSet;

/// Private [`PhysicalExpr`] used during row filtering of probe side batches
pub(super) struct HashEvalPhysicalExpr {
    /// Equi-join columns from the right (probe side)
    on_right: Vec<PhysicalExprRef>,
    hashes: NoHashSet<u64>,
    null_equality: NullEquality,
    random_state: RandomState,
}

impl HashEvalPhysicalExpr {
    pub fn new(
        on_right: Vec<PhysicalExprRef>,
        hashes: NoHashSet<u64>,
        null_equality: NullEquality,
        random_state: RandomState,
    ) -> Self {
        Self {
            on_right,
            hashes,
            null_equality,
            random_state,
        }
    }
}

impl std::fmt::Debug for HashEvalPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .on_right
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "HashEvalPhysicalExpr [ {cols} ]")
    }
}

impl Hash for HashEvalPhysicalExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.on_right.dyn_hash(state);
        self.null_equality.hash(state);
    }
}

impl PartialEq for HashEvalPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_right == other.on_right
            && self.null_equality == other.null_equality
            && self.hashes == other.hashes
    }
}

impl Eq for HashEvalPhysicalExpr {}

impl Display for HashEvalPhysicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HashEvalPhysicalExpr")
    }
}

impl PhysicalExpr for HashEvalPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();

        let keys_values = self
            .on_right
            .iter()
            .map(|c| {
                let col = c
                    .as_any()
                    .downcast_ref::<Column>()
                    .map(|col| {
                        Column::new_with_schema(col.name(), batch.schema().as_ref())
                    })
                    .ok_or(internal_datafusion_err!("expected join column"))??;

                col.evaluate(&batch)?.into_array(num_rows)
            })
            .collect::<Result<Vec<_>>>()?;

        let mut hashes_buffer = vec![0; num_rows];
        create_hashes(&keys_values, &self.random_state, &mut hashes_buffer)?;

        let mut buf = MutableBuffer::from_len_zeroed(bit_util::ceil(num_rows, 8));
        for (idx, hash) in hashes_buffer.into_iter().enumerate() {
            if self.hashes.contains(&hash) {
                bit_util::set_bit(buf.as_slice_mut(), idx);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(
            BooleanArray::new_from_packed(buf, 0, num_rows),
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HashEvalPhysicalExpr")
    }
}
