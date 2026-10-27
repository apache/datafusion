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

//! GROUPS frames and bounded rank peers for nested dictionary nulls (#26689).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Int8Array, Int32Array, Int64Array, ListArray,
    UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{Field, Int8Type};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;

fn nested_dict_null_batch() -> Result<RecordBatch> {
    let children: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![None, Some(0)]),
        Arc::new(Int32Array::from(vec![None])),
    )?);
    let key: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", children.data_type().clone(), true)),
        OffsetBuffer::from_lengths([1, 1]),
        children,
        None,
    ));
    Ok(RecordBatch::try_from_iter([("k", key)])?)
}

async fn ctx_with_batch() -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_batch("t", nested_dict_null_batch()?)?;
    Ok(ctx)
}

#[tokio::test]
async fn groups_current_row_counts_nested_dictionary_null_peers() -> Result<()> {
    let ctx = ctx_with_batch().await?;
    let batches = ctx
        .sql("SELECT count(*) OVER (ORDER BY k GROUPS BETWEEN CURRENT ROW AND CURRENT ROW) AS c FROM t")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count OVER returns Int64");
    assert_eq!(col.values(), &[2, 2]);
    Ok(())
}

#[tokio::test]
async fn bounded_rank_keeps_nested_dictionary_null_peers() -> Result<()> {
    let ctx = ctx_with_batch().await?;
    let batches = ctx
        .sql("SELECT rank() OVER w AS r, dense_rank() OVER w AS d FROM t WINDOW w AS (ORDER BY k)")
        .await?
        .collect()
        .await?;
    assert_eq!(batches.len(), 1);
    let rank = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let dense = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(rank.values(), &[1, 1]);
    assert_eq!(dense.values(), &[1, 1]);
    Ok(())
}
