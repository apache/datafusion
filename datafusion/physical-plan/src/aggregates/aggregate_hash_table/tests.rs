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

use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, StringArray, StringDictionaryBuilder};
use arrow::datatypes::{DataType, Field, Schema, UInt8Type, UInt16Type};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_physical_expr::expressions::col;

use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::test::TestMemoryExec;

use super::{AggregateHashTable, FinalMarker, PartialMarker};

#[test]
fn dictionary_groups_keep_partial_schema_and_promote_final_output() -> Result<()> {
    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "group_col",
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        false,
    )]));
    let input_batches = (0u32..257)
        .map(|value| -> Result<RecordBatch> {
            let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
            builder.append_value(format!("group_{value}"));
            let array: ArrayRef = Arc::new(builder.finish());
            Ok(RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![array],
            )?)
        })
        .collect::<Result<Vec<_>>>()?;

    let input = TestMemoryExec::try_new(
        &[input_batches.clone()],
        Arc::clone(&input_schema),
        None,
    )?;
    let input = Arc::new(TestMemoryExec::update_cache(&Arc::new(input)));
    let group_by = PhysicalGroupBy::new_single(vec![(
        col("group_col", &input_schema)?,
        "group_col".to_string(),
    )]);
    let partial_exec = AggregateExec::try_new(
        AggregateMode::Partial,
        group_by.clone(),
        vec![],
        vec![],
        input,
        Arc::clone(&input_schema),
    )?;
    let partial_schema = Arc::clone(&partial_exec.schema);
    let mut partial_table = AggregateHashTable::<PartialMarker>::new(
        &partial_exec,
        0,
        Arc::clone(&partial_schema),
        1024,
    )?;
    for batch in &input_batches {
        partial_table.aggregate_batch(batch)?;
    }
    partial_table.start_output()?;

    let mut partial_batches = vec![];
    while let Some(batch) = partial_table.next_output_batch()? {
        assert_eq!(batch.schema(), partial_schema);
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8))
        );
        assert!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt8Type>>()
                .is_some()
        );
        partial_batches.push(batch);
    }
    assert_eq!(
        partial_batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![256, 1]
    );

    let partial_input = TestMemoryExec::try_new(
        &[partial_batches.clone()],
        Arc::clone(&partial_schema),
        None,
    )?;
    let partial_input = Arc::new(TestMemoryExec::update_cache(&Arc::new(partial_input)));
    let final_exec = AggregateExec::try_new(
        AggregateMode::Final,
        group_by.as_final(),
        vec![],
        vec![],
        partial_input,
        Arc::clone(&input_schema),
    )?;
    let mut final_table = AggregateHashTable::<FinalMarker>::new(
        &final_exec,
        0,
        Arc::clone(&final_exec.schema),
        1024,
    )?;
    for batch in &partial_batches {
        final_table.aggregate_batch(batch)?;
    }
    final_table.start_output()?;

    let batch = final_table.next_output_batch()?.unwrap();
    assert_eq!(batch.num_rows(), 257);
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
    );
    let dictionary = batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()
        .unwrap();
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..257 {
        let key = dictionary.keys().value(row) as usize;
        assert_eq!(values.value(key), format!("group_{row}"));
    }
    assert!(final_table.next_output_batch()?.is_none());

    Ok(())
}
