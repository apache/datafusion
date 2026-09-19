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

//! Ordinary byte arrays preserve admitted memory and independent output ownership.

use super::*;
use arrow::array::{FixedSizeBinaryArray, StringArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};

fn utf8_array(offsets: &[i32], values: Buffer, nulls: Option<NullBuffer>) -> ArrayRef {
    Arc::new(StringArray::new(
        OffsetBuffer::new(offsets.to_vec().into()),
        values,
        nulls,
    ))
}

/// Return nullable key/payload columns sharing the same byte array.
fn aliased_byte_batch(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", array.data_type().clone(), true),
            Field::new("payload", array.data_type().clone(), true),
        ])),
        vec![Arc::clone(&array), array],
    )
    .unwrap()
}

// Output order is unspecified; compare all logical columns after releasing the build.
fn byte_rows(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        let array = array.as_string::<i32>();
                        (!array.is_null(row)).then(|| array.value(row).to_owned())
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// Both retained slices and concatenated inputs produce independently owned output,
/// including nulls with hidden bytes and duplicate keys.
#[tokio::test]
async fn prepared_plain_bytes_preserve_slices_nulls_and_output_ownership() -> Result<()> {
    const HIDDEN: usize = 4096;
    let mut values = b"prefixb".to_vec();
    values.extend(std::iter::repeat_n(b'h', HIDDEN));
    values.extend_from_slice(b"aasuffix");
    let array = utf8_array(
        &[
            0,
            6,
            7,
            7 + HIDDEN as i32,
            8 + HIDDEN as i32,
            9 + HIDDEN as i32,
            15 + HIDDEN as i32,
        ],
        Buffer::from_vec(values),
        Some(NullBuffer::from(vec![true, true, false, true, true, true])),
    );
    let build = aliased_byte_batch(array.slice(1, 4));
    let probe = aliased_byte_batch(Arc::new(StringArray::from(vec![
        Some("a"),
        Some("b"),
        None,
    ])));
    let base = join(build.schema(), probe)?
        .builder()
        .with_null_equality(NullEquality::NullEqualsNull)
        .build()?;
    for sources in [
        vec![build.clone()],
        vec![build.slice(0, 2), build.slice(2, 2)],
    ] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let prepared = prepare(&base, sources, Arc::clone(&pool)).await?;
        let addresses = buffer_addresses(prepared.build.batch.columns());
        let task = base
            .builder()
            .with_prepared_build(Arc::clone(&prepared))
            .build()?;
        let output = run(&task).await?;
        assert!(
            output
                .iter()
                .flat_map(|batch| buffer_addresses(batch.columns()))
                .all(|address| !addresses.contains(&address))
        );
        drop(task);
        drop(prepared);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(
            byte_rows(&output),
            vec![
                vec![None; 4],
                vec![Some("a".to_owned()); 4],
                vec![Some("a".to_owned()); 4],
                vec![Some("b".to_owned()); 4],
            ]
        );
    }
    Ok(())
}

/// Aliases share retained charges but require separate concat copies. Reject one
/// byte below the copy peak without leaking the reservation.
#[tokio::test]
async fn prepared_plain_bytes_copy_admission_counts_aliases_and_releases_errors()
-> Result<()> {
    const HIDDEN: usize = 8192;
    let mut values = vec![b'h'; HIDDEN];
    values.push(b'a');
    let build = aliased_byte_batch(utf8_array(
        &[0, HIDDEN as i32, HIDDEN as i32 + 1],
        Buffer::from_vec(values),
        Some(NullBuffer::from(vec![false, true])),
    ));
    let single = build.project(&[0])?;
    assert_eq!(
        get_record_batch_memory_size(&build),
        get_record_batch_memory_size(&single)
    );
    let copy = prepared_copy_bytes(&build)?;
    assert_eq!(copy, 2 * prepared_copy_bytes(&single)?);
    assert!(copy >= 2 * HIDDEN);
    let retained = get_record_batch_memory_size(&build);
    let base = join(build.schema(), build.slice(1, 1))?;
    assert_admission_denied(
        &base,
        vec![build.clone(), build.clone()],
        retained + 2 * copy - 1,
    )
    .await;
    let funded: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let prepared =
        prepare(&base, vec![build.clone(), build], Arc::clone(&funded)).await?;
    assert!(
        prepared.reserved_bytes() >= get_record_batch_memory_size(&prepared.build.batch)
    );
    assert_eq!(funded.reserved(), prepared.reserved_bytes());
    drop(prepared);
    assert_eq!(funded.reserved(), 0);
    Ok(())
}

/// Both byte-key encodings use the charged hash table instead of copied IN-list
/// literals. Their membership filter still completes and filters the probe.
#[tokio::test]
async fn prepared_plain_bytes_keys_keep_hash_filter_without_scalar_copies() -> Result<()>
{
    const KEY_BYTES: usize = 256 * 1024;
    let mut values = vec![b'k'; KEY_BYTES];
    values.extend(std::iter::repeat_n(b'x', KEY_BYTES));
    let values = Buffer::from_vec(values);
    for array in [
        utf8_array(
            &[0, KEY_BYTES as i32, (KEY_BYTES * 2) as i32],
            values.clone(),
            None,
        ),
        Arc::new(FixedSizeBinaryArray::try_new(
            KEY_BYTES as i32,
            values,
            None,
        )?),
    ] {
        let build = aliased_byte_batch(array.slice(0, 1));
        let probe = aliased_byte_batch(array);
        let base = join(build.schema(), probe)?;
        let mut options = ConfigOptions::default();
        options.optimizer.hash_join_inlist_pushdown_max_size = 4 << 20;
        options
            .optimizer
            .hash_join_inlist_pushdown_max_distinct_values = 100;
        let config = Arc::new(options);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 << 20));
        let prepared = base
            .prepare_build(
                input(vec![build.clone()], build.schema()),
                Arc::clone(&pool),
                Arc::clone(&config),
            )
            .await?;
        assert!(prepared.build.bounds.is_none());
        assert!(matches!(
            &prepared.build.membership,
            PushdownStrategy::Map(_)
        ));
        assert_eq!(config.optimizer.hash_join_inlist_pushdown_max_size, 4 << 20);
        let task = with_probe_filter(base)?
            .builder()
            .with_prepared_build(Arc::clone(&prepared))
            .build()?;
        let filter = Arc::clone(&task.dynamic_filter.as_ref().unwrap().filter);
        let output = run(&task).await?;
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let row = output.iter().find(|batch| batch.num_rows() != 0).unwrap();
        for array in row.columns() {
            assert_eq!(array.to_data(), build.column(0).to_data());
        }
        assert!(futures::poll!(Box::pin(filter.wait_complete())).is_ready());
        drop(filter);
        drop(task);
        drop(prepared);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}
