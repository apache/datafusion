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
//! Data generation: deterministic rows per seed, arranged per order and partition count.

use super::*;

/// `k1 Int64 nullable, k2 Int64 nullable, v Int64`
/// `k1, k2 Int64` (two-key query), `v Int64` (aggregated), and one column per
/// key type: `b Boolean`, `s Utf8`, `sv Utf8View`, `p Int64`, and
/// `st Struct<list: List<Int64>, num: Int64>`.
/// Every key column is nullable.
pub(super) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, true),
        Field::new("k2", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
        Field::new("b", DataType::Boolean, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("sv", DataType::Utf8View, true),
        Field::new("p", DataType::Int64, true),
        Field::new_struct("st", struct_fields(), true),
    ]))
}

/// About 3% nulls.
pub(super) fn not_null(rng: &mut StdRng) -> bool {
    rng.random_range(0..100) >= 3
}

pub(super) fn struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("list", DataType::new_list(DataType::Int64, true), true),
        Field::new("num", DataType::Int64, true),
    ])
}

/// The raw rows for one cardinality, deterministic per seed. The same multiset
/// is used for every `Order` and `Shape` so results are comparable.
///
/// Requirements:
/// - exactly `ROWS` rows
/// - `cardinality.groups()` distinct `(k1, k2)` pairs, spread so that `k1`
///   alone has fewer distinct values than `(k1, k2)`. Otherwise
///   `SortedByFirstKey` degenerates into `SortedByAllKeys`.
/// - some nulls in `k1` and `k2`
pub(super) fn generate_rows(cardinality: Cardinality, seed: u64) -> RecordBatch {
    let mut rng = StdRng::seed_from_u64(seed);
    let groups = cardinality.groups();
    // `k2` cycles through at most sqrt(groups) values, so `k1` alone has fewer
    // distinct values than the `(k1, k2)` pair.
    let k2_values = (groups as f64).sqrt().ceil().max(2.0) as i64;

    let mut k1 = Vec::with_capacity(ROWS);
    let mut k2 = Vec::with_capacity(ROWS);
    let mut v = Vec::with_capacity(ROWS);
    let mut b = Vec::with_capacity(ROWS);
    let mut s = Vec::with_capacity(ROWS);
    let mut sv = Vec::with_capacity(ROWS);
    let mut p = Vec::with_capacity(ROWS);
    let mut st_list = ListBuilder::new(Int64Builder::new());
    let mut st_num = Vec::with_capacity(ROWS);
    let mut st_valid = Vec::with_capacity(ROWS);
    for row in 0..ROWS {
        let group = (row % groups) as i64;
        k1.push(not_null(&mut rng).then_some(group / k2_values));
        k2.push(not_null(&mut rng).then_some(group % k2_values));
        v.push(rng.random_range(-1_000i64..1_000));
        // every key-type column has `groups` distinct values (boolean: two)
        b.push(not_null(&mut rng).then_some(group % 2 == 0));
        s.push(not_null(&mut rng).then(|| format!("s{group:06}")));
        sv.push(not_null(&mut rng).then(|| format!("sv{group:06}")));
        p.push(not_null(&mut rng).then_some(group));
        // struct { list: [group, group + 1], [] or null; num: group or null }
        st_valid.push(not_null(&mut rng));
        match rng.random_range(0..100) {
            0..3 => st_list.append_null(),
            3..6 => st_list.append(true),
            _ => {
                st_list.values().append_value(group);
                st_list.values().append_value(group + 1);
                st_list.append(true);
            }
        }
        st_num.push(not_null(&mut rng).then_some(group));
    }
    let st = StructArray::try_new(
        struct_fields(),
        vec![
            Arc::new(st_list.finish()),
            Arc::new(Int64Array::from(st_num)),
        ],
        Some(NullBuffer::from(st_valid)),
    )
    .unwrap();

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(k1)),
            Arc::new(Int64Array::from(k2)),
            Arc::new(Int64Array::from(v)),
            Arc::new(BooleanArray::from(b)),
            Arc::new(StringArray::from(s)),
            Arc::new(StringViewArray::from(sv)),
            Arc::new(Int64Array::from(p)),
            Arc::new(st),
        ],
    )
    .unwrap()
}

/// Arrange `rows` for the given `order` and split into `partitions` partitions
/// of `BATCH_SIZE` batches.
///
/// - `Unordered`: shuffle rows, round-robin into partitions
/// - `SortedByFirstKey`: sort by `k1` (nulls first), contiguous slice per partition
/// - `SortedByAllKeys`: sort by `k1, k2` (nulls first), contiguous slice per partition
///
/// Every partition individually satisfies the ordering.
pub(super) fn arrange(
    rows: &RecordBatch,
    keys: Keys,
    order: Order,
    partitions: usize,
) -> Vec<Vec<RecordBatch>> {
    let schema = rows.schema();
    let per_partition: Vec<RecordBatch> = match source_ordering(&schema, keys, order) {
        None => {
            let mut permutation: Vec<u32> = (0..rows.num_rows() as u32).collect();
            permutation.shuffle(&mut StdRng::seed_from_u64(0));
            let shuffled =
                take_record_batch(rows, &UInt32Array::from(permutation)).unwrap();
            (0..partitions)
                .map(|partition| {
                    let indices: UInt32Array = (partition as u32
                        ..shuffled.num_rows() as u32)
                        .step_by(partitions)
                        .collect();
                    take_record_batch(&shuffled, &indices).unwrap()
                })
                .collect()
        }
        Some(ordering) => {
            let sort_columns: Vec<SortColumn> = ordering
                .iter()
                .map(|sort_expr| SortColumn {
                    values: sort_expr
                        .expr
                        .evaluate(rows)
                        .unwrap()
                        .into_array(rows.num_rows())
                        .unwrap(),
                    options: Some(sort_expr.options),
                })
                .collect();
            let indices = lexsort_to_indices(&sort_columns, None).unwrap();
            let sorted = take_record_batch(rows, &indices).unwrap();
            let per_partition = sorted.num_rows().div_ceil(partitions);
            (0..partitions)
                .map(|partition| {
                    let start = (partition * per_partition).min(sorted.num_rows());
                    let length = per_partition.min(sorted.num_rows() - start);
                    copy_rows(&sorted, start, length)
                })
                .collect()
        }
    };

    // Copy every batch into its own buffers, as a real scan would produce.
    // A slice shares the whole partition's buffers, and operators that
    // account batches by `get_array_memory_size` (RepartitionExec, the merge)
    // would charge every 64-row batch the size of the entire partition.
    per_partition
        .iter()
        .map(|partition| {
            (0..partition.num_rows())
                .step_by(BATCH_SIZE)
                .map(|start| {
                    copy_rows(
                        partition,
                        start,
                        BATCH_SIZE.min(partition.num_rows() - start),
                    )
                })
                .collect()
        })
        .collect()
}

/// `batch[start..start + length]` in its own buffers. `take` copies where
/// `slice` shares and `concat_batches` of one batch only slices.
///
/// Take from the unsliced batch: `take` on a list sizes the new values buffer
/// as child length / list length * taken rows, so taking 64 rows out of a
/// 64-row slice of a 32k-row list allocates a values buffer for the whole
/// child, and `get_array_memory_size` reports capacity. That charged every
/// batch about 500 KB instead of 5 KB.
pub(super) fn copy_rows(batch: &RecordBatch, start: usize, length: usize) -> RecordBatch {
    let indices = UInt32Array::from_iter_values(start as u32..(start + length) as u32);
    take_record_batch(batch, &indices).unwrap()
}
