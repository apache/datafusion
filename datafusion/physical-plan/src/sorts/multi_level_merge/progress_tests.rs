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

//! Create a stream that do a multi level merge stream

//! Regression for writer-measured progress admission with more than two runs.
use super::*;
use crate::{
    metrics::{ExecutionPlanMetricsSet, SpillMetrics},
    sorts::sort::sort_batch,
};
use arrow::{
    array::{Array, ArrayRef, Int32Array, ListArray, StringArray, StringViewArray},
    buffer::OffsetBuffer,
    compute::concat_batches,
};
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_execution::{
    memory_pool::{FairSpillPool, MemoryConsumer, MemoryPool},
    runtime_env::RuntimeEnvBuilder,
};
use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};

fn batch(kind: &str, partition: usize, start: usize, rows: usize) -> RecordBatch {
    let key: ArrayRef = Arc::new(Int32Array::from_iter_values(
        (start..start + rows).map(|i| (i * 8 + partition) as i32),
    ));
    if kind == "fixed" {
        return RecordBatch::try_from_iter(vec![("key", key)]).unwrap();
    }
    let strings = (start..start + rows)
        .map(|i| format!("{i:08}{}", "x".repeat(55)))
        .collect::<Vec<_>>();
    let payload: ArrayRef = match kind {
        "struct_fixed" => Arc::new(arrow::array::StructArray::from(vec![(
            Arc::new(arrow::datatypes::Field::new(
                "value",
                arrow::datatypes::DataType::Int32,
                false,
            )),
            Arc::new(Int32Array::from_iter_values(
                (start..start + rows).map(|i| i as i32),
            )) as ArrayRef,
        )])),
        "utf8" => Arc::new(StringArray::from(strings)),
        "view" => Arc::new(StringViewArray::from(strings)),
        "list" | "list_view" => {
            let values = (0..rows * 16)
                .map(|i| format!("{:08}{}", start * 16 + i, "y".repeat(24)))
                .collect::<Vec<_>>();
            let child: ArrayRef = if kind == "list" {
                Arc::new(StringArray::from(values))
            } else {
                Arc::new(StringViewArray::from(values))
            };
            Arc::new(ListArray::new(
                Arc::new(arrow::datatypes::Field::new_list_field(
                    child.data_type().clone(),
                    false,
                )),
                OffsetBuffer::new(
                    (0..=rows)
                        .map(|i| (i * 16) as i32)
                        .collect::<Vec<_>>()
                        .into(),
                ),
                child,
                None,
            ))
        }
        _ => panic!("unknown kind"),
    };
    RecordBatch::try_from_iter(vec![("key", key), ("payload", payload)]).unwrap()
}

#[rstest::rstest]
#[case::fixed_struct("struct_fixed", 64, 2*1024*1024)]
#[case::list("list", 64, 2*1024*1024)]
#[case::view("view", 8192, 7*1024*1024)]
#[tokio::test]
async fn measured_eight_run_merge(
    #[case] kind: &str,
    #[case] rows: usize,
    #[case] limit: usize,
) -> Result<()> {
    let total = 8192;
    let partitions = 8;
    let schema = batch(kind, 0, 0, 1).schema();
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(limit));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()?;
    let metrics = ExecutionPlanMetricsSet::new();
    let manager =
        SpillManager::new(runtime, SpillMetrics::new(&metrics, 0), Arc::clone(&schema));
    let order =
        LexOrdering::new([PhysicalSortExpr::new_default(col("key", &schema)?)]).unwrap();
    let mut inputs = Vec::new();
    let mut files = Vec::new();
    let mut maximum = 0usize;
    for partition in 0..partitions {
        let batches = (0..total)
            .step_by(rows)
            .map(|start| batch(kind, partition, start, rows.min(total - start)))
            .collect::<Vec<_>>();
        let (file, memory) = manager
            .spill_record_batch_iter_with_memory(
                batches.iter().map(Ok),
                "progress input",
            )?
            .unwrap();
        let decoded = manager
            .read_spill_as_stream(Arc::clone(&file), None)?
            .try_collect::<Vec<_>>()
            .await?;
        maximum = maximum.max(
            decoded
                .iter()
                .map(get_record_batch_memory_size)
                .max()
                .unwrap(),
        );
        inputs.extend(batches);
        files.push(SpillFileWithMemory {
            file,
            max_record_batch_memory: memory.input,
            progress: memory.progress,
        });
    }
    // Same deliberately conservative read budget as the independent main/PR
    // comparison fixture; measured W/P still comes only from actual writes.
    let recorded_max = maximum.checked_next_power_of_two().unwrap().max(
        files
            .iter()
            .map(|file| file.max_record_batch_memory)
            .max()
            .unwrap(),
    );
    for file in &mut files {
        assert!(file.max_record_batch_memory <= recorded_max);
        file.max_record_batch_memory = recorded_max;
        assert!(file.progress.is_some());
    }
    let expected = sort_batch(&concat_batches(&schema, &inputs)?, &order, None)?;
    drop(inputs);
    let reservation = MemoryConsumer::new("progress merge").register(&pool);
    let mut builder = MultiLevelMergeBuilder::new_with_memory(
        manager,
        Arc::clone(&schema),
        files,
        vec![],
        order.clone(),
        BaselineMetrics::new(&metrics, 0),
        1024,
        reservation,
        None,
        true,
    );
    // Admission must not impose an unconditional two-run cap. At minimum
    // read-ahead is reduced before deciding how many runs can be seated.
    let mut probe = builder.reservation.new_empty();
    let selected = builder.get_sorted_spill_files_to_merge(2, 2, &mut probe, false)?;
    let SpillFilesToMerge::Ready(selected, _, _) = selected else {
        panic!("these rows fit")
    };
    assert!(
        selected.len() > 2,
        "budget-dependent fan-in must exceed two"
    );
    builder.sorted_spill_files.splice(0..0, selected);
    drop(probe);
    let started = datafusion_common::instant::Instant::now();
    let output = builder
        .create_spillable_merge_stream()
        .try_collect::<Vec<_>>()
        .await?;
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(concat_batches(&schema, &output)?, expected);
    assert_eq!(pool.reserved(), 0);
    eprintln!(
        "{}",
        serde_json::json!({"kind":kind,"input_rows":rows,"partitions":partitions,"pool":limit,"recorded_max":recorded_max,"actual_readback":maximum,"output_batches":output.len(),"elapsed_ns":elapsed,"spills":metrics.clone_inner().aggregate_by_name().spill_count(),"released":pool.reserved()})
    );
    Ok(())
}

#[rstest::rstest]
#[case::read(false)]
#[case::workspace(true)]
#[test]
fn admission_rejects_overflow_before_allocating(
    #[case] workspace_overflow: bool,
) -> Result<()> {
    let input = batch("struct_fixed", 0, 0, 1);
    let schema = input.schema();
    let manager = SpillManager::new(
        RuntimeEnvBuilder::new().build_arc()?,
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        Arc::clone(&schema),
    );
    let (file, memory) = manager
        .spill_record_batch_iter_with_memory(
            std::iter::once(Ok(input)),
            "overflow input",
        )?
        .unwrap();
    let mut progress = memory.progress.unwrap();
    if workspace_overflow {
        progress.workspace = usize::MAX;
    }
    let measured = SpillFileWithMemory {
        file,
        max_record_batch_memory: if workspace_overflow {
            memory.input
        } else {
            usize::MAX
        },
        progress: Some(progress),
    };
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1024 * 1024));
    let order =
        LexOrdering::new([PhysicalSortExpr::new_default(col("key", &schema)?)]).unwrap();
    let mut builder = MultiLevelMergeBuilder::new_with_memory(
        manager,
        schema,
        vec![measured],
        vec![],
        order,
        BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        1024,
        MemoryConsumer::new("overflow").register(&pool),
        None,
        true,
    );
    let mut grant = builder.reservation.new_empty();
    assert!(matches!(
        builder.get_sorted_spill_files_to_merge(2, 1, &mut grant, false),
        Err(datafusion_common::DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
