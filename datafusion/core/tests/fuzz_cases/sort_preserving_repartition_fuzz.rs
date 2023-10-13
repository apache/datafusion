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

#[cfg(test)]
mod sp_repartition_fuzz_tests {
    use arrow::compute::concat_batches;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::SortOptions;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::{collect, ExecutionPlan, Partitioning};
    use datafusion::prelude::SessionContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::sync::Arc;
    use test_utils::add_empty_batches;

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn sort_preserving_repartition_test() {
        let seed_start = 0;
        let seed_end = 100;
        let n_row = 1000;
        // Since ordering in the test (ORDER BY a,b,c)
        // covers all the table (table consists of a,b,c columns).
        // Result doesn't depend on the stable/unstable sort
        // behaviour. We can choose, n_distinct as we like. However,
        // we chose it a large number to decrease probability of having same rows in the table.
        let n_distinct = 1_000_000;
        for (is_first_roundrobin, is_first_sort_preserving) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            for is_second_roundrobin in [false, true] {
                let mut handles = Vec::new();

                for seed in seed_start..seed_end {
                    let job = tokio::spawn(run_sort_preserving_repartition_test(
                        make_staggered_batches::<true>(n_row, n_distinct, seed as u64),
                        is_first_roundrobin,
                        is_first_sort_preserving,
                        is_second_roundrobin,
                    ));
                    handles.push(job);
                }

                for job in handles {
                    job.await.unwrap();
                }
            }
        }
    }

    /// Check whether physical plan below
    ///     "SortPreservingMergeExec: [a@0 ASC,b@1 ASC,c@2 ASC]",
    ///     "  SortPreservingRepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 2), input_partitions=2", (Partitioning can be roundrobin also)
    ///     "    SortPreservingRepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 2), input_partitions=1", (Partitioning can be roundrobin also)
    ///     "      MemoryExec: partitions=1, partition_sizes=[75]",
    /// and / or
    ///     "SortPreservingMergeExec: [a@0 ASC,b@1 ASC,c@2 ASC]",
    ///     "  SortPreservingRepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 2), input_partitions=2", (Partitioning can be roundrobin also)
    ///     "    RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 2), input_partitions=1", (Partitioning can be roundrobin also)
    ///     "      MemoryExec: partitions=1, partition_sizes=[75]",
    /// preserves ordering. Input fed to the plan above should be same with the output of the plan.
    async fn run_sort_preserving_repartition_test(
        input1: Vec<RecordBatch>,
        // If `true`, first repartition executor after `MemoryExec` will be in `RoundRobin` mode
        // else it will be in `Hash` mode
        is_first_roundrobin: bool,
        // If `true`, first repartition executor after `MemoryExec` will be `SortPreservingRepartitionExec`
        // If `false`, first repartition executor after `MemoryExec` will be `RepartitionExec` (Since its input
        // partition number is 1, `RepartitionExec` also preserves ordering.).
        is_first_sort_preserving: bool,
        // If `true`, second repartition executor after `MemoryExec` will be in `RoundRobin` mode
        // else it will be in `Hash` mode
        is_second_roundrobin: bool,
    ) {
        let schema = input1[0].schema();
        let session_config = SessionConfig::new().with_batch_size(50);
        let ctx = SessionContext::new_with_config(session_config);
        let mut sort_keys = vec![];
        for ordering_col in ["a", "b", "c"] {
            sort_keys.push(PhysicalSortExpr {
                expr: col(ordering_col, &schema).unwrap(),
                options: SortOptions::default(),
            })
        }

        let concat_input_record = concat_batches(&schema, &input1).unwrap();

        let running_source = Arc::new(
            MemoryExec::try_new(&[input1.clone()], schema.clone(), None)
                .unwrap()
                .with_sort_information(vec![sort_keys.clone()]),
        );
        let hash_exprs = vec![col("c", &schema).unwrap()];

        let intermediate = match (is_first_roundrobin, is_first_sort_preserving) {
            (true, true) => sort_preserving_repartition_exec_round_robin(running_source),
            (true, false) => repartition_exec_round_robin(running_source),
            (false, true) => {
                sort_preserving_repartition_exec_hash(running_source, hash_exprs.clone())
            }
            (false, false) => repartition_exec_hash(running_source, hash_exprs.clone()),
        };

        let intermediate = if is_second_roundrobin {
            sort_preserving_repartition_exec_round_robin(intermediate)
        } else {
            sort_preserving_repartition_exec_hash(intermediate, hash_exprs.clone())
        };

        let final_plan = sort_preserving_merge_exec(sort_keys.clone(), intermediate);
        let task_ctx = ctx.task_ctx();

        let collected_running = collect(final_plan, task_ctx.clone()).await.unwrap();
        let concat_res = concat_batches(&schema, &collected_running).unwrap();
        assert_eq!(concat_res, concat_input_record);
    }

    fn sort_preserving_repartition_exec_round_robin(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(2))
                .unwrap()
                .with_preserve_order(true),
        )
    }

    fn repartition_exec_round_robin(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(2)).unwrap(),
        )
    }

    fn sort_preserving_repartition_exec_hash(
        input: Arc<dyn ExecutionPlan>,
        hash_expr: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::Hash(hash_expr, 2))
                .unwrap()
                .with_preserve_order(true),
        )
    }

    fn repartition_exec_hash(
        input: Arc<dyn ExecutionPlan>,
        hash_expr: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::Hash(hash_expr, 2)).unwrap(),
        )
    }

    fn sort_preserving_merge_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
    }

    /// Return randomly sized record batches with:
    /// three sorted int64 columns 'a', 'b', 'c' ranged from 0..'n_distinct' as columns
    pub(crate) fn make_staggered_batches<const STREAM: bool>(
        len: usize,
        n_distinct: usize,
        random_seed: u64,
    ) -> Vec<RecordBatch> {
        // use a random number generator to pick a random sized output
        let mut rng = StdRng::seed_from_u64(random_seed);
        let mut input123: Vec<(i64, i64, i64)> = vec![(0, 0, 0); len];
        input123.iter_mut().for_each(|v| {
            *v = (
                rng.gen_range(0..n_distinct) as i64,
                rng.gen_range(0..n_distinct) as i64,
                rng.gen_range(0..n_distinct) as i64,
            )
        });
        input123.sort();
        let input1 =
            Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.0));
        let input2 =
            Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.1));
        let input3 =
            Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.2));

        // split into several record batches
        let mut remainder = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(input1) as ArrayRef),
            ("b", Arc::new(input2) as ArrayRef),
            ("c", Arc::new(input3) as ArrayRef),
        ])
        .unwrap();

        let mut batches = vec![];
        if STREAM {
            while remainder.num_rows() > 0 {
                let batch_size = rng.gen_range(0..50);
                if remainder.num_rows() < batch_size {
                    break;
                }
                batches.push(remainder.slice(0, batch_size));
                remainder =
                    remainder.slice(batch_size, remainder.num_rows() - batch_size);
            }
        } else {
            while remainder.num_rows() > 0 {
                let batch_size = rng.gen_range(0..remainder.num_rows() + 1);
                batches.push(remainder.slice(0, batch_size));
                remainder =
                    remainder.slice(batch_size, remainder.num_rows() - batch_size);
            }
        }
        add_empty_batches(batches, &mut rng)
    }
}
