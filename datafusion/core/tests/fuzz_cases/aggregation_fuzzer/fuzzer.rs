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

use arrow_array::RecordBatch;
use datafusion::prelude::SessionContext;
use tokio::task::JoinSet;

use crate::fuzz_cases::aggregation_fuzzer::{
    check_equality_of_batches,
    context_generator::SessionContextGenerator,
    data_generator::{Dataset, DatasetGenerator, DatasetGeneratorConfig},
    run_sql,
};

pub struct AggregationFuzzerBuilder {
    /// Rounds to call `generate` of `DataSetsGenerator`
    /// `len(sort_keys_set) + 1` datasets will generated.
    data_gen_rounds: usize,

    /// Rounds to call `generate` of `SessionContextGenerator`
    /// `ctx_gen_rounds` datasets will generated.
    ctx_gen_rounds: usize,

    sql: Option<Arc<String>>,

    data_gen_config: Option<DatasetGeneratorConfig>,
}

impl AggregationFuzzerBuilder {
    fn new() -> Self {
        Self {
            data_gen_rounds: 300,
            ctx_gen_rounds: 50,
            sql: None,
            data_gen_config: None,
        }
    }

    pub fn data_gen_rounds(mut self, data_gen_rounds: usize) -> Self {
        self.data_gen_rounds = data_gen_rounds;
        self
    }

    pub fn ctx_gen_rounds(mut self, ctx_gen_rounds: usize) -> Self {
        self.ctx_gen_rounds = ctx_gen_rounds;
        self
    }

    pub fn sql(mut self, sql: &str) -> Self {
        self.sql = Some(Arc::new(sql.to_string()));
        self
    }

    pub fn data_gen_config(mut self, data_gen_config: DatasetGeneratorConfig) -> Self {
        self.data_gen_config = Some(data_gen_config);
        self
    }

    pub fn build(self) -> AggregationFuzzer {
        let sql = self.sql.expect("sql is required");
        let data_gen_config = self.data_gen_config.expect("data_gen_config is required");

        let dataset_generator = DatasetGenerator::new(data_gen_config);

        AggregationFuzzer {
            data_gen_rounds: self.data_gen_rounds,
            ctx_gen_rounds: self.ctx_gen_rounds,
            sql,
            dataset_generator,
        }
    }
}

impl Default for AggregationFuzzerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct AggregationFuzzer {
    /// Rounds to call `generate` of `DataSetsGenerator`
    /// `len(sort_keys_set) + 1` datasets will generated.
    data_gen_rounds: usize,

    /// Rounds to call `generate` of `SessionContextGenerator`
    /// `ctx_gen_rounds` datasets will generated.
    ctx_gen_rounds: usize,

    sql: Arc<String>,

    dataset_generator: DatasetGenerator,
}

impl AggregationFuzzer {
    pub async fn run(&self) {
        let mut join_set = JoinSet::new();

        // Loop to generate datasets
        for _ in 0..self.data_gen_rounds {
            let datasets = self.dataset_generator.generate();
            let tasks = self.generate_fuzz_tasks(datasets).await;
            for task in tasks {
                join_set.spawn(async move {
                    task.run().await;
                });
            }
        }

        while let Some(join_handle) = join_set.join_next().await {
            // propagate errors
            join_handle.unwrap();
        }
    }

    pub async fn generate_fuzz_tasks(
        &self,
        datasets: Vec<Dataset>,
    ) -> Vec<AggregationFuzzTestTask> {
        let mut tasks = Vec::with_capacity(datasets.len() * self.ctx_gen_rounds);
        for dataset in datasets {
            let ctx_generator = SessionContextGenerator::new(dataset);

            // Generate the baseline context, and get the baseline result firstly
            let baseline_ctx = ctx_generator.generate_baseline();
            let baseline_result = run_sql(&self.sql, &baseline_ctx).await;
            let baseline_result = Arc::new(baseline_result);

            // Generate test tasks
            for _ in 0..self.ctx_gen_rounds {
                let ctx = ctx_generator.generate();
                let task = AggregationFuzzTestTask {
                    expected_result: baseline_result.clone(),
                    query: self.sql.clone(),
                    ctx,
                };

                tasks.push(task);
            }
        }

        tasks
    }
}

struct AggregationFuzzTestTask {
    /// Expected result in current test case
    /// It is generate from `query` + `baseline session context`
    expected_result: Arc<Vec<RecordBatch>>,

    /// The test query
    /// Use sql to represent it currently.
    query: Arc<String>,

    /// Generated session context in current test case
    ctx: SessionContext,
}

impl AggregationFuzzTestTask {
    async fn run(&self) {
        let task_result = run_sql(&self.query, &self.ctx).await;
        check_equality_of_batches(&self.expected_result, &task_result);
    }
}
