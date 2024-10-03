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

    /// Test query represented by sql
    sql: Option<Arc<str>>,

    /// The queried table name
    table_name: Option<Arc<str>>,

    /// Config for the random datasets generator
    data_gen_config: Option<DatasetGeneratorConfig>,
}

impl AggregationFuzzerBuilder {
    fn new() -> Self {
        Self {
            data_gen_rounds: 50,
            ctx_gen_rounds: 50,
            sql: None,
            table_name: None,
            data_gen_config: None,
        }
    }

    #[allow(dead_code)]
    pub fn data_gen_rounds(mut self, data_gen_rounds: usize) -> Self {
        self.data_gen_rounds = data_gen_rounds;
        self
    }

    #[allow(dead_code)]
    pub fn ctx_gen_rounds(mut self, ctx_gen_rounds: usize) -> Self {
        self.ctx_gen_rounds = ctx_gen_rounds;
        self
    }

    pub fn sql(mut self, sql: &str) -> Self {
        self.sql = Some(Arc::from(sql));
        self
    }

    pub fn table_name(mut self, table_name: &str) -> Self {
        self.table_name = Some(Arc::from(table_name));
        self
    }

    pub fn data_gen_config(mut self, data_gen_config: DatasetGeneratorConfig) -> Self {
        self.data_gen_config = Some(data_gen_config);
        self
    }

    pub fn build(self) -> AggregationFuzzer {
        let sql = self.sql.expect("sql is required");
        let table_name = self.table_name.expect("table_name is required");
        let data_gen_config = self.data_gen_config.expect("data_gen_config is required");

        let dataset_generator = DatasetGenerator::new(data_gen_config);

        AggregationFuzzer {
            data_gen_rounds: self.data_gen_rounds,
            ctx_gen_rounds: self.ctx_gen_rounds,
            sql,
            table_name,
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

    sql: Arc<str>,

    table_name: Arc<str>,

    dataset_generator: DatasetGenerator,
}

impl AggregationFuzzer {
    pub async fn run(&self) {
        let mut join_set = JoinSet::new();

        // Loop to generate datasets
        for _ in 0..self.data_gen_rounds {
            let datasets = self
                .dataset_generator
                .generate()
                .expect("should success to generate dataset");
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

    async fn generate_fuzz_tasks(
        &self,
        datasets: Vec<Dataset>,
    ) -> Vec<AggregationFuzzTestTask> {
        let mut tasks = Vec::with_capacity(datasets.len() * self.ctx_gen_rounds);
        for dataset in datasets {
            let ctx_generator = SessionContextGenerator::new(dataset, &self.table_name);

            // Generate the baseline context, and get the baseline result firstly
            let baseline_ctx = ctx_generator
                .generate_baseline()
                .expect("should success to generate baseline session context");
            let baseline_result = run_sql(&self.sql, &baseline_ctx).await;
            let baseline_result = Arc::new(baseline_result);

            // Generate test tasks
            for _ in 0..self.ctx_gen_rounds {
                let ctx = ctx_generator
                    .generate()
                    .expect("should success to generate session context");
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
    query: Arc<str>,

    /// Generated session context in current test case
    ctx: SessionContext,
}

impl AggregationFuzzTestTask {
    async fn run(&self) {
        let task_result = run_sql(&self.query, &self.ctx).await;
        check_equality_of_batches(&self.expected_result, &task_result);
    }
}
