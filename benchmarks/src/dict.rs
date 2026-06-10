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

use crate::util::BenchmarkRun;
use crate::util::CommonOpt;
use crate::util::QueryResult;
use arrow::array::{ArrayRef, DictionaryArray, Int32Array, ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::exec_err;
use datafusion_common::instant::Instant;
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;

const ITEMS_PER_VALUE: usize = 4;

#[derive(Debug, Clone, clap::Parser)]
pub struct RunOpt {
    /// Number of rows in the generated table.
    #[clap(long, default_value = "1000000")]
    pub num_rows: usize,

    /// Which query to run (1-based). Omit to run all queries.
    #[clap(long)]
    pub query: Option<usize>,

    /// Output path for the JSON benchmark summary.
    #[arg(short = 'o', long = "output")]
    pub output_path: Option<PathBuf>,

    #[clap(flatten)]
    pub common: CommonOpt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictValueType {
    Utf8,
    ListUtf8,
}
// percent of row that are null
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullPercent {
    Zero,
    Fifteen,
}

impl NullPercent {
    pub fn null_every(self) -> Option<usize> {
        match self {
            NullPercent::Zero => None,
            NullPercent::Fifteen => Some(7), // 1-in-7 ≈ 14.3 %
        }
    }
}

/// Fraction of `size` that becomes the number of distinct dictionary entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    Five,
    Ten,
    TwentyFive,
}

impl Cardinality {
    pub fn num_distinct(self, size: usize) -> usize {
        match self {
            Cardinality::Five => (size / 20).max(1),
            Cardinality::Ten => (size / 10).max(1),
            Cardinality::TwentyFive => (size / 4).max(1),
        }
    }
}

/// `generate_dict` arguments for one dictionary column.
#[derive(Debug)]
pub struct DictColParams {
    pub value_type: DictValueType,
    pub null_percent: NullPercent,
    pub cardinality: Cardinality,
}

#[derive(Debug)]
pub struct DictionaryQuery {
    pub name: &'static str,
    pub col: DictColParams,
    /// Params for a second dictionary column; `None` for single-column queries.
    pub col2: Option<DictColParams>,
    pub sql: &'static str,
}

pub const DICTIONARY_QUERIES: &[DictionaryQuery] = &[
    // single-column group-by: Utf8
    DictionaryQuery {
        name: "group_by_utf8_card5_no_nulls",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::Five,
        },
        col2: None,
        sql: r#"SELECT dict_col, COUNT(*) FROM test_data GROUP BY dict_col"#,
    },
    DictionaryQuery {
        name: "group_by_utf8_card10_no_nulls",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::Ten,
        },
        col2: None,
        sql: r#"SELECT dict_col, COUNT(*) FROM test_data GROUP BY dict_col"#,
    },
    DictionaryQuery {
        name: "group_by_utf8_card25_no_nulls",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::TwentyFive,
        },
        col2: None,
        sql: r#"SELECT dict_col, COUNT(*) FROM test_data GROUP BY dict_col"#,
    },
    DictionaryQuery {
        name: "group_by_utf8_card5_null15",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Fifteen,
            cardinality: Cardinality::Five,
        },
        col2: None,
        sql: r#"SELECT dict_col, COUNT(*) FROM test_data GROUP BY dict_col"#,
    },
    DictionaryQuery {
        name: "group_by_utf8_card25_null15",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Fifteen,
            cardinality: Cardinality::TwentyFive,
        },
        col2: None,
        sql: r#"SELECT dict_col, COUNT(*) FROM test_data GROUP BY dict_col"#,
    },
    // currently not supported by GroupValuesRows,
    // https://github.com/apache/datafusion/pull/21765 Intends to address this.
    // commenting out these benchmarks fornow
    /*
    DictionaryQuery {
        name: "group_by_list_utf8_card5_no_nulls",
        col: DictColParams {
            value_type: DictValueType::ListUtf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::Five,
        },
        col2: None,
        sql: r#"SELECT dict_col, COUNT(*) FROM test_data GROUP BY dict_col"#,
    },
    DictionaryQuery {
        name: "group_by_list_utf8_card10_no_nulls",
        col: DictColParams {
            value_type: DictValueType::ListUtf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::Ten,
        },
        col2: None,
        sql: r#"SELECT dict_col, COUNT(*) FROM test_data GROUP BY dict_col"#,
    },
    DictionaryQuery {
        name: "group_by_list_utf8_card25_null15",
        col: DictColParams {
            value_type: DictValueType::ListUtf8,
            null_percent: NullPercent::Fifteen,
            cardinality: Cardinality::TwentyFive,
        },
        col2: None,
        sql: r#"SELECT dict_col, COUNT(*) FROM test_data GROUP BY dict_col"#,
    },
    */
    DictionaryQuery {
        name: "group_by_two_utf8_card5_no_nulls",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::Five,
        },
        col2: Some(DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Fifteen,
            cardinality: Cardinality::Ten,
        }),
        sql: r#"SELECT dict_col, dict_col2, COUNT(*) FROM test_data GROUP BY dict_col, dict_col2"#,
    },
    DictionaryQuery {
        name: "group_by_two_utf8_card25_null15",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Fifteen,
            cardinality: Cardinality::TwentyFive,
        },
        col2: Some(DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::Five,
        }),
        sql: r#"SELECT dict_col, dict_col2, COUNT(*) FROM test_data GROUP BY dict_col, dict_col2"#,
    },
    // --- multi-column group-by: Utf8 + List<Utf8> ----------------------------
    /*
    DictionaryQuery {
        name: "group_by_utf8_and_list_utf8_card10_null15",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Fifteen,
            cardinality: Cardinality::Ten,
        },
        col2: Some(DictColParams {
            value_type: DictValueType::ListUtf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::Five,
        }),
        sql: r#"SELECT dict_col, dict_col2, COUNT(*) FROM test_data GROUP BY dict_col, dict_col2"#,
    },
    DictionaryQuery {
        name: "group_by_utf8_and_list_utf8_card25_no_nulls",
        col: DictColParams {
            value_type: DictValueType::Utf8,
            null_percent: NullPercent::Zero,
            cardinality: Cardinality::TwentyFive,
        },
        col2: Some(DictColParams {
            value_type: DictValueType::ListUtf8,
            null_percent: NullPercent::Fifteen,
            cardinality: Cardinality::Ten,
        }),
        sql: r#"SELECT dict_col, dict_col2, COUNT(*) FROM test_data GROUP BY dict_col, dict_col2"#,
    },
    */
];

pub fn generate_dict(
    value_type: DictValueType,
    cardinality: Cardinality,
    null_percent: NullPercent,
    size: usize,
) -> ArrayRef {
    let num_distinct = cardinality.num_distinct(size);

    let dict_values: ArrayRef = match value_type {
        DictValueType::Utf8 => {
            let strings: StringArray = (0..num_distinct)
                .map(|i| format!("value_{i}"))
                .collect::<Vec<_>>()
                .into();
            Arc::new(strings)
        }
        DictValueType::ListUtf8 => {
            let flat: StringArray = (0..num_distinct)
                .flat_map(|i| (0..ITEMS_PER_VALUE).map(move |j| format!("value_{i}_{j}")))
                .collect::<Vec<_>>()
                .into();

            let offsets: Vec<i32> = (0..=(num_distinct * ITEMS_PER_VALUE))
                .step_by(ITEMS_PER_VALUE)
                .map(|o| o as i32)
                .collect();

            Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, false)),
                OffsetBuffer::new(offsets.into()),
                Arc::new(flat),
                None,
            ))
        }
    };

    let null_every = null_percent.null_every();

    let mut key_builder = Int32Array::builder(size);
    for i in 0..size {
        if null_every.is_some_and(|n| i % n == 0) {
            key_builder.append_null();
        } else {
            key_builder.append_value((i % num_distinct) as i32);
        }
    }
    let keys = key_builder.finish();

    Arc::new(
        DictionaryArray::<Int32Type>::try_new(keys, dict_values)
            .expect("valid dictionary array"),
    )
}
impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!(
            "Running dictionary encoding benchmarks with the following options: {self:#?}\n"
        );

        let query_range = match self.query {
            Some(query_id) => {
                if query_id >= 1 && query_id <= DICTIONARY_QUERIES.len() {
                    query_id..=query_id
                } else {
                    return exec_err!(
                        "Query {query_id} not found. Available queries: 1 to {}",
                        DICTIONARY_QUERIES.len()
                    );
                }
            }
            None => 1..=DICTIONARY_QUERIES.len(),
        };

        let config = self.common.config()?;
        let rt = self.common.build_runtime()?;
        let ctx = SessionContext::new_with_config_rt(config, rt);
        let mut benchmark_run = BenchmarkRun::new();

        for query_id in query_range {
            let query = &DICTIONARY_QUERIES[query_id - 1];
            benchmark_run.start_new_case(query.name);

            let query_run = self.benchmark_query(query, &ctx).await;
            match query_run {
                Ok(query_results) => {
                    for iter in query_results {
                        benchmark_run.write_iter(iter.elapsed, iter.row_count);
                    }
                }
                Err(e) => {
                    return Err(DataFusionError::Context(
                        format!("Dictionary benchmark '{}' failed:", query.name),
                        Box::new(e),
                    ));
                }
            }
        }

        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    async fn benchmark_query(
        &self,
        query: &DictionaryQuery,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryResult>> {
        let batch = self.make_record_batch(query)?;
        ctx.deregister_table("test_data")?;
        ctx.register_batch("test_data", batch)?;

        let mut query_results = vec![];
        for i in 0..self.common.iterations {
            let start = Instant::now();
            let row_count =
                Self::execute_query_without_result_buffering(query.sql, ctx).await?;
            let elapsed = start.elapsed();
            println!(
                "Query '{}' iteration {i} returned {row_count} rows in {elapsed:?}",
                query.name
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        Ok(query_results)
    }

    fn make_record_batch(&self, query: &DictionaryQuery) -> Result<RecordBatch> {
        let size = self.num_rows;

        let col1 = generate_dict(
            query.col.value_type,
            query.col.cardinality,
            query.col.null_percent,
            size,
        );

        let (schema, columns): (Schema, Vec<ArrayRef>) = match &query.col2 {
            None => {
                let schema = Schema::new(vec![Field::new(
                    "dict_col",
                    col1.data_type().clone(),
                    true,
                )]);
                (schema, vec![col1])
            }
            Some(col2_params) => {
                let col2 = generate_dict(
                    col2_params.value_type,
                    col2_params.cardinality,
                    col2_params.null_percent,
                    size,
                );
                let schema = Schema::new(vec![
                    Field::new("dict_col", col1.data_type().clone(), true),
                    Field::new("dict_col2", col2.data_type().clone(), true),
                ]);
                (schema, vec![col1, col2])
            }
        };

        Ok(RecordBatch::try_new(Arc::new(schema), columns)?)
    }

    async fn execute_query_without_result_buffering(
        sql: &str,
        ctx: &SessionContext,
    ) -> Result<usize> {
        let mut row_count = 0;
        let df = ctx.sql(sql).await?;
        let physical_plan = df.create_physical_plan().await?;
        let mut stream = execute_stream(physical_plan, ctx.task_ctx())?;
        while let Some(batch) = stream.next().await {
            row_count += batch?.num_rows();
        }
        Ok(row_count)
    }
}
