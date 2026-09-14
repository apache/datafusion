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
use clap::ValueEnum;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
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

    /// How string group keys are represented at the aggregate input.
    #[clap(long, value_enum, default_value_t = InputEncoding::Dictionary)]
    pub input_encoding: InputEncoding,

    /// Override the generated number of distinct values.
    #[clap(long)]
    pub num_distinct: Option<usize>,

    /// Split the generated input into batches of this many rows.
    #[clap(long)]
    pub input_batch_size: Option<usize>,

    /// Pad generated strings to at least this many bytes.
    #[clap(long)]
    pub value_length: Option<usize>,

    /// Whether dictionary values are shared by all input batches.
    #[clap(long, value_enum, default_value_t = DictionaryValueReuse::Shared)]
    pub dictionary_value_reuse: DictionaryValueReuse,

    /// Cast dictionary group keys back to strings in the query output.
    #[clap(long, default_value_t = false)]
    pub normalize_output: bool,

    /// Output path for the JSON benchmark summary.
    #[arg(short = 'o', long = "output")]
    pub output_path: Option<PathBuf>,

    #[clap(flatten)]
    pub common: CommonOpt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InputEncoding {
    /// Build dictionary arrays before query execution.
    Dictionary,
    /// Keep the group keys as plain string arrays.
    Plain,
    /// Build plain strings and dictionary-encode them in the query plan.
    RuntimeDictionary,
}

impl InputEncoding {
    fn label(self) -> &'static str {
        match self {
            Self::Dictionary => "dictionary",
            Self::Plain => "plain",
            Self::RuntimeDictionary => "runtime_dictionary",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DictionaryValueReuse {
    /// Slice one array so every batch shares the same dictionary values.
    Shared,
    /// Build an independent values array for every input batch.
    PerBatch,
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
    generate_dict_with_offset(value_type, cardinality, None, None, null_percent, size, 0)
}

fn generate_dict_with_offset(
    value_type: DictValueType,
    cardinality: Cardinality,
    num_distinct: Option<usize>,
    value_length: Option<usize>,
    null_percent: NullPercent,
    size: usize,
    row_offset: usize,
) -> ArrayRef {
    let num_distinct = num_distinct
        .unwrap_or_else(|| cardinality.num_distinct(size))
        .clamp(1, i32::MAX as usize);

    let dict_values: ArrayRef = match value_type {
        DictValueType::Utf8 => {
            let strings: StringArray = (0..num_distinct)
                .map(|i| padded_value(format!("value_{i}"), value_length))
                .collect::<Vec<_>>()
                .into();
            Arc::new(strings)
        }
        DictValueType::ListUtf8 => {
            let flat: StringArray = (0..num_distinct)
                .flat_map(|i| {
                    (0..ITEMS_PER_VALUE).map(move |j| {
                        padded_value(format!("value_{i}_{j}"), value_length)
                    })
                })
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
        let row_index = row_offset + i;
        if null_every.is_some_and(|n| row_index.is_multiple_of(n)) {
            key_builder.append_null();
        } else {
            key_builder.append_value((row_index % num_distinct) as i32);
        }
    }
    let keys = key_builder.finish();

    Arc::new(
        DictionaryArray::<Int32Type>::try_new(keys, dict_values)
            .expect("valid dictionary array"),
    )
}

fn padded_value(mut value: String, value_length: Option<usize>) -> String {
    if let Some(value_length) = value_length {
        value.extend(std::iter::repeat_n(
            'x',
            value_length.saturating_sub(value.len()),
        ));
    }
    value
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
        benchmark_run.set_memory_pool(&ctx.runtime_env().memory_pool);

        for query_id in query_range {
            let query = &DICTIONARY_QUERIES[query_id - 1];
            let case_name = if self.input_encoding == InputEncoding::Dictionary
                && !self.normalize_output
                && self.dictionary_value_reuse == DictionaryValueReuse::Shared
            {
                query.name.to_string()
            } else {
                let output = if self.normalize_output {
                    "_normalized"
                } else {
                    ""
                };
                let dictionary_values =
                    if self.dictionary_value_reuse == DictionaryValueReuse::PerBatch {
                        "_per_batch_values"
                    } else {
                        ""
                    };
                format!(
                    "{}_{}{output}{dictionary_values}",
                    query.name,
                    self.input_encoding.label()
                )
            };
            benchmark_run.start_new_case(&case_name);

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
        let batches = self.make_record_batches(query)?;
        let schema = batches.first().expect("at least one input batch").schema();
        ctx.deregister_table("test_data")?;
        ctx.register_table(
            "test_data",
            Arc::new(MemTable::try_new(schema, vec![batches])?),
        )?;

        let sql = self.query_sql(query);

        let mut query_results = vec![];
        for i in 0..self.common.iterations {
            let start = Instant::now();
            let row_count =
                Self::execute_query_without_result_buffering(&sql, ctx).await?;
            let elapsed = start.elapsed();
            println!(
                "Query '{}' iteration {i} returned {row_count} rows in {elapsed:?}",
                query.name
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        Ok(query_results)
    }

    fn query_sql(&self, query: &DictionaryQuery) -> String {
        if self.input_encoding == InputEncoding::Plain
            || (self.input_encoding == InputEncoding::Dictionary
                && !self.normalize_output)
        {
            return query.sql.to_string();
        }

        let group_col = match self.input_encoding {
            InputEncoding::Dictionary => "dict_col",
            InputEncoding::RuntimeDictionary => {
                "arrow_cast(dict_col, 'Dictionary(Int32, Utf8)')"
            }
            InputEncoding::Plain => unreachable!(),
        };
        let select_col = if self.normalize_output {
            format!("arrow_cast({group_col}, 'Utf8') AS dict_col")
        } else {
            group_col.to_string()
        };

        if query.col2.is_some() {
            let group_col2 = match self.input_encoding {
                InputEncoding::Dictionary => "dict_col2",
                InputEncoding::RuntimeDictionary => {
                    "arrow_cast(dict_col2, 'Dictionary(Int32, Utf8)')"
                }
                InputEncoding::Plain => unreachable!(),
            };
            let select_col2 = if self.normalize_output {
                format!("arrow_cast({group_col2}, 'Utf8') AS dict_col2")
            } else {
                group_col2.to_string()
            };
            format!(
                "SELECT {select_col}, {select_col2}, COUNT(*) FROM test_data GROUP BY {group_col}, {group_col2}"
            )
        } else {
            format!("SELECT {select_col}, COUNT(*) FROM test_data GROUP BY {group_col}")
        }
    }

    fn make_record_batches(&self, query: &DictionaryQuery) -> Result<Vec<RecordBatch>> {
        let size = self.num_rows;
        if size == 0 {
            return exec_err!("num_rows must be greater than zero");
        }
        if let Some(num_distinct) = self.num_distinct {
            if num_distinct == 0 {
                return exec_err!("num_distinct must be greater than zero");
            }
            if num_distinct > size {
                return exec_err!(
                    "num_distinct ({num_distinct}) must not exceed num_rows ({size})"
                );
            }
            if num_distinct > i32::MAX as usize {
                return exec_err!(
                    "num_distinct must fit in the Int32 dictionary key type"
                );
            }
        }
        let input_batch_size = self.input_batch_size.unwrap_or(size);
        if input_batch_size == 0 {
            return exec_err!("input_batch_size must be greater than zero");
        }
        if self.input_encoding != InputEncoding::Dictionary
            && self.dictionary_value_reuse == DictionaryValueReuse::PerBatch
        {
            return exec_err!(
                "dictionary_value_reuse=per-batch requires input_encoding=dictionary"
            );
        }

        if self.input_encoding == InputEncoding::Dictionary
            && self.dictionary_value_reuse == DictionaryValueReuse::PerBatch
        {
            return (0..size)
                .step_by(input_batch_size)
                .map(|offset| {
                    self.make_record_batch(
                        query,
                        input_batch_size.min(size - offset),
                        offset,
                    )
                })
                .collect();
        }

        let batch = self.make_record_batch(query, size, 0)?;
        Ok((0..size)
            .step_by(input_batch_size)
            .map(|offset| batch.slice(offset, input_batch_size.min(size - offset)))
            .collect())
    }

    fn make_record_batch(
        &self,
        query: &DictionaryQuery,
        size: usize,
        row_offset: usize,
    ) -> Result<RecordBatch> {
        let num_distinct = |params: &DictColParams| {
            Some(
                self.num_distinct
                    .unwrap_or_else(|| params.cardinality.num_distinct(self.num_rows)),
            )
        };

        let col1 = generate_dict_with_offset(
            query.col.value_type,
            query.col.cardinality,
            num_distinct(&query.col),
            self.value_length,
            query.col.null_percent,
            size,
            row_offset,
        );

        let col1 = match self.input_encoding {
            InputEncoding::Dictionary => col1,
            InputEncoding::Plain | InputEncoding::RuntimeDictionary => {
                arrow::compute::cast(col1.as_ref(), &DataType::Utf8)?
            }
        };

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
                let col2 = generate_dict_with_offset(
                    col2_params.value_type,
                    col2_params.cardinality,
                    num_distinct(col2_params),
                    self.value_length,
                    col2_params.null_percent,
                    size,
                    row_offset,
                );
                let col2 = match self.input_encoding {
                    InputEncoding::Dictionary => col2,
                    InputEncoding::Plain | InputEncoding::RuntimeDictionary => {
                        arrow::compute::cast(col2.as_ref(), &DataType::Utf8)?
                    }
                };
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use std::collections::BTreeMap;

    fn run_opt(input_encoding: InputEncoding) -> RunOpt {
        RunOpt {
            num_rows: 10,
            query: Some(1),
            input_encoding,
            num_distinct: Some(3),
            input_batch_size: Some(4),
            value_length: Some(32),
            dictionary_value_reuse: DictionaryValueReuse::Shared,
            normalize_output: true,
            output_path: None,
            common: CommonOpt {
                iterations: 1,
                partitions: Some(1),
                batch_size: None,
                mem_pool_type: "fair".to_string(),
                memory_limit: None,
                sort_spill_reservation_bytes: None,
                debug: false,
                simulate_latency: false,
            },
        }
    }

    #[test]
    fn dictionary_batches_share_values_and_preserve_padding() {
        let batches = run_opt(InputEncoding::Dictionary)
            .make_record_batches(&DICTIONARY_QUERIES[0])
            .unwrap();

        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![4, 4, 2]
        );

        let first = batches[0].column(0);
        let second = batches[1].column(0);
        let first = first
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let second = second
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert!(Arc::ptr_eq(first.values(), second.values()));

        let values = first
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.iter().flatten().all(|value| value.len() == 32));
    }

    #[test]
    fn query_sql_keeps_user_visible_string_type() {
        let query = &DICTIONARY_QUERIES[0];
        assert_eq!(run_opt(InputEncoding::Plain).query_sql(query), query.sql);

        let mut default_dictionary = run_opt(InputEncoding::Dictionary);
        default_dictionary.normalize_output = false;
        assert_eq!(default_dictionary.query_sql(query), query.sql);

        assert_eq!(
            run_opt(InputEncoding::Dictionary).query_sql(query),
            "SELECT arrow_cast(dict_col, 'Utf8') AS dict_col, COUNT(*) FROM test_data GROUP BY dict_col"
        );
        assert_eq!(
            run_opt(InputEncoding::RuntimeDictionary).query_sql(query),
            "SELECT arrow_cast(arrow_cast(dict_col, 'Dictionary(Int32, Utf8)'), 'Utf8') AS dict_col, COUNT(*) FROM test_data GROUP BY arrow_cast(dict_col, 'Dictionary(Int32, Utf8)')"
        );
    }

    #[test]
    fn per_batch_dictionaries_keep_global_row_pattern() {
        let mut opt = run_opt(InputEncoding::Dictionary);
        opt.dictionary_value_reuse = DictionaryValueReuse::PerBatch;
        let batches = opt.make_record_batches(&DICTIONARY_QUERIES[0]).unwrap();

        let first = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let second = batches[1]
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert!(!Arc::ptr_eq(first.values(), second.values()));
        assert_eq!(first.keys().value(0), 0);
        assert_eq!(second.keys().value(0), 1);
    }

    #[test]
    fn rejects_zero_input_batch_size() {
        let mut opt = run_opt(InputEncoding::Dictionary);
        opt.input_batch_size = Some(0);
        let err = opt.make_record_batches(&DICTIONARY_QUERIES[0]).unwrap_err();
        assert!(
            err.to_string()
                .contains("input_batch_size must be greater than zero")
        );
    }

    #[test]
    fn rejects_invalid_distinct_count_and_dictionary_reuse() {
        let query = &DICTIONARY_QUERIES[0];

        let mut zero_distinct = run_opt(InputEncoding::Dictionary);
        zero_distinct.num_distinct = Some(0);
        assert!(
            zero_distinct
                .make_record_batches(query)
                .unwrap_err()
                .to_string()
                .contains("num_distinct must be greater than zero")
        );

        let mut excess_distinct = run_opt(InputEncoding::Dictionary);
        excess_distinct.num_distinct = Some(excess_distinct.num_rows + 1);
        assert!(
            excess_distinct
                .make_record_batches(query)
                .unwrap_err()
                .to_string()
                .contains("must not exceed num_rows")
        );

        let mut plain_per_batch = run_opt(InputEncoding::Plain);
        plain_per_batch.dictionary_value_reuse = DictionaryValueReuse::PerBatch;
        assert!(
            plain_per_batch
                .make_record_batches(query)
                .unwrap_err()
                .to_string()
                .contains("requires input_encoding=dictionary")
        );
    }

    async fn grouped_counts(opt: &RunOpt) -> BTreeMap<String, i64> {
        let query = &DICTIONARY_QUERIES[0];
        let batches = opt.make_record_batches(query).unwrap();
        let schema = batches[0].schema();
        let ctx = SessionContext::new();
        ctx.register_table(
            "test_data",
            Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()),
        )
        .unwrap();

        let batches = ctx
            .sql(&opt.query_sql(query))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut counts = BTreeMap::new();
        for batch in batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                counts.insert(keys.value(row).to_string(), values.value(row));
            }
        }
        counts
    }

    #[tokio::test]
    async fn encodings_produce_identical_grouped_counts() {
        let plain = run_opt(InputEncoding::Plain);
        let shared = run_opt(InputEncoding::Dictionary);
        let mut per_batch = run_opt(InputEncoding::Dictionary);
        per_batch.dictionary_value_reuse = DictionaryValueReuse::PerBatch;
        let runtime = run_opt(InputEncoding::RuntimeDictionary);

        let expected = grouped_counts(&plain).await;
        assert_eq!(
            expected.values().copied().collect::<Vec<_>>(),
            vec![4, 3, 3]
        );
        assert_eq!(grouped_counts(&shared).await, expected);
        assert_eq!(grouped_counts(&per_batch).await, expected);
        assert_eq!(grouped_counts(&runtime).await, expected);
    }
}
