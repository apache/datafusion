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
use std::time::SystemTime;

use crate::fuzz_cases::join_fuzz::JoinTestType::{HjSmj, NljHj};

use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::JoinSide;
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::memory::MemorySourceConfig;
use datafusion_physical_plan::source::DataSourceExec;

use itertools::Itertools;
use rand::Rng;
use test_utils::stagger_batch_with_seed;

// Determines what Fuzz tests needs to run
// Ideally all tests should match, but in reality some tests
// passes only partial cases
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum JoinTestType {
    // compare NestedLoopJoin and HashJoin
    NljHj,
    // compare HashJoin and SortMergeJoin, no need to compare SortMergeJoin and NestedLoopJoin
    // because if existing variants both passed that means SortMergeJoin and NestedLoopJoin also passes
    HjSmj,
}

fn col_lt_col_filter(schema1: Arc<Schema>, schema2: Arc<Schema>) -> JoinFilter {
    let less_filter = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("x", 1)),
        Operator::Lt,
        Arc::new(Column::new("x", 0)),
    )) as _;
    let column_indices = vec![
        ColumnIndex {
            index: 2,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: 2,
            side: JoinSide::Right,
        },
    ];
    let intermediate_schema = Schema::new(vec![
        schema1
            .field_with_name("x")
            .unwrap()
            .clone()
            .with_nullable(true),
        schema2
            .field_with_name("x")
            .unwrap()
            .clone()
            .with_nullable(true),
    ]);

    JoinFilter::new(less_filter, column_indices, Arc::new(intermediate_schema))
}

#[tokio::test]
async fn test_inner_join_1k_filtered() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::Inner,
        Some(Box::new(col_lt_col_filter)),
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_inner_join_1k() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::Inner,
        None,
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_left_join_1k() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::Left,
        None,
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_left_join_1k_filtered() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::Left,
        Some(Box::new(col_lt_col_filter)),
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_right_join_1k() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::Right,
        None,
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_right_join_1k_filtered() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::Right,
        Some(Box::new(col_lt_col_filter)),
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_full_join_1k() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::Full,
        None,
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_full_join_1k_filtered() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::Full,
        Some(Box::new(col_lt_col_filter)),
    )
    .run_test(&[NljHj, HjSmj], false)
    .await
}

#[tokio::test]
async fn test_semi_join_1k() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::LeftSemi,
        None,
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_semi_join_1k_filtered() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::LeftSemi,
        Some(Box::new(col_lt_col_filter)),
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_left_anti_join_1k() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::LeftAnti,
        None,
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_left_anti_join_1k_filtered() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::LeftAnti,
        Some(Box::new(col_lt_col_filter)),
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_right_anti_join_1k() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::RightAnti,
        None,
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_right_anti_join_1k_filtered() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::RightAnti,
        Some(Box::new(col_lt_col_filter)),
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_left_mark_join_1k() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::LeftMark,
        None,
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

#[tokio::test]
async fn test_left_mark_join_1k_filtered() {
    JoinFuzzTestCase::new(
        make_staggered_batches(1000),
        make_staggered_batches(1000),
        JoinType::LeftMark,
        Some(Box::new(col_lt_col_filter)),
    )
    .run_test(&[HjSmj, NljHj], false)
    .await
}

type JoinFilterBuilder = Box<dyn Fn(Arc<Schema>, Arc<Schema>) -> JoinFilter>;

struct JoinFuzzTestCase {
    batch_sizes: &'static [usize],
    input1: Vec<RecordBatch>,
    input2: Vec<RecordBatch>,
    join_type: JoinType,
    join_filter_builder: Option<JoinFilterBuilder>,
}

impl JoinFuzzTestCase {
    fn new(
        input1: Vec<RecordBatch>,
        input2: Vec<RecordBatch>,
        join_type: JoinType,
        join_filter_builder: Option<JoinFilterBuilder>,
    ) -> Self {
        Self {
            batch_sizes: &[1, 2, 7, 49, 50, 51, 100],
            input1,
            input2,
            join_type,
            join_filter_builder,
        }
    }

    fn on_columns(&self) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
        let schema1 = self.input1[0].schema();
        let schema2 = self.input2[0].schema();
        vec![
            (
                Arc::new(Column::new_with_schema("a", &schema1).unwrap()) as _,
                Arc::new(Column::new_with_schema("a", &schema2).unwrap()) as _,
            ),
            (
                Arc::new(Column::new_with_schema("b", &schema1).unwrap()) as _,
                Arc::new(Column::new_with_schema("b", &schema2).unwrap()) as _,
            ),
        ]
    }

    /// Helper function for building NLJoin filter, returning intermediate
    /// schema as a union of origin filter intermediate schema and
    /// on-condition schema
    fn intermediate_schema(&self) -> Schema {
        let filter_schema = if let Some(filter) = self.join_filter() {
            filter.schema().as_ref().to_owned()
        } else {
            Schema::empty()
        };

        let schema1 = self.input1[0].schema();
        let schema2 = self.input2[0].schema();

        let on_schema = Schema::new(vec![
            schema1
                .field_with_name("a")
                .unwrap()
                .to_owned()
                .with_nullable(true),
            schema1
                .field_with_name("b")
                .unwrap()
                .to_owned()
                .with_nullable(true),
            schema2.field_with_name("a").unwrap().to_owned(),
            schema2.field_with_name("b").unwrap().to_owned(),
        ]);

        Schema::new(
            filter_schema
                .fields
                .into_iter()
                .cloned()
                .chain(on_schema.fields.into_iter().cloned())
                .collect_vec(),
        )
    }

    /// Helper function for building NLJoin filter, returns the union
    /// of original filter expression and on-condition expression
    fn composite_filter_expression(&self) -> PhysicalExprRef {
        let (filter_expression, column_idx_offset) =
            if let Some(filter) = self.join_filter() {
                (
                    filter.expression().to_owned(),
                    filter.schema().fields().len(),
                )
            } else {
                (Arc::new(Literal::new(ScalarValue::from(true))) as _, 0)
            };

        let equal_a = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", column_idx_offset)),
            Operator::Eq,
            Arc::new(Column::new("a", column_idx_offset + 2)),
        ));
        let equal_b = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", column_idx_offset + 1)),
            Operator::Eq,
            Arc::new(Column::new("b", column_idx_offset + 3)),
        ));
        let on_expression = Arc::new(BinaryExpr::new(equal_a, Operator::And, equal_b));

        Arc::new(BinaryExpr::new(
            filter_expression,
            Operator::And,
            on_expression,
        ))
    }

    /// Helper function for building NLJoin filter, returning the union
    /// of original filter column indices and on-condition column indices.
    /// Result must match intermediate schema.
    fn column_indices(&self) -> Vec<ColumnIndex> {
        let mut column_indices = if let Some(filter) = self.join_filter() {
            filter.column_indices().to_vec()
        } else {
            vec![]
        };

        let on_column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ];

        column_indices.extend(on_column_indices);
        column_indices
    }

    fn left_right(&self) -> (Arc<DataSourceExec>, Arc<DataSourceExec>) {
        let schema1 = self.input1[0].schema();
        let schema2 = self.input2[0].schema();
        let left =
            MemorySourceConfig::try_new_exec(&[self.input1.clone()], schema1, None)
                .unwrap();
        let right =
            MemorySourceConfig::try_new_exec(&[self.input2.clone()], schema2, None)
                .unwrap();
        (left, right)
    }

    fn join_filter(&self) -> Option<JoinFilter> {
        let schema1 = self.input1[0].schema();
        let schema2 = self.input2[0].schema();
        self.join_filter_builder
            .as_ref()
            .map(|builder| builder(schema1, schema2))
    }

    fn sort_merge_join(&self) -> Arc<SortMergeJoinExec> {
        let (left, right) = self.left_right();
        Arc::new(
            SortMergeJoinExec::try_new(
                left,
                right,
                self.on_columns().clone(),
                self.join_filter(),
                self.join_type,
                vec![SortOptions::default(); self.on_columns().len()],
                false,
            )
            .unwrap(),
        )
    }

    fn hash_join(&self) -> Arc<HashJoinExec> {
        let (left, right) = self.left_right();
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                self.on_columns().clone(),
                self.join_filter(),
                &self.join_type,
                None,
                PartitionMode::Partitioned,
                false,
            )
            .unwrap(),
        )
    }

    fn nested_loop_join(&self) -> Arc<NestedLoopJoinExec> {
        let (left, right) = self.left_right();

        let column_indices = self.column_indices();
        let intermediate_schema = self.intermediate_schema();
        let expression = self.composite_filter_expression();

        let filter =
            JoinFilter::new(expression, column_indices, Arc::new(intermediate_schema));

        Arc::new(
            NestedLoopJoinExec::try_new(left, right, Some(filter), &self.join_type, None)
                .unwrap(),
        )
    }

    /// Perform joins tests on same inputs and verify outputs are equal
    /// `join_tests` - identifies what join types to test
    /// if `debug` flag is set the test will save randomly generated inputs and outputs to user folders,
    /// so it is easy to debug a test on top of the failed data
    async fn run_test(&self, join_tests: &[JoinTestType], debug: bool) {
        for batch_size in self.batch_sizes {
            let session_config = SessionConfig::new().with_batch_size(*batch_size);
            let ctx = SessionContext::new_with_config(session_config);
            let task_ctx = ctx.task_ctx();

            let hj = self.hash_join();
            let hj_collected = collect(hj, task_ctx.clone()).await.unwrap();

            let smj = self.sort_merge_join();
            let smj_collected = collect(smj, task_ctx.clone()).await.unwrap();

            let nlj = self.nested_loop_join();
            let nlj_collected = collect(nlj, task_ctx.clone()).await.unwrap();

            // Get actual row counts(without formatting overhead) for HJ and SMJ
            let hj_rows = hj_collected.iter().fold(0, |acc, b| acc + b.num_rows());
            let smj_rows = smj_collected.iter().fold(0, |acc, b| acc + b.num_rows());
            let nlj_rows = nlj_collected.iter().fold(0, |acc, b| acc + b.num_rows());

            // compare
            let smj_formatted =
                pretty_format_batches(&smj_collected).unwrap().to_string();
            let hj_formatted = pretty_format_batches(&hj_collected).unwrap().to_string();
            let nlj_formatted =
                pretty_format_batches(&nlj_collected).unwrap().to_string();

            let mut smj_formatted_sorted: Vec<&str> =
                smj_formatted.trim().lines().collect();
            smj_formatted_sorted.sort_unstable();

            let mut hj_formatted_sorted: Vec<&str> =
                hj_formatted.trim().lines().collect();
            hj_formatted_sorted.sort_unstable();

            let mut nlj_formatted_sorted: Vec<&str> =
                nlj_formatted.trim().lines().collect();
            nlj_formatted_sorted.sort_unstable();

            if debug
                && ((join_tests.contains(&NljHj) && nlj_rows != hj_rows)
                    || (join_tests.contains(&HjSmj) && smj_rows != hj_rows))
            {
                let fuzz_debug = "fuzz_test_debug";
                std::fs::remove_dir_all(fuzz_debug).unwrap_or(());
                std::fs::create_dir_all(fuzz_debug).unwrap();
                let out_dir_name = &format!("{fuzz_debug}/batch_size_{batch_size}");
                println!("Test result data mismatch found. HJ rows {}, SMJ rows {}, NLJ rows {}", hj_rows, smj_rows, nlj_rows);
                println!("The debug is ON. Input data will be saved to {out_dir_name}");

                Self::save_partitioned_batches_as_parquet(
                    &self.input1,
                    out_dir_name,
                    "input1",
                );
                Self::save_partitioned_batches_as_parquet(
                    &self.input2,
                    out_dir_name,
                    "input2",
                );

                if join_tests.contains(&NljHj) && nlj_rows != hj_rows {
                    println!("=============== HashJoinExec ==================");
                    hj_formatted_sorted.iter().for_each(|s| println!("{}", s));
                    println!("=============== NestedLoopJoinExec ==================");
                    nlj_formatted_sorted.iter().for_each(|s| println!("{}", s));

                    Self::save_partitioned_batches_as_parquet(
                        &nlj_collected,
                        out_dir_name,
                        "nlj",
                    );
                    Self::save_partitioned_batches_as_parquet(
                        &hj_collected,
                        out_dir_name,
                        "hj",
                    );
                }

                if join_tests.contains(&HjSmj) && smj_rows != hj_rows {
                    println!("=============== HashJoinExec ==================");
                    hj_formatted_sorted.iter().for_each(|s| println!("{}", s));
                    println!("=============== SortMergeJoinExec ==================");
                    smj_formatted_sorted.iter().for_each(|s| println!("{}", s));

                    Self::save_partitioned_batches_as_parquet(
                        &hj_collected,
                        out_dir_name,
                        "hj",
                    );
                    Self::save_partitioned_batches_as_parquet(
                        &smj_collected,
                        out_dir_name,
                        "smj",
                    );
                }
            }

            if join_tests.contains(&NljHj) {
                let err_msg_rowcnt = format!("NestedLoopJoinExec and HashJoinExec produced different row counts, batch_size: {}", batch_size);
                assert_eq!(nlj_rows, hj_rows, "{}", err_msg_rowcnt.as_str());

                let err_msg_contents = format!("NestedLoopJoinExec and HashJoinExec produced different results, batch_size: {}", batch_size);
                // row level compare if any of joins returns the result
                // the reason is different formatting when there is no rows
                for (i, (nlj_line, hj_line)) in nlj_formatted_sorted
                    .iter()
                    .zip(&hj_formatted_sorted)
                    .enumerate()
                {
                    assert_eq!(
                        (i, nlj_line),
                        (i, hj_line),
                        "{}",
                        err_msg_contents.as_str()
                    );
                }
            }

            if join_tests.contains(&HjSmj) {
                let err_msg_row_cnt = format!("HashJoinExec and SortMergeJoinExec produced different row counts, batch_size: {}", &batch_size);
                assert_eq!(hj_rows, smj_rows, "{}", err_msg_row_cnt.as_str());

                let err_msg_contents = format!("SortMergeJoinExec and HashJoinExec produced different results, batch_size: {}", &batch_size);
                // row level compare if any of joins returns the result
                // the reason is different formatting when there is no rows
                if smj_rows > 0 || hj_rows > 0 {
                    for (i, (smj_line, hj_line)) in smj_formatted_sorted
                        .iter()
                        .zip(&hj_formatted_sorted)
                        .enumerate()
                    {
                        assert_eq!(
                            (i, smj_line),
                            (i, hj_line),
                            "{}",
                            err_msg_contents.as_str()
                        );
                    }
                }
            }
        }
    }

    /// This method useful for debugging fuzz tests
    /// It helps to save randomly generated input test data for both join inputs into the user folder
    /// as a parquet files preserving partitioning.
    /// Once the data is saved it is possible to run a custom test on top of the saved data and debug
    ///
    /// #[tokio::test]
    /// async fn test1() {
    ///     let left: Vec<RecordBatch> = JoinFuzzTestCase::load_partitioned_batches_from_parquet("fuzz_test_debug/batch_size_2/input1").await.unwrap();
    ///     let right: Vec<RecordBatch> = JoinFuzzTestCase::load_partitioned_batches_from_parquet("fuzz_test_debug/batch_size_2/input2").await.unwrap();
    ///
    ///     JoinFuzzTestCase::new(
    ///         left,
    ///         right,
    ///         JoinType::LeftSemi,
    ///         Some(Box::new(col_lt_col_filter)),
    ///     )
    ///     .run_test(&[JoinTestType::HjSmj], false)
    ///     .await;
    /// }
    fn save_partitioned_batches_as_parquet(
        input: &[RecordBatch],
        output_dir: &str,
        out_name: &str,
    ) {
        let out_path = &format!("{output_dir}/{out_name}");
        std::fs::remove_dir_all(out_path).unwrap_or(());
        std::fs::create_dir_all(out_path).unwrap();

        input.iter().enumerate().for_each(|(idx, batch)| {
            let file_path = format!("{out_path}/file_{}.parquet", idx);
            let mut file = std::fs::File::create(&file_path).unwrap();
            println!(
                "{}: Saving batch idx {} rows {} to parquet {}",
                &out_name,
                idx,
                batch.num_rows(),
                &file_path
            );
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                &mut file,
                input.first().unwrap().schema(),
                None,
            )
            .expect("creating writer");
            writer.write(batch).unwrap();
            writer.close().unwrap();
        });
    }

    /// Read parquet files preserving partitions, i.e. 1 file -> 1 partition
    /// Files can be of different sizes
    /// The method can be useful to read partitions have been saved by `save_partitioned_batches_as_parquet`
    /// for test debugging purposes
    #[allow(dead_code)]
    async fn load_partitioned_batches_from_parquet(
        dir: &str,
    ) -> std::io::Result<Vec<RecordBatch>> {
        let ctx: SessionContext = SessionContext::new();
        let mut batches: Vec<RecordBatch> = vec![];
        let mut entries = std::fs::read_dir(dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?;

        // important to read files using the same order as they have been written
        // sort by modification time
        entries.sort_by_key(|path| {
            std::fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        });

        for entry in entries {
            let path = entry.as_path();

            if path.is_file() {
                let mut batch = ctx
                    .read_parquet(
                        path.to_str().unwrap(),
                        datafusion::prelude::ParquetReadOptions::default(),
                    )
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();

                batches.append(&mut batch);
            }
        }
        Ok(batches)
    }
}

/// Return randomly sized record batches with:
/// two sorted int32 columns 'a', 'b' ranged from 0..99 as join columns
/// two random int32 columns 'x', 'y' as other columns
fn make_staggered_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::thread_rng();
    let mut input12: Vec<(i32, i32)> = vec![(0, 0); len];
    let mut input3: Vec<i32> = vec![0; len];
    let mut input4: Vec<i32> = vec![0; len];
    input12
        .iter_mut()
        .for_each(|v| *v = (rng.gen_range(0..100), rng.gen_range(0..100)));
    rng.fill(&mut input3[..]);
    rng.fill(&mut input4[..]);
    input12.sort_unstable();
    let input1 = Int32Array::from_iter_values(input12.clone().into_iter().map(|k| k.0));
    let input2 = Int32Array::from_iter_values(input12.clone().into_iter().map(|k| k.1));
    let input3 = Int32Array::from_iter_values(input3);
    let input4 = Int32Array::from_iter_values(input4);

    // split into several record batches
    let batch = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(input1) as ArrayRef),
        ("b", Arc::new(input2) as ArrayRef),
        ("x", Arc::new(input3) as ArrayRef),
        ("y", Arc::new(input4) as ArrayRef),
    ])
    .unwrap();

    // use a random number generator to pick a random sized output
    stagger_batch_with_seed(batch, 42)
}
