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

use super::*;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::listing_table_factory::ListingTableFactory;
use datafusion::datasource::provider::TableProviderFactory;
use datafusion_expr::logical_plan::DdlStatement;
use test_utils::{batches_to_vec, partitions_to_sorted_vec};

#[tokio::test]
async fn sort_with_lots_of_repetition_values() -> Result<()> {
    let ctx = SessionContext::new();
    let filename = "tests/data/repeat_much.snappy.parquet";

    ctx.register_parquet("rep", filename, ParquetReadOptions::default())
        .await?;
    let sql = "select a from rep order by a";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = batches_to_vec(&actual);

    let sql1 = "select a from rep";
    let expected = execute_to_batches(&ctx, sql1).await;
    let expected = partitions_to_sorted_vec(&[expected]);

    assert_eq!(actual.len(), expected.len());
    for i in 0..actual.len() {
        assert_eq!(actual[i], expected[i]);
    }
    Ok(())
}

#[tokio::test]
async fn create_external_table_with_order() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "CREATE EXTERNAL TABLE dt (a_id integer, a_str string, a_bool boolean) STORED AS CSV WITH ORDER (a_id ASC) LOCATION 'file://path/to/table';";
    let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) =
        ctx.state().create_logical_plan(sql).await?
    else {
        panic!("Wrong command")
    };

    let listing_table_factory = Arc::new(ListingTableFactory::new());
    let table_dyn = listing_table_factory.create(&ctx.state(), &cmd).await?;
    let table = table_dyn.as_any().downcast_ref::<ListingTable>().unwrap();
    assert_eq!(cmd.order_exprs.len(), 1);
    assert_eq!(cmd.order_exprs, table.options().file_sort_order);
    Ok(())
}

#[tokio::test]
async fn create_external_table_with_ddl_ordered_non_cols() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "CREATE EXTERNAL TABLE dt (a_id integer, a_str string, a_bool boolean) STORED AS CSV WITH ORDER (a ASC) LOCATION 'file://path/to/table';";
    match ctx.state().create_logical_plan(sql).await {
        Ok(_) => panic!("Expecting error."),
        Err(e) => {
            assert_eq!(
                e.strip_backtrace(),
                "Error during planning: Column a is not in schema"
            )
        }
    }
    Ok(())
}

#[tokio::test]
async fn create_external_table_with_ddl_ordered_without_schema() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "CREATE EXTERNAL TABLE dt STORED AS CSV WITH ORDER (a ASC) LOCATION 'file://path/to/table';";
    match ctx.state().create_logical_plan(sql).await {
        Ok(_) => panic!("Expecting error."),
        Err(e) => {
            assert_eq!(e.strip_backtrace(), "Error during planning: Provide a schema before specifying the order while creating a table.")
        }
    }
    Ok(())
}

#[tokio::test]
async fn sort_with_duplicate_sort_exprs() -> Result<()> {
    let ctx = SessionContext::new();

    let t1_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let t1_data = RecordBatch::try_new(
        t1_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2, 4, 9, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )?;
    ctx.register_batch("t1", t1_data)?;

    let sql = "select * from t1 order by id desc, id, name, id asc";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan().unwrap();
    let expected = vec![
        "Sort: t1.id DESC NULLS FIRST, t1.name ASC NULLS LAST [id:Int32;N, name:Utf8;N]",
        "  TableScan: t1 projection=[id, name] [id:Int32;N, name:Utf8;N]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = [
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 9  | c    |",
        "| 4  | b    |",
        "| 4  | e    |",
        "| 3  | d    |",
        "| 2  | a    |",
        "+----+------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &results);

    let sql = "select * from t1 order by id asc, id, name, id desc;";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan().unwrap();
    let expected = vec![
        "Sort: t1.id ASC NULLS LAST, t1.name ASC NULLS LAST [id:Int32;N, name:Utf8;N]",
        "  TableScan: t1 projection=[id, name] [id:Int32;N, name:Utf8;N]",
    ];

    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let expected = [
        "+----+------+",
        "| id | name |",
        "+----+------+",
        "| 2  | a    |",
        "| 3  | d    |",
        "| 4  | b    |",
        "| 4  | e    |",
        "| 9  | c    |",
        "+----+------+",
    ];

    let results = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &results);

    Ok(())
}

/// Minimal test case for https://github.com/apache/arrow-datafusion/issues/5970
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_issue5970_mini() -> Result<()> {
    let config = SessionConfig::new()
        .with_target_partitions(2)
        .with_repartition_sorts(true);
    let ctx = SessionContext::new_with_config(config);
    let sql = "
WITH
    m0(t) AS (
        VALUES (0), (1), (2)),
    m1(t) AS (
        VALUES (0), (1)),
    u AS (
        SELECT 0 as m, t FROM m0 GROUP BY 1, 2),
    v AS (
        SELECT 1 as m, t FROM m1 GROUP BY 1, 2)
SELECT * FROM u
UNION ALL
SELECT * FROM v
ORDER BY 1, 2;
    ";

    // check phys. plan
    let dataframe = ctx.sql(sql).await.unwrap();
    let plan = dataframe.into_optimized_plan().unwrap();
    let plan = ctx.state().create_physical_plan(&plan).await.unwrap();
    let expected = vec![
        "SortPreservingMergeExec: [m@0 ASC NULLS LAST,t@1 ASC NULLS LAST]",
        "  SortExec: expr=[m@0 ASC NULLS LAST,t@1 ASC NULLS LAST]",
        "    InterleaveExec",
        "      ProjectionExec: expr=[Int64(0)@0 as m, t@1 as t]",
        "        AggregateExec: mode=FinalPartitioned, gby=[Int64(0)@0 as Int64(0), t@1 as t], aggr=[]",
        "          CoalesceBatchesExec: target_batch_size=8192",
        "            RepartitionExec: partitioning=Hash([Int64(0)@0, t@1], 2), input_partitions=2",
        "              RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        "                AggregateExec: mode=Partial, gby=[0 as Int64(0), t@0 as t], aggr=[]",
        "                  ProjectionExec: expr=[column1@0 as t]",
        "                    ValuesExec",
        "      ProjectionExec: expr=[Int64(1)@0 as m, t@1 as t]",
        "        AggregateExec: mode=FinalPartitioned, gby=[Int64(1)@0 as Int64(1), t@1 as t], aggr=[]",
        "          CoalesceBatchesExec: target_batch_size=8192",
        "            RepartitionExec: partitioning=Hash([Int64(1)@0, t@1], 2), input_partitions=2",
        "              RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
        "                AggregateExec: mode=Partial, gby=[1 as Int64(1), t@0 as t], aggr=[]",
        "                  ProjectionExec: expr=[column1@0 as t]",
        "                    ValuesExec",
    ];
    let formatted = displayable(plan.as_ref()).indent(true).to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // sometimes it "just works"
    for i in 0..10 {
        println!("run: {i}");
        let actual = execute_to_batches(&ctx, sql).await;

        // in https://github.com/apache/arrow-datafusion/issues/5970 the order of the output was sometimes not right
        let expected = [
            "+---+---+",
            "| m | t |",
            "+---+---+",
            "| 0 | 0 |",
            "| 0 | 1 |",
            "| 0 | 2 |",
            "| 1 | 0 |",
            "| 1 | 1 |",
            "+---+---+",
        ];
        assert_batches_eq!(expected, &actual);
    }
    Ok(())
}
