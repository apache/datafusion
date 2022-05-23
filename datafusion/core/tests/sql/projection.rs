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

use datafusion::logical_plan::{provider_as_source, LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::test_util::scan_empty;
use tempfile::TempDir;

use super::*;

#[tokio::test]
async fn projection_same_fields() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select (1+1) as a from (select 1 as a) as b;";
    let actual = execute_to_batches(&ctx, sql).await;

    #[rustfmt::skip]
    let expected = vec![
        "+---+",
        "| a |",
        "+---+",
        "| 2 |",
        "+---+"
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn projection_type_alias() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    // Query that aliases one column to the name of a different column
    // that also has a different type (c1 == float32, c3 == boolean)
    let sql = "SELECT c1 as c3 FROM aggregate_simple ORDER BY c3 LIMIT 2";
    let actual = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c3      |",
        "+---------+",
        "| 0.00001 |",
        "| 0.00002 |",
        "+---------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn csv_query_group_by_avg_with_projection() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT avg(c12), c1 FROM aggregate_test_100 GROUP BY c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------------------+----+",
        "| AVG(aggregate_test_100.c12) | c1 |",
        "+-----------------------------+----+",
        "| 0.41040709263815384         | b  |",
        "| 0.48600669271341534         | e  |",
        "| 0.48754517466109415         | a  |",
        "| 0.48855379387549824         | d  |",
        "| 0.6600456536439784          | c  |",
        "+-----------------------------+----+",
    ];
    assert_batches_sorted_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn parallel_projection() -> Result<()> {
    let partition_count = 4;
    let results =
        partitioned_csv::execute("SELECT c1, c2 FROM test", partition_count).await?;

    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 3  | 1  |",
        "| 3  | 2  |",
        "| 3  | 3  |",
        "| 3  | 4  |",
        "| 3  | 5  |",
        "| 3  | 6  |",
        "| 3  | 7  |",
        "| 3  | 8  |",
        "| 3  | 9  |",
        "| 3  | 10 |",
        "| 2  | 1  |",
        "| 2  | 2  |",
        "| 2  | 3  |",
        "| 2  | 4  |",
        "| 2  | 5  |",
        "| 2  | 6  |",
        "| 2  | 7  |",
        "| 2  | 8  |",
        "| 2  | 9  |",
        "| 2  | 10 |",
        "| 1  | 1  |",
        "| 1  | 2  |",
        "| 1  | 3  |",
        "| 1  | 4  |",
        "| 1  | 5  |",
        "| 1  | 6  |",
        "| 1  | 7  |",
        "| 1  | 8  |",
        "| 1  | 9  |",
        "| 1  | 10 |",
        "| 0  | 1  |",
        "| 0  | 2  |",
        "| 0  | 3  |",
        "| 0  | 4  |",
        "| 0  | 5  |",
        "| 0  | 6  |",
        "| 0  | 7  |",
        "| 0  | 8  |",
        "| 0  | 9  |",
        "| 0  | 10 |",
        "+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn subquery_alias_case_insensitive() -> Result<()> {
    let partition_count = 1;
    let results =
        partitioned_csv::execute("SELECT V1.c1, v1.C2 FROM (SELECT test.C1, TEST.c2 FROM test) V1 ORDER BY v1.c1, V1.C2 LIMIT 1", partition_count).await?;

    let expected = vec![
        "+----+----+",
        "| c1 | c2 |",
        "+----+----+",
        "| 0  | 1  |",
        "+----+----+",
    ];
    assert_batches_sorted_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn projection_on_table_scan() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let partition_count = 4;
    let ctx = partitioned_csv::create_ctx(&tmp_dir, partition_count).await?;

    let table = ctx.table("test")?;
    let logical_plan = LogicalPlanBuilder::from(table.to_logical_plan()?)
        .project(vec![col("c2")])?
        .build()?;

    let optimized_plan = ctx.optimize(&logical_plan)?;
    match &optimized_plan {
        LogicalPlan::Projection(Projection { input, .. }) => match &**input {
            LogicalPlan::TableScan(TableScan {
                source,
                projected_schema,
                ..
            }) => {
                assert_eq!(source.schema().fields().len(), 3);
                assert_eq!(projected_schema.fields().len(), 1);
            }
            _ => panic!("input to projection should be TableScan"),
        },
        _ => panic!("expect optimized_plan to be projection"),
    }

    let expected = "Projection: #test.c2\
                    \n  TableScan: test projection=Some([1])";
    assert_eq!(format!("{:?}", optimized_plan), expected);

    let physical_plan = ctx.create_physical_plan(&optimized_plan).await?;

    assert_eq!(1, physical_plan.schema().fields().len());
    assert_eq!("c2", physical_plan.schema().field(0).name().as_str());
    let task_ctx = ctx.task_ctx();
    let batches = collect(physical_plan, task_ctx).await?;
    assert_eq!(40, batches.iter().map(|x| x.num_rows()).sum::<usize>());

    Ok(())
}

#[tokio::test]
async fn preserve_nullability_on_projection() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let ctx = partitioned_csv::create_ctx(&tmp_dir, 1).await?;

    let schema: Schema = ctx.table("test").unwrap().schema().clone().into();
    assert!(!schema.field_with_name("c1")?.is_nullable());

    let plan = scan_empty(None, &schema, None)?
        .project(vec![col("c1")])?
        .build()?;

    let plan = ctx.optimize(&plan)?;
    let physical_plan = ctx.create_physical_plan(&Arc::new(plan)).await?;
    assert!(!physical_plan.schema().field_with_name("c1")?.is_nullable());
    Ok(())
}

#[tokio::test]
async fn projection_on_memory_scan() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]);
    let schema = SchemaRef::new(schema);

    let partitions = vec![vec![RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_slice(&[1, 10, 10, 100])),
            Arc::new(Int32Array::from_slice(&[2, 12, 12, 120])),
            Arc::new(Int32Array::from_slice(&[3, 12, 12, 120])),
        ],
    )?]];

    let provider = Arc::new(MemTable::try_new(schema, partitions)?);
    let plan =
        LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(provider), None)?
            .project(vec![col("b")])?
            .build()?;
    assert_fields_eq(&plan, vec!["b"]);

    let ctx = SessionContext::new();
    let optimized_plan = ctx.optimize(&plan)?;
    match &optimized_plan {
        LogicalPlan::Projection(Projection { input, .. }) => match &**input {
            LogicalPlan::TableScan(TableScan {
                source,
                projected_schema,
                ..
            }) => {
                assert_eq!(source.schema().fields().len(), 3);
                assert_eq!(projected_schema.fields().len(), 1);
            }
            _ => panic!("input to projection should be InMemoryScan"),
        },
        _ => panic!("expect optimized_plan to be projection"),
    }

    let expected = format!(
        "Projection: #{}.b\
         \n  TableScan: {} projection=Some([1])",
        UNNAMED_TABLE, UNNAMED_TABLE
    );
    assert_eq!(format!("{:?}", optimized_plan), expected);

    let physical_plan = ctx.create_physical_plan(&optimized_plan).await?;

    assert_eq!(1, physical_plan.schema().fields().len());
    assert_eq!("b", physical_plan.schema().field(0).name().as_str());

    let task_ctx = ctx.task_ctx();
    let batches = collect(physical_plan, task_ctx).await?;
    assert_eq!(1, batches.len());
    assert_eq!(1, batches[0].num_columns());
    assert_eq!(4, batches[0].num_rows());

    Ok(())
}

fn assert_fields_eq(plan: &LogicalPlan, expected: Vec<&str>) {
    let actual: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(actual, expected);
}
