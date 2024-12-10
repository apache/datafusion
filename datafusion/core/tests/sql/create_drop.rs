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

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::test_util::TestTableFactory;

use super::*;

#[tokio::test]
async fn create_custom_table() -> Result<()> {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(TestTableFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://bucket/schema/table';";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();
    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    Ok(())
}

#[tokio::test]
async fn create_external_table_with_ddl() -> Result<()> {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state
        .table_factories_mut()
        .insert("MOCKTABLE".to_string(), Arc::new(TestTableFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL TABLE dt (a_id integer, a_str string, a_bool boolean) STORED AS MOCKTABLE LOCATION 'mockprotocol://path/to/table';";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();

    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    let table_schema = schema.table("dt").await.unwrap().unwrap().schema();

    assert_eq!(3, table_schema.fields().len());

    assert_eq!(&DataType::Int32, table_schema.field(0).data_type());
    assert_eq!(&DataType::Utf8, table_schema.field(1).data_type());
    assert_eq!(&DataType::Boolean, table_schema.field(2).data_type());

    Ok(())
}

// TODO:
#[tokio::test]
async fn test_table_target_partitions() -> Result<()> {
    use arrow::util::pretty::pretty_format_batches;

    let runtime_env = datafusion_execution::runtime_env::RuntimeEnvBuilder::new();
    let runtime_env = runtime_env
        .with_memory_pool(Arc::new(
            datafusion_execution::memory_pool::GreedyMemoryPool::new(2 * 1024 * 1024),
        ))
        .build();
    let session_config = SessionConfig::from_env()?
        .with_information_schema(true)
        .with_feature_flag(true);

    let ctx = SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env?));
    let sql = "set datafusion.execution.target_partitions = 2;";
    let _ = ctx.sql(sql).await?.collect().await?;

    let sql = "create table t(a int, b varchar) as values 
        (1, 'a'),
        (1, 'a'),
        (1, 'a'),
        (2, 'b'),
        (2, 'b'),
        (2, 'b');
        ";
    let _ = ctx.sql(sql).await?.collect().await?;

    let sql = "explain select b, sum(DISTINCT a) from T group by b;";
    let actual = ctx.sql(sql).await?.collect().await?;
    println!("{}", pretty_format_batches(&actual)?);
    // ---- sql::create_drop::test_table_target_partitions stdout ----
    // +---------------+----------------------------------------------------------------------------------------------------+
    // | plan_type     | plan                                                                                               |
    // +---------------+----------------------------------------------------------------------------------------------------+
    // | logical_plan  | Projection: t.b, sum(alias1) AS sum(DISTINCT t.a)                                                  |
    // |               |   Aggregate: groupBy=[[t.b]], aggr=[[sum(alias1)]]                                                 |
    // |               |     Aggregate: groupBy=[[t.b, CAST(t.a AS Int64) AS alias1]], aggr=[[]]                            |
    // |               |       TableScan: t projection=[a, b]                                                               |
    // | physical_plan | ProjectionExec: expr=[b@0 as b, sum(alias1)@1 as sum(DISTINCT t.a)]                                |
    // |               |   AggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[sum(alias1)]                         |
    // |               |     CoalesceBatchesExec: target_batch_size=8192                                                    |
    // |               |       RepartitionExec: partitioning=Hash([b@0], 2), input_partitions=2                             |
    // |               |         AggregateExec: mode=Partial, gby=[b@0 as b], aggr=[sum(alias1)]                            |
    // |               |           AggregateExec: mode=FinalPartitioned, gby=[b@0 as b, alias1@1 as alias1], aggr=[]        |
    // |               |             CoalesceBatchesExec: target_batch_size=8192                                            |
    // |               |               RepartitionExec: partitioning=Hash([b@0, alias1@1], 2), input_partitions=2           |
    // |               |                 AggregateExec: mode=Partial, gby=[b@1 as b, CAST(a@0 AS Int64) as alias1], aggr=[] |
    // |               |                   MemoryExec: partitions=2, partition_sizes=[1, 1]                                 |
    // |               |                                                                                                    |
    // +---------------+----------------------------------------------------------------------------------------------------+

    // let sql = "select b, sum(DISTINCT a) from T group by b;";
    // let actual = ctx.sql(sql).await?.collect().await?;
    // println!("{}", pretty_format_batches(&actual)?);

    // let sql = "select * from T;";
    // let actual = ctx.sql(sql).await?.collect().await?;
    // println!("{}", pretty_format_batches(&actual)?);
    println!(
        "============================================================================="
    );

    Ok(())
}
