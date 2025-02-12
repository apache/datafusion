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

use std::{fmt::Write, sync::Arc, time::Duration};

use arrow::array::{Int64Builder, RecordBatch, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTableUrl},
    },
    prelude::{SessionConfig, SessionContext},
};
use datafusion_execution::runtime_env::RuntimeEnv;
use itertools::Itertools;
use object_store::{
    memory::InMemory,
    path::Path,
    throttle::{ThrottleConfig, ThrottledStore},
    ObjectStore,
};
use parquet::arrow::ArrowWriter;
use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::runtime::Runtime;
use url::Url;

const THREADS: usize = 4;
const TABLES: usize = 3;
const TABLE_PARTITIONS: usize = 10;
const PARTITION_FILES: usize = 2;
const FILE_ROWS: usize = 1_000_000;
const RNG_SEED: u64 = 1234;
const STORE_LATENCY: Duration = Duration::from_millis(100);

fn table_name(table_id: usize) -> String {
    format!("table_{table_id}")
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("payload", DataType::Int64, false),
    ]))
}

fn create_parquet_file(rng: &mut StdRng, id_offset: usize) -> Bytes {
    let schema = schema();
    let mut id_builder = UInt64Builder::new();
    let mut payload_builder = Int64Builder::new();
    for row in 0..FILE_ROWS {
        id_builder.append_value((row + id_offset) as u64);
        payload_builder.append_value(rng.gen());
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(payload_builder.finish()),
        ],
    )
    .unwrap();

    let mut data = Vec::<u8>::new();
    let mut writer = ArrowWriter::try_new(&mut data, schema, Default::default()).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    data.into()
}

async fn setup_files(store: Arc<dyn ObjectStore>) {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    for table_id in 0..TABLES {
        let table_name = table_name(table_id);
        for partition in 0..TABLE_PARTITIONS {
            for file in 0..PARTITION_FILES {
                let data = create_parquet_file(&mut rng, file * FILE_ROWS);
                let location = Path::from(format!(
                    "{table_name}/partition={partition}/{file}.parquet"
                ));
                store.put(&location, data.into()).await.unwrap();
            }
        }
    }
}

fn make_store_slow<S>(object_store: S) -> Arc<dyn ObjectStore>
where
    S: ObjectStore,
{
    let config = ThrottleConfig {
        wait_get_per_call: STORE_LATENCY,
        wait_list_per_call: STORE_LATENCY,
        wait_list_with_delimiter_per_call: STORE_LATENCY,
        ..Default::default()
    };
    Arc::new(ThrottledStore::new(object_store, config))
}

async fn setup_context(object_store: Arc<dyn ObjectStore>) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(THREADS);
    let rt = Arc::new(RuntimeEnv::default());
    rt.register_object_store(&Url::parse("data://my_store").unwrap(), object_store);
    let context = SessionContext::new_with_config_rt(config, rt);

    for table_id in 0..TABLES {
        let table_name = table_name(table_id);
        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let options = ListingOptions::new(Arc::new(file_format))
            .with_table_partition_cols(vec![(String::from("partition"), DataType::UInt8)])
            .with_target_partitions(THREADS);

        // make sure we actually find the data
        let path = format!("data://my_store/{table_name}/");
        let schema2 = options
            .infer_schema(&context.state(), &ListingTableUrl::parse(&path).unwrap())
            .await
            .unwrap();
        assert_eq!(schema2, schema());

        context
            .register_listing_table(&table_name, &path, options, Some(schema2), None)
            .await
            .unwrap();
    }

    context
}

async fn bench(ctx: &SessionContext, query: &str, output_rows: Option<usize>) {
    let df = ctx.sql(query).await.unwrap();

    let batches = df.collect().await.unwrap();
    if let Some(expected) = output_rows {
        let actual = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(actual, expected);
    }
}

fn bench_query(
    c: &mut Criterion,
    ctx: &SessionContext,
    rt: &Runtime,
    group_name: &str,
    query: &str,
    output_rows: usize,
) {
    let mut g = c.benchmark_group(group_name);
    g.sampling_mode(SamplingMode::Flat);
    g.sample_size(10);
    g.measurement_time(Duration::from_secs(10));
    g.bench_function("explain", |b| {
        let query = format!("EXPLAIN {query}");
        b.to_async(rt).iter(|| bench(ctx, &query, None));
    });
    g.bench_function("run", |b| {
        b.to_async(rt).iter(|| bench(ctx, query, Some(output_rows)));
    });
    g.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    env_logger::try_init().ok();
    let store = make_store_slow(InMemory::new());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(THREADS)
        .enable_time()
        .build()
        .unwrap();

    rt.block_on(setup_files(Arc::clone(&store)));
    let ctx = rt.block_on(setup_context(store));

    let table0_name = table_name(0);
    let select_query = format!("SELECT * FROM {table0_name}");
    bench_query(
        c,
        &ctx,
        &rt,
        "IO: SELECT, one table, all partitions",
        &select_query,
        TABLE_PARTITIONS * PARTITION_FILES * FILE_ROWS,
    );
    bench_query(
        c,
        &ctx,
        &rt,
        "IO: SELECT, one table, single partition",
        &format!("{select_query} WHERE partition = 0"),
        PARTITION_FILES * FILE_ROWS,
    );

    let union_query = (0..TABLES)
        .map(|table_id| format!("(SELECT * FROM {})", table_name(table_id)))
        .join("UNION ALL");
    bench_query(
        c,
        &ctx,
        &rt,
        "IO: UNION ALL, all tables, all partitions",
        &union_query,
        TABLES * TABLE_PARTITIONS * PARTITION_FILES * FILE_ROWS,
    );
    bench_query(
        c,
        &ctx,
        &rt,
        "IO: UNION ALL, all tables, single partitions",
        &union_query.replace(')', " WHERE partition = 0)"),
        TABLES * PARTITION_FILES * FILE_ROWS,
    );

    let mut join_query = "SELECT * FROM".to_owned();
    for table_id in 0..TABLES {
        let table_name = table_name(table_id);
        if table_id == 0 {
            write!(join_query, " {table_name}").unwrap();
        } else {
            write!(
                join_query,
                " INNER JOIN {table_name} on {table_name}.id = {table0_name}.id AND {table_name}.partition = {table0_name}.partition",
            ).unwrap();
        }
    }
    bench_query(
        c,
        &ctx,
        &rt,
        "IO: INNER JOIN, all tables, all partitions",
        &join_query,
        TABLE_PARTITIONS * PARTITION_FILES * FILE_ROWS,
    );
    bench_query(
        c,
        &ctx,
        &rt,
        "IO: INNER JOIN, all tables, single partition",
        &format!("{join_query} WHERE {table0_name}.partition = 0"),
        PARTITION_FILES * FILE_ROWS,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
