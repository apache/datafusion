use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[test]
fn foo() {
    let file_path = "/tmp/parquet_query_sqlfeBpC5.parquet";
    assert!(Path::new(file_path).exists(), "path not found");
    println!("Using parquet file {}", file_path);

    let mut context = SessionContext::new();

    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    rt.block_on(context.register_parquet("t", file_path))
        .unwrap();

    // We read the queries from a file so they can be changed without recompiling the benchmark
    let mut queries_file = File::open("benches/parquet_query_sql.sql").unwrap();
    let mut queries = String::new();
    queries_file.read_to_string(&mut queries).unwrap();

    for query in queries.split(';') {
        let query = query.trim();

        // Remove comment lines
        let query: Vec<_> = query.split('\n').filter(|x| !x.starts_with("--")).collect();
        let query = query.join(" ");

        // Ignore blank lines
        if query.is_empty() {
            continue;
        }

        let query = query.as_str();

        let mut context = context.clone();
        rt.block_on(async move {
            let query = context.sql(query).await.unwrap();
            let mut stream = query.execute_stream().await.unwrap();
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                assert!(batch.num_rows() != 0);
            }
        })
    }
}
