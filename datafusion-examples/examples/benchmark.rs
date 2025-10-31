use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::{
    DefaultFileStatisticsCache, DefaultFilesMetadataCache,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::future::join_all;
use std::env;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    println!("Program path: {}", &args[0]);
    println!("Arguments: {:?}", &args[1..]);

    let metadata_cache = Arc::new(DefaultFilesMetadataCache::new(10737418240));
    let cache = Arc::new(DefaultFileStatisticsCache::default());

    let tasks: Vec<_> = (0..10).map(|i| {
        let cache_option = args[1].clone();
        let metadata_cache = metadata_cache.clone();
        let cache = cache.clone();

        tokio::spawn(async move {
            let mut cache_manager_config = CacheManagerConfig::default();
            if cache_option.contains("metadata_cache") {
                println!("Adding metadata cache");
                cache_manager_config = cache_manager_config.with_file_metadata_cache(Some(metadata_cache));
            }
            if cache_option.contains("statistics_cache") {
                println!("Adding statistics cache");
                cache_manager_config = cache_manager_config.with_files_statistics_cache(Some(cache));
            }

            let rt_builder = RuntimeEnvBuilder::new()
                .with_cache_manager(cache_manager_config).build().expect("Expected the runtime to be created");

            let mut sessionConfig = SessionConfig::default();
            let config_options = sessionConfig.options_mut();
            //config_options.execution.target_partitions = 1;

            let ctx = SessionContext::new_with_config_rt(sessionConfig, Arc::new(rt_builder));
            let query = "EXPLAIN SELECT SUM(\"AdvEngineID\"), COUNT(*), AVG(\"ResolutionWidth\") FROM hits";

            ctx.register_parquet("hits", "/Users/abandeji/Public/workplace/datafusion/datafusion-examples/resources/hits.parquet", ParquetReadOptions::default()).await.unwrap();
            let cache_enabled = ctx.runtime_env().cache_manager.get_file_metadata_cache();
            println!("Cache enabled: {:?}", cache_enabled);

            println!("Thread {} starting", i);
            for j in 0..100 {
                let timer = Instant::now();
                let df = ctx
                    .sql(query)
                    .await.unwrap();

                df.to_string().await.unwrap();

                let time_taken = timer.elapsed().as_millis();
                println!("Time Taken: {}", time_taken);
            }
            println!("Thread {} completed", i);
        })
    }).collect();

    join_all(tasks).await;
}
