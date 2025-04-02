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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::TableProvider;
    use datafusion_common::config::ConfigOptions;
    use datafusion_datasource::ListingTableUrl;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_datasource_parquet::ParquetFormat;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_plan::ExecutionPlan;

    async fn generate_listing_table_with_statistics() -> Arc<dyn ExecutionPlan> {
        let testdata = datafusion::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_tiny_pages.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();
        let opt = ListingOptions::new(Arc::new(ParquetFormat::default())).with_collect_stat(true);
        let rt = RuntimeEnvBuilder::new()
            .build_arc()
            .expect("could not build runtime environment");

        let state = SessionContext::new_with_config_rt(SessionConfig::default(), rt).state();
        let schema = opt
            .infer_schema(
                &SessionStateBuilder::new().with_default_features().build(),
                &table_path,
            )
            .await
            .unwrap();
        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(opt.clone())
            .with_schema(schema);
        let table = ListingTable::try_new(config).unwrap();
        let res=  table.scan(&state, None, &[], None).await.unwrap();
        dbg!(&res.statistics().unwrap());
        dbg!(&res.statistics_by_partition().unwrap());
        let mut config = ConfigOptions::new();
        config.set("datafusion.optimizer.repartition_file_min_size", "10").unwrap();
        let res = res.repartitioned(5, &config).unwrap().unwrap();
        dbg!(&res.statistics_by_partition().unwrap());
        res
    }

    #[tokio::test]
    async fn test_statistics_by_partition_of_data_source() -> datafusion_common::Result<()> {
        generate_listing_table_with_statistics().await;
        Ok(())
    }

    #[test]
    fn test_statistics_by_partition_of_projection() -> datafusion_common::Result<()> {
        Ok(())
    }

    #[test]
    fn test_statistics_by_partition_of_sort() -> datafusion_common::Result<()> {
        Ok(())
    }

    #[test]
    fn test_statistics_by_partition_of_filter() -> datafusion_common::Result<()> {
        Ok(())
    }

    #[test]
    fn test_statistics_by_partition_of_aggregate() -> datafusion_common::Result<()> {
        Ok(())
    }

    #[test]
    fn test_statistic_by_partition_of_cross_join() -> datafusion_common::Result<()> {
        Ok(())
    }

    #[test]
    fn test_statistic_by_partition_of_union() -> datafusion_common::Result<()> {
        Ok(())
    }

    #[test]
    fn test_statistic_by_partition_of_smp() -> datafusion_common::Result<()> {
        Ok(())
    }

    #[test]
    fn test_statistic_by_partition_of_limit() -> datafusion_common::Result<()> {
        Ok(())
    }

    #[test]
    fn test_statistic_by_partition_of_coalesce() -> datafusion_common::Result<()> {
        Ok(())
    }

}