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

use crate::datasource::physical_plan::plan_to_csv;

use super::super::options::{CsvReadOptions, ReadOptions};
use super::{DataFilePaths, DataFrame, ExecutionPlan, Result, SessionContext};

impl SessionContext {
    /// Creates a [`DataFrame`] for reading a CSV data source.
    ///
    /// For more control such as reading multiple files, you can use
    /// [`read_table`](Self::read_table) with a [`super::ListingTable`].
    ///
    /// Example usage is given below:
    ///
    /// ```
    /// use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// // You can read a single file using `read_csv`
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// // you can also read multiple files:
    /// let df = ctx.read_csv(vec!["tests/data/example.csv", "tests/data/example.csv"], CsvReadOptions::new()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_csv<P: DataFilePaths>(
        &self,
        table_paths: P,
        options: CsvReadOptions<'_>,
    ) -> Result<DataFrame> {
        self._read_type(table_paths, options).await
    }

    /// Registers a CSV file as a table which can referenced from SQL
    /// statements executed against this context.
    pub async fn register_csv(
        &self,
        name: &str,
        table_path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options.to_listing_options(&self.copied_config());

        self.register_listing_table(
            name,
            table_path,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
            None,
        )
        .await?;

        Ok(())
    }

    /// Executes a query and writes the results to a partitioned CSV file.
    pub async fn write_csv(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        plan_to_csv(self.task_ctx(), plan, path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    // Test for compilation error when calling read_* functions from an #[async_trait] function.
    // See https://github.com/apache/arrow-datafusion/issues/1154
    #[async_trait]
    trait CallReadTrait {
        async fn call_read_csv(&self) -> DataFrame;
    }

    struct CallRead {}

    #[async_trait]
    impl CallReadTrait for CallRead {
        async fn call_read_csv(&self) -> DataFrame {
            let ctx = SessionContext::new();
            ctx.read_csv("dummy", CsvReadOptions::new()).await.unwrap()
        }
    }
}
