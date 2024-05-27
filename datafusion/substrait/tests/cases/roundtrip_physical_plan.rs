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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::error::Result;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_substrait::physical_plan::{consumer, producer};

use substrait::proto::extensions;

#[tokio::test]
async fn parquet_exec() -> Result<()> {
    let scan_config = FileScanConfig::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(Schema::empty()),
    )
    .with_file_groups(vec![
        vec![PartitionedFile::new(
            "file://foo/part-0.parquet".to_string(),
            123,
        )],
        vec![PartitionedFile::new(
            "file://foo/part-1.parquet".to_string(),
            123,
        )],
    ]);
    let parquet_exec: Arc<dyn ExecutionPlan> = Arc::new(ParquetExec::new(
        scan_config,
        None,
        None,
        Default::default(),
    ));

    let mut extension_info: (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ) = (vec![], HashMap::new());

    let substrait_rel =
        producer::to_substrait_rel(parquet_exec.as_ref(), &mut extension_info)?;

    let ctx = SessionContext::new();

    let parquet_exec_roundtrip =
        consumer::from_substrait_rel(&ctx, substrait_rel.as_ref(), &HashMap::new())
            .await?;

    let expected = format!("{}", displayable(parquet_exec.as_ref()).indent(true));
    let actual = format!(
        "{}",
        displayable(parquet_exec_roundtrip.as_ref()).indent(true)
    );
    assert_eq!(expected, actual);

    Ok(())
}
