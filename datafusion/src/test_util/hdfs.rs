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

//! It's a utility for testing with data source in hdfs.
//! The hdfs data source will be prepared by copying local parquet file.
//! Users don't need to care about the trial things, like data moving, data cleaning up, etc.
//! ```ignore
//! use crate::test::hdfs::run_hdfs_test;
//!
//! #[tokio::test]
//! async fn test_data_on_hdfs() -> Result<()> {
//!     run_hdfs_test("alltypes_plain.parquet".to_string(), |filename_hdfs| {
//!         Box::pin(async move {
//!             ...
//!         })
//!     })
//!     .await
//! }
//! ```

use std::pin::Pin;

use futures::Future;
use hdfs::minidfs;
use hdfs::util::HdfsUtil;
use uuid::Uuid;

use crate::datasource::object_store::hdfs::HadoopFileSystem;
use crate::error::Result;

/// Run test after related data prepared
pub async fn run_hdfs_test<F>(filename: String, test: F) -> Result<()>
where
    F: FnOnce(
        HadoopFileSystem,
        String,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>,
{
    let (hdfs, tmp_dir, dst_file) = setup_with_hdfs_data(&filename);

    let result = test(hdfs, dst_file).await;

    teardown(&tmp_dir);

    result
}

/// Prepare hdfs parquet file by copying local parquet file to hdfs
fn setup_with_hdfs_data(filename: &str) -> (HadoopFileSystem, String, String) {
    let uuid = Uuid::new_v4().to_string();
    let tmp_dir = format!("/{}", uuid);

    let dfs = minidfs::get_dfs();
    let fs = dfs.get_hdfs().ok().unwrap();
    assert!(fs.mkdir(&tmp_dir).is_ok());

    // Source
    let testdata = crate::test_util::parquet_test_data();
    let src_path = format!("{}/{}", testdata, filename);

    // Destination
    let dst_path = format!("{}/{}", tmp_dir, filename);

    // Copy to hdfs
    assert!(HdfsUtil::copy_file_to_hdfs(dfs.clone(), &src_path, &dst_path).is_ok());

    (
        HadoopFileSystem::wrap(fs),
        tmp_dir,
        format!("{}{}", dfs.namenode_addr(), dst_path),
    )
}

/// Cleanup testing files in hdfs
fn teardown(tmp_dir: &str) {
    let dfs = minidfs::get_dfs();
    let fs = dfs.get_hdfs().ok().unwrap();
    assert!(fs.delete(&tmp_dir, true).is_ok());
}
