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

//! Contains a `Source` enum represents where the data comes from.

use std::{io::Read, sync::Mutex};

///  Source represents where the data comes from.
pub(crate) enum Source<R = Box<dyn Read + Send + Sync>> {
    /// The data comes from partitioned files
    PartitionedFiles {
        /// Path to directory containing partitioned files with the same schema
        path: String,
        /// The individual files under path
        filenames: Vec<String>,
    },

    /// The data comes from anything impl Read trait
    Reader(Mutex<Option<R>>),
}

impl<R> std::fmt::Debug for Source<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::PartitionedFiles { path, filenames } => f
                .debug_struct("PartitionedFiles")
                .field("path", path)
                .field("filenames", filenames)
                .finish()?,
            Source::Reader(_) => f.write_str("Reader")?,
        };
        Ok(())
    }
}
impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::PartitionedFiles { path, filenames } => {
                write!(f, "Path({}: [{}])", path, filenames.join(","))
            }
            Source::Reader(_) => {
                write!(f, "Reader(...)")
            }
        }
    }
}

impl<R> Clone for Source<R> {
    fn clone(&self) -> Self {
        match self {
            Source::PartitionedFiles { path, filenames } => Self::PartitionedFiles {
                path: path.clone(),
                filenames: filenames.clone(),
            },
            Source::Reader(_) => Self::Reader(Mutex::new(None)),
        }
    }
}

impl<R> Source<R> {
    /// Path to directory containing partitioned files with the same schema
    pub fn path(&self) -> &str {
        match self {
            Source::PartitionedFiles { path, .. } => path.as_str(),
            Source::Reader(_) => "",
        }
    }

    /// The individual files under path
    pub fn filenames(&self) -> &[String] {
        match self {
            Source::PartitionedFiles { filenames, .. } => filenames,
            Source::Reader(_) => &[],
        }
    }
}
