// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The settings each query runs under. Both tables always use the same one.

use std::sync::Arc;

use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;

use super::data::Dataset;

#[derive(Debug, Clone, Copy)]
pub struct Setting {
    pub target_partitions: usize,
    pub batch_size: usize,
    pub prefer_hash_join: bool,
    /// Whether zeros in the `raw` query are written as `-0.0`.
    pub negative_literals: bool,
}

impl Setting {
    /// Every combination.
    pub fn all() -> Vec<Setting> {
        let mut settings = vec![];
        for target_partitions in [1, 4] {
            for batch_size in [1, 8192] {
                for prefer_hash_join in [true, false] {
                    for negative_literals in [false, true] {
                        settings.push(Setting {
                            target_partitions,
                            batch_size,
                            prefer_hash_join,
                            negative_literals,
                        });
                    }
                }
            }
        }
        settings
    }

    /// How a zero is written in the `raw` query.
    pub fn raw_zero(&self) -> &'static str {
        if self.negative_literals {
            "-0.0"
        } else {
            "0.0"
        }
    }

    /// A session with both tables.
    pub fn context(&self, data: &Dataset) -> Result<SessionContext> {
        let mut config = SessionConfig::new()
            .with_target_partitions(self.target_partitions)
            .with_batch_size(self.batch_size);
        config.options_mut().optimizer.prefer_hash_join = self.prefer_hash_join;
        let ctx = SessionContext::new_with_config(config);
        for (name, partitions) in [("raw", &data.raw), ("canonical", &data.canonical)] {
            let table = MemTable::try_new(Arc::clone(&data.schema), partitions.clone())?;
            ctx.register_table(name, Arc::new(table))?;
        }
        Ok(ctx)
    }
}
