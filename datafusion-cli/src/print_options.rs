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

use crate::print_format::PrintFormat;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct PrintOptions {
    pub format: PrintFormat,
    pub quiet: bool,
}

fn print_timing_info(row_count: usize, now: Instant) {
    println!(
        "{} {} in set. Query took {:.3} seconds.",
        row_count,
        if row_count == 1 { "row" } else { "rows" },
        now.elapsed().as_secs_f64()
    );
}

impl PrintOptions {
    /// print the batches to stdout using the specified format
    pub fn print_batches(&self, batches: &[RecordBatch], now: Instant) -> Result<()> {
        if batches.is_empty() {
            if !self.quiet {
                print_timing_info(0, now);
            }
        } else {
            self.format.print_batches(batches)?;
            if !self.quiet {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                print_timing_info(row_count, now);
            }
        }
        Ok(())
    }
}
