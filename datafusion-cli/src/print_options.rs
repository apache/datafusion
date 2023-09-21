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
    pub maxrows: Option<usize>,
}

fn get_timing_info_str(
    row_count: usize,
    maxrows_opt: Option<usize>,
    query_start_time: Instant,
) -> String {
    let row_word = if row_count == 1 { "row" } else { "rows" };
    let maxrows_shown_msg = maxrows_opt
        .map(|maxrows| {
            if maxrows < row_count {
                format!(" ({} shown)", maxrows)
            } else {
                String::new()
            }
        })
        .unwrap_or_default();

    format!(
        "{} {} in set{}. Query took {:.3} seconds.\n",
        row_count,
        row_word,
        maxrows_shown_msg,
        query_start_time.elapsed().as_secs_f64()
    )
}

impl PrintOptions {
    /// print the batches to stdout using the specified format
    pub fn print_batches(
        &self,
        batches: &[RecordBatch],
        query_start_time: Instant,
    ) -> Result<()> {
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Elapsed time should not count time for printing batches
        let timing_info = get_timing_info_str(row_count, self.maxrows, query_start_time);

        self.format.print_batches(batches, self.maxrows)?;

        if !self.quiet {
            println!("{timing_info}");
        }

        Ok(())
    }
}
