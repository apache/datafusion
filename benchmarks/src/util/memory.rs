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

/// Print Peak RSS, Peak Commit, Page Faults based on mimalloc api, followed by
/// the peak `MemoryPool` reservation when a memory limit was configured.
pub fn print_memory_stats() {
    print_allocator_stats();
    print_pool_stats();
}

/// Print the peak `MemoryPool` reservation for the run.
///
/// Prints nothing when the benchmark ran without a memory limit, since no pool
/// was installed to record. Comparing this against the peak RSS above shows how
/// much of a run's memory the pool actually accounted for — DataFusion only
/// tracks the "large" allocations that scale with input size, so the two are
/// expected to differ.
fn print_pool_stats() {
    if let Some(peak) = super::max_pool_reserved() {
        println!(
            "Peak pool reserved: {}",
            datafusion_common::human_readable_size(peak)
        );
    }
}

fn print_allocator_stats() {
    #[cfg(all(feature = "mimalloc", feature = "mimalloc_extended"))]
    {
        use datafusion_common::human_readable_size;
        let mut peak_rss = 0;
        let mut peak_commit = 0;
        let mut page_faults = 0;
        unsafe {
            libmimalloc_sys::mi_process_info(
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut peak_rss,
                std::ptr::null_mut(),
                &mut peak_commit,
                &mut page_faults,
            );
        }

        // When modifying this output format, make sure to update the corresponding
        // parsers in `mem_profile.rs`, specifically `parse_vm_line` and `parse_query_time`,
        // to keep the log output and parser logic in sync.
        println!(
            "Peak RSS: {}, Peak Commit: {}, Page Faults: {}",
            if peak_rss == 0 {
                "N/A".to_string()
            } else {
                human_readable_size(peak_rss)
            },
            if peak_commit == 0 {
                "N/A".to_string()
            } else {
                human_readable_size(peak_commit)
            },
            page_faults
        );
    }
}
