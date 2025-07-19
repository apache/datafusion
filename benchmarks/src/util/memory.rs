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

use datafusion::execution::memory_pool::human_readable_size;

#[derive(Debug)]
pub struct MemoryStats {
    pub vm_rss_kb: Option<u64>,
    pub vm_hwm_kb: Option<u64>,
    pub vm_size_kb: Option<u64>,
    pub vm_peak_kb: Option<u64>,
}

pub fn print_memory_stats() {
    #[cfg(target_os = "linux")]
    {
        use procfs::process::Process;

        let pid = std::process::id();
        let process = Process::new(pid as i32).unwrap();
        let statm = process.statm().unwrap();
        let status = process.status().unwrap();
        let page_size = procfs::page_size();

        let resident_bytes = (statm.resident * page_size) as usize;
        let vmpeak_bytes = status.vmpeak.map(|kb| (kb * 1024) as usize);
        let vmhwm_bytes = status.vmhwm.map(|kb| (kb * 1024) as usize);

        println!(
            "VmPeak: {}, VmHWM: {}, RSS: {}",
            vmpeak_bytes
                .map(human_readable_size)
                .unwrap_or_else(|| "N/A".to_string()),
            vmhwm_bytes
                .map(human_readable_size)
                .unwrap_or_else(|| "N/A".to_string()),
            human_readable_size(resident_bytes)
        );
    }
}
