<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion + Conbench Integration


## Quick start

```
$ cd ~/arrow-datafusion/conbench/
$ conda create -y -n conbench python=3.9
$ conda activate conbench
(conbench) $ pip install -r requirements.txt
(conbench) $ conbench datafusion
```

## Example output

```
{
    "batch_id": "3c82f9d23fce49328b78ba9fd963b254",
    "context": {
        "benchmark_language": "Rust"
    },
    "github": {
        "commit": "e8c198b9fac6cd8822b950b9f71898e47965488d",
        "repository": "https://github.com/dianaclarke/arrow-datafusion"
    },
    "info": {},
    "machine_info": {
        "architecture_name": "x86_64",
        "cpu_core_count": "8",
        "cpu_frequency_max_hz": "2400000000",
        "cpu_l1d_cache_bytes": "65536",
        "cpu_l1i_cache_bytes": "131072",
        "cpu_l2_cache_bytes": "4194304",
        "cpu_l3_cache_bytes": "0",
        "cpu_model_name": "Apple M1",
        "cpu_thread_count": "8",
        "gpu_count": "0",
        "gpu_product_names": [],
        "kernel_name": "20.6.0",
        "memory_bytes": "17179869184",
        "name": "diana",
        "os_name": "macOS",
        "os_version": "10.16"
    },
    "run_id": "ec2a50b9380c470b96d7eb7d63ab5b77",
    "stats": {
        "data": [
            "0.001532",
            "0.001394",
            "0.001333",
            "0.001356",
            "0.001379",
            "0.001361",
            "0.001307",
            "0.001348",
            "0.001436",
            "0.001397",
            "0.001339",
            "0.001523",
            "0.001593",
            "0.001415",
            "0.001344",
            "0.001312",
            "0.001402",
            "0.001362",
            "0.001329",
            "0.001330",
            "0.001447",
            "0.001413",
            "0.001536",
            "0.001330",
            "0.001333",
            "0.001338",
            "0.001333",
            "0.001331",
            "0.001426",
            "0.001575",
            "0.001362",
            "0.001343",
            "0.001334",
            "0.001383",
            "0.001476",
            "0.001356",
            "0.001362",
            "0.001334",
            "0.001390",
            "0.001497",
            "0.001330",
            "0.001347",
            "0.001331",
            "0.001468",
            "0.001377",
            "0.001351",
            "0.001328",
            "0.001509",
            "0.001338",
            "0.001355",
            "0.001332",
            "0.001485",
            "0.001370",
            "0.001366",
            "0.001507",
            "0.001358",
            "0.001331",
            "0.001463",
            "0.001362",
            "0.001336",
            "0.001428",
            "0.001343",
            "0.001359",
            "0.001905",
            "0.001726",
            "0.001411",
            "0.001433",
            "0.001391",
            "0.001453",
            "0.001346",
            "0.001339",
            "0.001420",
            "0.001330",
            "0.001422",
            "0.001683",
            "0.001426",
            "0.001349",
            "0.001342",
            "0.001430",
            "0.001330",
            "0.001436",
            "0.001331",
            "0.001415",
            "0.001332",
            "0.001408",
            "0.001343",
            "0.001392",
            "0.001371",
            "0.001655",
            "0.001354",
            "0.001438",
            "0.001347",
            "0.001341",
            "0.001374",
            "0.001453",
            "0.001352",
            "0.001358",
            "0.001398",
            "0.001362",
            "0.001454"
        ],
        "iqr": "0.000088",
        "iterations": 100,
        "max": "0.001905",
        "mean": "0.001401",
        "median": "0.001362",
        "min": "0.001307",
        "q1": "0.001340",
        "q3": "0.001428",
        "stdev": "0.000095",
        "time_unit": "s",
        "times": [],
        "unit": "s"
    },
    "tags": {
        "name": "aggregate_query_group_by",
        "suite": "aggregate_query_group_by"
    },
    "timestamp": "2022-02-09T01:32:55.769468+00:00"
}
```

## Debug with test benchmark

```
(conbench) $ cd ~/arrow-datafusion/conbench/
(conbench) $ conbench test --iterations=3

Benchmark result:
{
    "batch_id": "41a144761bc24d82b94efa70d6e460b3",
    "context": {
        "benchmark_language": "Python"
    },
    "github": {
        "commit": "e8c198b9fac6cd8822b950b9f71898e47965488d",
        "repository": "https://github.com/dianaclarke/arrow-datafusion"
    },
    "info": {
        "benchmark_language_version": "Python 3.9.7"
    },
    "machine_info": {
        "architecture_name": "x86_64",
        "cpu_core_count": "8",
        "cpu_frequency_max_hz": "2400000000",
        "cpu_l1d_cache_bytes": "65536",
        "cpu_l1i_cache_bytes": "131072",
        "cpu_l2_cache_bytes": "4194304",
        "cpu_l3_cache_bytes": "0",
        "cpu_model_name": "Apple M1",
        "cpu_thread_count": "8",
        "gpu_count": "0",
        "gpu_product_names": [],
        "kernel_name": "20.6.0",
        "memory_bytes": "17179869184",
        "name": "diana",
        "os_name": "macOS",
        "os_version": "10.16"
    },
    "run_id": "71f46362db8844afacea82cba119cefc",
    "stats": {
        "data": [
            "0.000001",
            "0.000001",
            "0.000000"
        ],
        "iqr": "0.000000",
        "iterations": 3,
        "max": "0.000001",
        "mean": "0.000001",
        "median": "0.000001",
        "min": "0.000000",
        "q1": "0.000000",
        "q3": "0.000001",
        "stdev": "0.000001",
        "time_unit": "s",
        "times": [],
        "unit": "s"
    },
    "tags": {
        "name": "test"
    },
    "timestamp": "2022-02-09T01:36:45.823615+00:00"
}
```

