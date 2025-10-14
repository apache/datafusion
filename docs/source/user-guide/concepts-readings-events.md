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

# Concepts, Readings, Events

## 🧭 Background Concepts

- **2024-06-13**: [2024 ACM SIGMOD International Conference on Management of Data: Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine](https://dl.acm.org/doi/10.1145/3626246.3653368) - [Download](http://andrew.nerdnetworks.org/other/SIGMOD-2024-lamb.pdf), [Talk](https://youtu.be/-DpKcPfnNms), [Slides](https://docs.google.com/presentation/d/1gqcxSNLGVwaqN0_yJtCbNm19-w5pqPuktII5_EDA6_k/edit#slide=id.p), [Recording ](https://youtu.be/-DpKcPfnNms)

- **2024-06-07**: [Video: SIGMOD 2024 Practice: Apache Arrow DataFusion A Fast, Embeddable, Modular Analytic Query Engine](https://www.youtube.com/watch?v=-DpKcPfnNms&t=5s) - [Slides](https://docs.google.com/presentation/d/1gqcxSNLGVwaqN0_yJtCbNm19-w5pqPuktII5_EDA6_k/edit#slide=id.p)

- **2023-04-05**: [Video: DataFusion Architecture Part 3: Physical Plan and Execution](https://youtu.be/2jkWU3_w6z0) - [Slides](https://docs.google.com/presentation/d/1cA2WQJ2qg6tx6y4Wf8FH2WVSm9JQ5UgmBWATHdik0hg)

- **2023-04-04**: [Video: DataFusion Architecture Part 2: Logical Plans and Expressions](https://youtu.be/EzZTLiSJnhY) - [Slides](https://docs.google.com/presentation/d/1ypylM3-w60kVDW7Q6S99AHzvlBgciTdjsAfqNP85K30)

- **2023-03-31**: [Video: DataFusion Architecture Part 1: Query Engines](https://youtu.be/NVKujPxwSBA) - [Slides](https://docs.google.com/presentation/d/1D3GDVas-8y0sA4c8EOgdCvEjVND4s2E7I6zfs67Y4j8)

- **2020-02-27**: [Online Book: How Query Engines Work](https://andygrove.io/2020/02/how-query-engines-work/)

## ✨ Good Reads

This is a list of DataFusion related blog posts, articles, and other resources. Please open a PR to add any new resources you create or find

- **2025-03-21** [Blog: Efficient Filter Pushdown in Parquet](https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/)

- **2025-03-20** [Blog: Parquet Pruning in DataFusion: Read Only What Matters](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/)

- **2025-02-12** [Video: Alex Kesling on Apache Arrow DataFusion - Papers We Love NYC ](https://www.youtube.com/watch?v=6A4vFRpSq3k)

- **2025-01-30** [Video: Data & Drinks: Building Next-Gen Data Systems with Apache DataFusion](https://www.youtube.com/watch?v=GruBeVDoWq4)

- **2024-11-22** [Blog: Apache Datafusion Comet and the story of my first contribution to it](https://semyonsinchenko.github.io/ssinchenko/post/comet-first-contribution/)

- **2024-11-21** [Blog: DataFusion is featured as one of the coolest 10 open source software tools by CRN](https://www.crn.com/news/software/2024/the-10-coolest-open-source-software-tools-of-2024?page=3)

- **2024-11-20** [Blog: Apache DataFusion Comet 0.4.0 Release](https://datafusion.apache.org/blog/2024/11/20/datafusion-comet-0.4.0/)

- **2024-11-19** [Blog: Comparing approaches to User Defined Functions in Apache DataFusion using Python](https://datafusion.apache.org/blog/2024/11/19/datafusion-python-udf-comparisons/)

- **2024-11-18** [Blog: Apache DataFusion is now the fastest single node engine for querying Apache Parquet files](https://datafusion.apache.org/blog/2024/11/18/datafusion-fastest-single-node-parquet-clickbench/)

- **2024-11-18** [Blog: Building Databases over a Weekend](https://www.denormalized.io/blog/building-databases)

- **2024-10-29** [Video: MiDAS Seminar Fall 2024 on "Apache DataFusion" by Andrew Lamb](https://www.youtube.com/watch?v=CpnxuBwHbUc)

- **2024-10-27** [Blog: Caching in DataFusion: Don't read twice](https://blog.haoxp.xyz/posts/caching-datafusion)

- **2024-10-24** [Blog: Parquet pruning in DataFusion: Read no more than you need](https://blog.haoxp.xyz/posts/parquet-to-arrow/)

- **2024-09-13** [Blog: Using StringView / German Style Strings to make Queries Faster: Part 2 - String Operations](https://www.influxdata.com/blog/faster-queries-with-stringview-part-two-influxdb/) | [Reposted on DataFusion Blog](https://datafusion.apache.org/blog/2024/09/13/string-view-german-style-strings-part-2/)

- **2024-09-13** [Blog: Using StringView / German Style Strings to Make Queries Faster: Part 1- Reading Parquet](https://www.influxdata.com/blog/faster-queries-with-stringview-part-one-influxdb/) | [Reposted on Datafusion Blog](https://datafusion.apache.org/blog/2024/09/13/string-view-german-style-strings-part-1/)

- **2024-10-16** [Blog: Candle Image Segmentation](https://www.letsql.com/posts/candle-image-segmentation/)

- **2024-09-23 → 2024-12-02** [Talks: Carnegie Mellon University: Database Building Blocks Seminar Series - Fall 2024](https://db.cs.cmu.edu/seminar2024/)

  - **2024-11-12** [Video: Building InfluxDB 3.0 with the FDAP Stack: Apache Flight, DataFusion, Arrow and Parquet (Paul Dix)](https://www.youtube.com/watch?v=AGS4GNGDK_4)

  - **2024-11-04** [Video: Synnada: Towards “Unified” Compute Engines: Opportunities and Challenges (Mehmet Ozan Kabak)](https://www.youtube.com/watch?v=z38WY9uZtt4)
  - **2024-10-28** [Video: Exon: A Built for Purpose Bioinformatics Database (Trent Hauck)](https://www.youtube.com/watch?v=fltZMO8EGl0&list=PLSE8ODhjZXjZc2AdXq_Lc1JS62R48UX2L&index=6)
  - **2024-10-21** [Video: Accelerating Data and AI with Spice.ai Open-Source Software (Luke Kim)](https://www.youtube.com/watch?v=tyM-ec1lKfU&list=PLSE8ODhjZXjZc2AdXq_Lc1JS62R48UX2L&index=5)
  - **2024-10-07** [Video: ParadeDB – Postgres for Search and Analytics (Philippe Noël)](https://www.youtube.com/watch?v=Vxb8TELNM98&list=PLSE8ODhjZXjZc2AdXq_Lc1JS62R48UX2L&index=4)
  - **2024-09-30** [Video: Accelerating Apache Spark Workloads with Apache DataFusion Comet (Andy Grove)](https://www.youtube.com/watch?v=o59s0d3HE1k&list=PLSE8ODhjZXjZc2AdXq_Lc1JS62R48UX2L&index=3)
  - **2024-09-23** [Video: Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine (Andrew Lamb)](https://www.youtube.com/watch?v=iJhRbDFJjbg&list=PLSE8ODhjZXjZc2AdXq_Lc1JS62R48UX2L&index=2)

- **2024-09-17** [Video: Profiling Apache DataFusion using flamegraph](https://www.youtube.com/watch?v=2z11xtYw_xs)

- **2024-08-25** [Blog: Pydantic/logfire: We're changing database](https://github.com/pydantic/logfire/issues/408)

- **2024-08-15** [Video: Faster DataFusion with StringView - Xiangpeng Hao (Aug 15, 2024)](https://www.youtube.com/watch?v=RVLshX6fbds)

- **2024-08-14** [Blog: DataFusion @ UWheel](https://uwheel.rs/post/datafusion_uwheel/)

- **2024-06-17** [Blog: Columnar File Readers In-Depth: APIs and Fusion](https://blog.lancedb.com/columnar-file-readers-in-depth-apis-and-fusion/)

- **2024-06-14** [Talk: 2024 Simplicity in Management of Data (SiMOD): DataFusion: The Case for Building Open Data Systems (Keynote)](https://sfu-dis.github.io/simod/) - [Slides](https://docs.google.com/presentation/d/1K3EdknzkqU2LhWi_eNKXdcvNk0OEvk9AqTLqhZkPxuI/edit)

- **2024-05-29** [Blog: Query Push Down in Cube's Semantic Layer](https://cube.dev/blog/query-push-down-in-cubes-semantic-layer)

- **2024-06-26** [Talk: Microsoft Gray Systems Lab: Building InfluxDB 3.0 (and other systems)](https://www.microsoft.com/en-us/research/group/gray-systems-lab) - [Slides](https://docs.google.com/presentation/d/1a4wHZij_69drdmD32TPombQ9zSaE6l26LZ87DAz2New/edit#slide=id.p)

- **2024-04-06** [Video: 1 billion row challenge in Rust using Apache Arrow](https://www.youtube.com/watch?v=Bc55FBwuJLA)

- **2024-03-26** [Talk: DataCouncil 2024: Building InfluxDB 3.0 with Apache Arrow, DataFusion, Flight, and Parquet](https://www.datacouncil.ai/talks24/building-influxdb-30-with-apache-arrow-datafusion-flight-and-parquet?hsLang=en) - [Slides](https://docs.google.com/presentation/d/12kdYHLyH79B5__9xs3de_hZyG9geW4jC3vUpiy39VA0), [Recording](https://www.youtube.com/watch?v=I-Z7kFGsYRI)

- **2024-03-20** [Video: Profiling DataFusion with Instruments (part of XCode on Mac OSx)](https://www.youtube.com/watch?v=P3dXH61Kr5U)

- **2024-03-18** [Blog: Making Recent Value Queries Hundreds of Times Faster](https://www.influxdata.com/blog/making-recent-value-queries-hundreds-times-faster/)

- **2023-10-25** [Blog: Flight, DataFusion, Arrow, and Parquet: Using the FDAP Architecture to build InfluxDB 3.0](https://www.influxdata.com/blog/flight-datafusion-arrow-parquet-fdap-architecture-influxdb/)

- **2023-09-26** [Blog: 100x Faster Ingest with DataFusion + Better Connectivity with FlightSQL](https://www.kamu.dev/blog/2023-09-datafusion-flightsql/)

- **2023-08-15** [Blog: Running Window Query in Stream Processing](https://www.synnada.ai/blog/running-window-query-in-stream-processing)

- **2023-08-05** [Blog: Aggregating Millions of Groups Fast in Apache Arrow DataFusion](https://www.influxdata.com/blog/aggregating-millions-groups-fast-apache-arrow-datafusion/) | [DataFusion Blog](https://arrow.apache.org/blog/2023/08/05/datafusion_fast_grouping/)

- **2023-07-28** [Blog: Sliding Window Hash Join (SWHJ)](https://www.synnada.ai/blog/sliding-window-hash-join-swhj)

- **2023-07-13** [Blog: Probabilistic Data Structures in Streaming: Count-Min Sketch](https://www.synnada.ai/blog/probabilistic-data-structures-in-streaming-count-min-sketch)

- **2023-05-25** [Video: D3L2: Discussing Rust, Ballista, Ray SQL, Data Fusion with Andy Grove](https://www.youtube.com/watch?v=NEL6DluUxgw)

- **2023-02-20** [Blog: General Purpose Stream Joins via Pruning Symmetric Hash Joins](https://www.synnada.ai/blog/general-purpose-stream-joins-via-pruning-symmetric-hash-joins)

- **2023-09-27** [Slides: MIT Database Group: Implementing InfluxDB IOx](https://docs.google.com/presentation/d/1_JXxapY2jksCOm5hePK8FIjO3buDzsrBBy0jUEpJR4A)

- **2023-06-02** [Talk: Dutch Seminar on Database System Design: Implementing InfluxDB IOx](https://dsdsd.da.cwi.nl/past_talks/post_talks/Andrew-Lamb/) - [Slides](https://docs.google.com/presentation/d/1XTsO2zsHkgBCF6C0YVwk0BnhZzLBrm39oeapOBb-s9A), [Recording](https://youtu.be/Y5K2Ik2oo-8)

- **2023-02-15** [Slides: Invited Talk at Optum Labs: Building a New Time Series Database](https://docs.google.com/presentation/d/1SzqgTtSKVqpuFUDdOHhRNC3mLmJ7oyVp0OyrYwHvgPA)

- **2023-01-01** [Blog: What I Want from DataFusion 2023](https://andygrove.io/2023/01/what-i-want-from-datafusion-2023/)

- **2022-12-07** [Blog: Querying Parquet with Millisecond Latency](https://www.influxdata.com/blog/querying-parquet-millisecond-latency/)

- **2022-06-27** [Talk: DataBricks Data+AI Summit: DataFusion and Arrow](https://www.databricks.com/dataaisummit/session/datafusion-and-arrow-supercharge-your-data-analytical-tool-rusty-query-engine) - [Slides](https://docs.google.com/presentation/d/1wLORMn23RD_sQ84W2w51s-Xysly5S8F5mGXzaeJ4QWY), [Recording](https://www.databricks.com/dataaisummit/session/datafusion-and-arrow-supercharge-your-data-analytical-tool-rusty-query-engine)

- **2022-05-23** [Video: The Data Thread 2022: Apache Arrow and DataFusion](https://www.youtube.com/watch?v=rb61lVH2vYc) - [Slides](https://docs.google.com/presentation/d/1Tkjfup5z_nsrBWIO7dXscEzC5toTQCXj0IsZeO3endc)

- **2021-03-10** [Video: InfluxData Tech Talk: Query Engine Design and Rust-Based DataFusion in Apache Arrow](https://www.youtube.com/watch?v=K6eCAVEk4kU) - [Slides](https://www.Slideshare.net/influxdata/influxdb-iox-tech-talks-query-engine-design-and-the-rustbased-datafusion-in-apache-arrow-244161934)

## 📅 Release Notes & Updates

- **2025-03-24** [Apache DataFusion 46.0.0 Released](https://datafusion.apache.org/blog/2025/03/24/datafusion-46.0.0/)

- **2024-09-14** [Apache DataFusion Python 43.1.0 Released](https://datafusion.apache.org/blog/2024/12/14/datafusion-python-43.1.0/)

- **2024-08-24** [Apache DataFusion Python 40.1.0 Released, Significant usability updates](https://datafusion.apache.org/blog/2024/08/20/python-datafusion-40.0.0/)

- **2024-07-24** [DataFusion 40.0.0 Release](https://datafusion.apache.org/blog/2024/07/24/datafusion-40.0.0/)

- **2024-01-19** [DataFusion 34.0.0 Release](https://datafusion.apache.org/blog/2024/01/19/datafusion-34.0.0/)

- **2023-06-24** [DataFusion 25.0.0 Release](https://arrow.apache.org/blog/2023/06/24/datafusion-25.0.0/)

- **2023-01-19** [DataFusion 16.0.0 Release](https://arrow.apache.org/blog/2023/01/19/datafusion-16.0.0/)

- **2022-10-25** [DataFusion 13.0.0 Release](https://arrow.apache.org/blog/2022/10/25/datafusion-13.0.0/)

- **2022-05-16** [DataFusion 8.0.0 Release](https://arrow.apache.org/blog/2022/05/16/datafusion-8.0.0/)

- **2022-02-28** [DataFusion 7.0.0 Release](https://arrow.apache.org/blog/2022/02/28/datafusion-7.0.0/)

- **2021-11-19** [DataFusion 6.0.0 Release](https://arrow.apache.org/blog/2021/11/19/datafusion-6.0.0/)

- **2021-08-18** [DataFusion 5.0.0 Release](https://arrow.apache.org/blog/2021/08/18/datafusion-5.0.0/)

- **2019-09-22** [DataFusion 0.15.0 Release Notes](https://andygrove.io/2019/09/datafusion-0.15.0-release-notes/)

# 🌎 Community Events

- **2025-01-23** [Amsterdam Apache DataFusion Meetup](https://github.com/apache/datafusion/discussions/12988) - [Slides](https://github.com/apache/datafusion/discussions/12988)
- **2025-01-22** [Datadog Apache DataFusion Community Meeting](https://www.linkedin.com/posts/seshendranalla_apache-datafusion-community-meeting-2025-activity-7290384383201435648-8tqv) - [Recording](https://www.youtube.com/watch?v=ceTo2vUyRI0)
- **2025-01-15** [Boston Apache DataFusion Meetup](https://github.com/apache/datafusion/discussions/13165) - [Slides](https://docs.google.com/presentation/d/1_zBLHdqxPlhWuNK2oCA2d_hCpb6HWgHbVJBseiUXA80)
- **2024-12-18** [Chicago Apache DataFusion Meetup](https://lu.ma/eq5myc5i) - [Slides](https://github.com/apache/datafusion/discussions/12894), [Recording](https://www.youtube.com/playlist?list=PLrhIfEjaw9ilQEczOQlHyMznabtVRptyX)
- **2024-10-14** [Seattle Apache DataFusion Meetup](https://lu.ma/tnwl866b)
- **2024-09-27** [Belgrade Apache DataFusion Meetup](https://lu.ma/tmwuz4lg) - [Recap](https://github.com/apache/datafusion/discussions/11431#discussioncomment-10832070), [Slides](https://github.com/apache/datafusion/discussions/11431#discussioncomment-10826169), [Recording](https://www.youtube.com/playlist?list=PLrhIfEjaw9ilQEczOQlHyMznabtVRptyX)
- **2024-06-26** [New York City Apache DataFusion Meetup](https://lu.ma/2iwba0xm) - [Slides](https://docs.google.com/presentation/d/1dOLPAFPEMLhLv4NN6O9QSDIyyeiIySqAjky5cVgdWAE/edit#slide=id.g26bebde4fcc_3_7)
- **2024-06-25** [San Francisco Bay Area Apache DataFusion Meetup](https://lu.ma/6bphole2) - [Slides](https://docs.google.com/presentation/d/1Oz2yGllrWBkNGyiRMLr8qXTt4vmvtJWuI_weGThaZak/edit#slide=id.g26bebde4fcc_3_7), [Recording](https://www.youtube.com/playlist?list=PLrhIfEjaw9ilQEczOQlHyMznabtVRptyX)
- **2024-03-27** [Austin Apache DataFusion Meetup](https://github.com/apache/datafusion/discussions/8522) - [Slides](https://docs.google.com/presentation/d/1S51TK8waxHEJaxi_-uiSMrgQZ09m_hfaasPk5X5ExEY), [Recording](https://www.youtube.com/watch?v=q1N3pH3tFw8)
