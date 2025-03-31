# GSoC Project Ideas

## Introduction

Welcome to the Apache DataFusion Google Summer of Code (GSoC) 2025 project ideas list. Below you can find information about the projects. Please refer to [this page](https://datafusion.apache.org/contributor-guide/gsoc_application_guidelines.html) for application guidelines.

## Projects

### [Implement Continuous Monitoring of DataFusion Performance](https://github.com/apache/datafusion/issues/5504)

- **Description and Outcomes:** DataFusion lacks continuous monitoring of how performance evolves over time -- we do this somewhat manually today. Even though performance has been one of our top priorities for a while now, we didn't build a continuous monitoring system yet. This linked issue contains a summary of all the previous efforts that made us inch closer to having such a system, but a functioning system needs to built on top of that progress. A student successfully completing this project would gain experience in building an end-to-end monitoring system that integrates with GitHub, scheduling/running benchmarks on some sort of a cloud infrastructure, and building a versatile web UI to expose the results. The outcome of this project will benefit Apache DataFusion on an ongoing basis in its quest for ever-more performance.
- **Category:** Tooling
- **Difficulty:** Medium
- **Possible Mentor(s) and/or Helper(s):** [alamb](https://github.com/alamb) and [mertak-synnada](https://github.com/mertak-synnada)
- **Skills:** DevOps, Cloud Computing, Web Development, Integrations
- **Expected Project Size:** 175 to 350 hours\*

### [Supporting Correlated Subqueries](https://github.com/apache/datafusion/issues/5483)

- **Description and Outcomes:** Correlated subqueries are an important SQL feature that enables some users to express their business logic more intuitively without thinking about "joins". Even though DataFusion has decent join support, it doesn't fully support correlated subqueries. The linked epic contains bite-size pieces of the steps necessary to achieve full support. For students interested in internals of data systems and databases, this project is a good opportunity to apply and/or improve their computer science knowledge. The experience of adding such a feature to a widely-used foundational query engine can also serve as a good opportunity to kickstart a career in the area of databases and data systems.
- **Category:** Core
- **Difficulty:** Advanced
- **Possible Mentor(s) and/or Helper(s):** [jayzhan-synnada](https://github.com/jayzhan-synnada) and [xudong963](https://github.com/xudong963)
- **Skills:** Databases, Algorithms, Data Structures, Testing Techniques
- **Expected Project Size:** 350 hours

### Improving DataFusion DX (e.g. [1](https://github.com/apache/datafusion/issues/9371) and [2](https://github.com/apache/datafusion/issues/14429))

- **Description and Outcomes:** While performance, extensibility and customizability is DataFusion's strong aspects, we have much work to do in terms of user-friendliness and ease of debug-ability. This project aims to make strides in these areas by improving terminal visualizations of query plans and increasing the "deployment" of the newly-added diagnostics framework. This project is a potential high-impact project with high output visibility, and reduce the barrier to entry to new users.
- **Category:** DX
- **Difficulty:** Medium
- **Possible Mentor(s) and/or Helper(s):** [eliaperantoni](https://github.com/eliaperantoni) and [mkarbo](https://github.com/mkarbo)
- **Skills:** Software Engineering, Terminal Visualizations
- **Expected Project Size:** 175 to 350 hours\*

### [Robust WASM Support](https://github.com/apache/datafusion/issues/13815)

- **Description and Outcomes:** DataFusion can be compiled today to WASM with some care. However, it is somewhat tricky and brittle. Having robust WASM support improves the _embeddability_ aspect of DataFusion, and can enable many practical use cases. A good conclusion of this project would be the addition of a live demo sub-page to the DataFusion homepage.
- **Category:** Build
- **Difficulty:** Medium
- **Possible Mentor(s) and/or Helper(s):** [alamb](https://github.com/alamb) and [waynexia](https://github.com/waynexia)
- **Skills:** WASM, Advanced Rust, Web Development, Software Engineering
- **Expected Project Size:** 175 to 350 hours\*

### [High Performance Aggregations](https://github.com/apache/datafusion/issues/7000)

- **Description and Outcomes:** An aggregation is one of the most fundamental operations within a query engine. Practical performance in many use cases, and results in many well-known benchmarks (e.g. [ClickBench](https://benchmark.clickhouse.com/)), depend heavily on aggregation performance. DataFusion community has been working on improving aggregation performance for a while now, but there is still work to do. A student working on this project will get the chance to hone their skills on high-performance, low(ish) level coding, intricacies of measuring performance, data structures and others.
- **Category:** Core
- **Difficulty:** Advanced
- **Possible Mentor(s) and/or Helper(s):** [jayzhan-synnada](https://github.com/jayzhan-synnada) and [Rachelint](https://github.com/Rachelint)
- **Skills:** Algorithms, Data Structures, Advanced Rust, Databases, Benchmarking Techniques
- **Expected Project Size:** 350 hours

### [Improving Python Bindings](https://github.com/apache/datafusion-python)

- **Description and Outcomes:** DataFusion offers Python bindings that enable users to build data systems using Python. However, the Python bindings are still relatively low-level, and do not expose all APIs libraries like [Pandas](https://pandas.pydata.org/) and [Polars](https://pola.rs/) with a end-user focus offer. This project aims to improve DataFusion's Python bindings to make progress towards moving it closer to such libraries in terms of built-in APIs and functionality.
- **Category:** Python Bindings
- **Difficulty:** Medium
- **Possible Mentor(s) and/or Helper(s):** [timsaucer](https://github.com/timsaucer)
- **Skills:** APIs, FFIs, DataFrame Libraries
- **Expected Project Size:** 175 to 350 hours\*

### [Optimizing DataFusion Binary Size](https://github.com/apache/datafusion/issues/13816)

- **Description and Outcomes:** DataFusion is a foundational library with a large feature set. Even though we try to avoid adding too many dependencies and implement many low-level functionalities inside the codebase, the fast moving nature of the project results in an accumulation of dependencies over time. This inflates DataFusion's binary size over time, which reduces portability and embeddability. This project involves a study of the codebase, using compiler tooling, to understand where code bloat comes from, simplifying/reducing the number of dependencies by efficient in-house implementations, and avoiding code duplications.
- **Category:** Core/Build
- **Difficulty:** Medium
- **Possible Mentor(s) and/or Helper(s):** [comphead](https://github.com/comphead) and [alamb](https://github.com/alamb)
- **Skills:** Software Engineering, Refactoring, Dependency Management, Compilers
- **Expected Project Size:** 175 to 350 hours\*

### [Ergonomic SQL Features](https://github.com/apache/datafusion/issues/14514)

- **Description and Outcomes:** [DuckDB](https://duckdb.org/) has many innovative features that significantly improve the SQL UX. Even though some of those features are already implemented in DataFusion, there are many others we can implement (and get inspiration from). [This page](https://duckdb.org/docs/sql/dialect/friendly_sql.html) contains a good summary of such features. Each such feature will serve as a bite-size, achievable milestone for a cool GSoC project that will have user-facing impact improving the UX on a broad basis. The project will start with a survey of what is already implemented, what is missing, and kick off with a prioritization proposal/implementation plan.
- **Category:** SQL FE
- **Difficulty:** Medium
- **Possible Mentor(s) and/or Helper(s):** [berkaysynnada](https://github.com/berkaysynnada)
- **Skills:** SQL, Planning, Parsing, Software Engineering
- **Expected Project Size:** 350 hours

### [Advanced Interval Analysis](https://github.com/apache/datafusion/issues/14515)

- **Description and Outcomes:** DataFusion implements interval arithmetic and utilizes it for range estimations, which enables use cases in data pruning, optimizations and statistics. However, the current implementation only works efficiently for forward evaluation; i.e. calculating the output range of an expression given input ranges (ranges of columns). When propagating constraints using the same graph, the current approach requires multiple bottom-up and top-down traversals to narrow column bounds fully. This project aims to fix this deficiency by utilizing a better algorithmic approach. Note that this is a _very advanced_ project for students with a deep interest in computational methods, expression graphs, and constraint solvers.
- **Category:** Core
- **Difficulty:** Advanced
- **Possible Mentor(s) and/or Helper(s):** [ozankabak](https://github.com/ozankabak) and [berkaysynnada](https://github.com/berkaysynnada)
- **Skills:** Algorithms, Data Structures, Applied Mathematics, Software Engineering
- **Expected Project Size:** 350 hours

### [Spark-Compatible Functions Crate](https://github.com/apache/datafusion/issues/5600)

- **Description and Outcomes:** In general, DataFusion aims to be compatible with PostgreSQL in terms of functions and behaviors. However, there are many users (and downstream projects, such as [DataFusion Comet](https://datafusion.apache.org/comet/)) that desire compatibility with [Apache Spark](https://spark.apache.org/). This project aims to collect Spark-compatible functions into a separate crate to help such users and/or projects. The project will be an exercise in creating the right APIs, explaining how to use them, and then telling the world about them (e.g. via creating a compatibility-tracking page cataloging such functions, writing blog posts etc.).
- **Category:** Extensions
- **Difficulty:** Medium
- **Possible Mentor(s) and/or Helper(s):** [alamb](https://github.com/alamb) and [andygrove](https://github.com/andygrove)
- **Skills:** SQL, Spark, Software Engineering
- **Expected Project Size:** 175 to 350 hours\*

### [SQL Fuzzing Framework in Rust](https://github.com/apache/datafusion/issues/14535)

- **Description and Outcomes:** Fuzz testing is a very important technique we utilize often in DataFusion. Having SQL-level fuzz testing enables us to battle-test DataFusion in an end-to-end fashion. Initial version of our fuzzing framework is Java-based, but the time has come to migrate to Rust-native solution. This will simplify the overall implementation (by avoiding things like JDBC), enable us to implement more advanced algorithms for query generation, and attract more contributors over time. This project is a good blend of software engineering, algorithms and testing techniques (i.e. fuzzing techniques).
- **Category:** Extensions
- **Difficulty:** Advanced
- **Possible Mentor(s) and/or Helper(s):** [2010YOUY01](https://github.com/2010YOUY01)
- **Skills:** SQL, Testing Techniques, Advanced Rust, Software Engineering
- **Expected Project Size:** 175 to 350 hours\*

\*_There is enough material to make this a 350-hour project, but it is granular enough to make it a 175-hour project as well._

## Contact Us

You can join our [mailing list](mailto:dev%40datafusion.apache.org) and [Discord](https://discord.gg/jHzkpK4em5) to introduce yourself and ask questions.
