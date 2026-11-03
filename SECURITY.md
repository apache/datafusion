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

# Security Policy

This document outlines the security model for Apache DataFusion and how to
report vulnerabilities.

This model also applies to the [datafusion-cli] command line tool, which is
a thin wrapper around the DataFusion library.

## Security Model

DataFusion is a low level library, designed to be embedded in applications
that have their own security model. This section describes DataFusion's own
model: what counts as a bug versus a vulnerability.

In general, crashes, panics, hangs, and excessive resource consumption
(memory, CPU, or disk) are treated as **bugs**, not vulnerabilities, unless
they are **exploitable** and could let an attacker:

* Execute arbitrary code (Remote Code Execution), or
* Exfiltrate sensitive information from process memory (Information Disclosure).

If the exploitation path is unclear, please report the issue as a bug rather
than a vulnerability. The sections below describe what DataFusion treats as
trusted input in a few specific areas.

### SQL and DataFrame Queries

SQL and DataFrame queries are executable code, comparable to a scripting
language: a query can legitimately read files, open network connections
(e.g. via `CREATE EXTERNAL TABLE`), and consume significant CPU, memory, or
disk. Running such a query is not, on its own, a vulnerability.

It is the embedding application's responsibility to decide whether a query is
safe to run (e.g. validating externally supplied SQL text or URLs) and to
sandbox untrusted queries at the OS/container level if needed. APIs such as
[`SQLOptions::with_allow_dml`] can help restrict what a query is allowed to
do, but do not guarantee that all input is safe to execute.

### Data Files

Format readers (e.g. Parquet, CSV, JSON, Avro, and Arrow IPC) assume a
well-formed file from a trusted writer; use the [arrow validation APIs] to
validate an untrusted file's structure. Issues due to malformed files, and
well-formed files that contain unexpected or adversarial *data*, are bugs rather
than vulnerabilities, as explained above.

### Serialized Plans

Serialized plans, such as [Substrait] and [`datafusion-proto`], are treated
as trusted input. If received from an untrusted source, they should be
validated before being passed to DataFusion for execution.

### Extensions and Other Uses of Public APIs

Code that uses DataFusion's public extension APIs (e.g. user defined functions,
`TableProvider`, `ExecutionPlan`) is trusted to uphold their contracts (for
example, that any `ArrayRef` is a valid Arrow array). Issues arising from
violating the API contracts are not considered a DataFusion vulnerability.

## Reporting a Bug

We treat all bugs seriously and welcome help fixing them. If you find a bug
that does not meet the criteria for a security vulnerability, please report it
in the [public issue tracker](https://github.com/apache/datafusion/issues/).

## Reporting a Vulnerability

For security vulnerabilities **do not file a public issue.** Follow the [ASF
security reporting process] by emailing
[security@apache.org](mailto:security@apache.org).

Include in your report:

- A clear description and minimal reproducer.
- Affected crates and versions.
- Potential impact.

[datafusion-cli]: https://datafusion.apache.org/user-guide/cli/index.html
[`sqloptions::with_allow_dml`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SQLOptions.html#method.with_allow_dml
[arrow validation apis]: https://docs.rs/arrow/latest/arrow/array/struct.ArrayData.html#method.validate_full
[substrait]: https://docs.rs/datafusion-substrait/latest/datafusion_substrait/
[`datafusion-proto`]: https://docs.rs/datafusion-proto/latest/datafusion_proto/
[asf security reporting process]: https://www.apache.org/security/#reporting-a-vulnerability
