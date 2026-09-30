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

## Security Model

DataFusion is a low level library, designed to be embedded in applications that
have their own security model. This document describes the security model of
DataFusion itself.

### Memory Safety

Given a valid SQL or DataFrame query over well-formed input files, DataFusion
is expected to execute it correctly without memory safety issues. Most of
DataFusion is written using `safe` Rust, which helps provide memory safety
guarantees, but some parts of DataFusion use `unsafe` Rust for performance
reasons. In general crashes (segfaults, panics, etc.) are treated as **bugs**,
not vulnerabilities, unless they are **exploitable** and could allow an attacker
to:

* Execute arbitrary code (Remote Code Execution);
* Exfiltrate sensitive information from process memory (Information Disclosure);

If that exploitation path is unclear, the issue should likely be reported as a
bug.

### SQL and DataFrame Queries

SQL and DataFrame queries are executable code, comparable to a query language
like Bash or Python: a query can read files, open network connections (e.g via
`CREATE EXTERNAL TABLE` ), and consume arbitrary amounts of CPU, memory, or
disk. Executing a query that does any of the above is not, on its own, a
vulnerability.

It is the responsibility of the embedding application to decide and validate
that a query is safe to run (for example, by validating externally supplied
SQL text or URLs before passing them to DataFusion) and to sandbox execution of
untrusted queries at the OS / container level if needed. 

DataFusion provides some APIs to help applications restrict what a query can do,
such as [`SQLOptions::with_allow_dml`] , but it does not guarantee that all
input is safe to execute; that remains the responsibility of the embedding
application.

### Data Files

Format readers (e.g. Parquet, CSV, JSON, Avro, and Arrow IPC) assume a
well-formed file written by a trusted writer. Crashes, panics, excessive
resource consumption, or other unexpected behavior triggered by a malformed or
corrupted file are generally treated as **bugs**, not vulnerabilities, unless
they are exploitable as described above. Applications can use APIs such as the
[arrow validation APIs] to validate a file's structure.

Similarly, a file that is well-formed but contains unexpected or adversarial
data should also be handled properly, but is generally considered a **bug**
(though a more serious one), not a vulnerability, unless it is exploitable as
described above.


### Extensions and Other Uses of Public APIs

DataFusion exposes many public APIs for extending its behavior, such as user
defined function implementations, `TableProvider`s, and `ExecutionPlan`. Code
that uses these APIs is trusted (for example, that any `ArrayRef` passed to
or returned from an operator is a valid Arrow array). A crash, memory safety
issue, or other unexpected behavior that only occurs because custom extension
code violates such a contract is not considered a DataFusion vulnerability.

## Denial of Service (DoS) and Resource Exhaustion

Unexpected behavior (e.g., panics, crashes, excessive resource consumption, or
infinite loops) triggered by malformed or adversarial input is generally
considered a **bug**, not a security vulnerability.

## Reporting a Bug

We treat all bugs seriously and welcome help fixing them. If you find a bug
that does not meet the criteria for a security vulnerability, please report it
in the public issue tracker.

## Reporting a Vulnerability

For security vulnerabilities **do not file a public issue.** Follow the [ASF
security reporting process] by emailing
[security@apache.org](mailto:security@apache.org).

Include in your report:

- A clear description and minimal reproducer.
- Affected crates and versions.
- Potential impact.

[asf security reporting process]: https://www.apache.org/security/#reporting-a-vulnerability
