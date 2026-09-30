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

DataFusion is a low level library, designed to be embedded in applications
that have their own security model, and it has no internal privilege
boundary: anything that can be expressed as a valid query, a valid data
file, or a call to a public API is assumed to be intentional and is not, by
itself, a vulnerability. The embedding application controls what queries are
run, what data is read, and what extension code is loaded, and that is the
security boundary.

### SQL and DataFrame Queries

SQL and DataFrame queries are executable code, comparable to a query
language like Bash or Python: a query can read files, open network
connections (e.g. via an `ObjectStore` or a custom `TableProvider`), and
consume significant CPU, memory, or disk. Executing an untrusted SQL or
DataFrame query is therefore unsafe by design, and a query that does any of
the above is not, on its own, a vulnerability.

DataFusion will run whatever valid query it is given, and it is the
responsibility of the embedding application to decide which queries are safe
to run (for example, by validating externally supplied SQL text or URLs
before passing them to DataFusion) and to sandbox execution of untrusted
queries at the OS level if needed. DataFusion provides some APIs to help
applications restrict what a query can do, such as
[`SQLOptions::with_allow_dml`] to reject DML statements (e.g. `INSERT`,
`UPDATE`, `DELETE`) and DDL, but it does not guarantee that all input is safe
to execute; that remains the responsibility of the embedding application.

Given a valid DataFusion SQL or DataFrame query over well-formed input,
DataFusion is expected to execute it correctly without memory safety issues.

### Data Files

Format readers (e.g. Parquet, CSV, JSON, Avro, and Arrow IPC) assume a
well-formed file written by a trusted writer. Crashes, panics, excessive
resource consumption, or other unexpected behavior triggered by a malformed
or corrupted file are generally treated as **bugs**, not vulnerabilities,
unless they are exploitable as described below. Applications that cannot
guarantee a file's provenance should not treat reading it as safe, and can
use APIs such as the [arrow validation APIs] to validate a file's structure
before trusting it.

Data *values* inside an otherwise well-formed file are a separate, in-scope
case: column values are frequently attacker controlled (e.g. user input,
scraped text, or log lines written into a file by a trusted pipeline).
Memory corruption, code execution, or other unsafe behavior caused by the
*values* in a file, rather than by its structure, is a vulnerability and is
prioritized as such.

### Extensions and Other Uses of Public APIs

DataFusion exposes many public APIs for extending its behavior, such as
custom `ScalarUDF`/`AggregateUDF`/`WindowUDF` implementations, `TableProvider`
and `ExecutionPlan` implementations, physical expressions, and APIs for
constructing Arrow arrays directly. Code that uses these APIs is trusted to
conform to their documented contracts (for example, that any `ArrayRef`
passed to or returned from an operator is a valid Arrow array). A crash,
memory safety issue, or other unexpected behavior that only occurs because
custom extension code violates such a contract (e.g., constructing an
invalid or corrupt Arrow array) is not considered a DataFusion vulnerability.

## Denial of Service (DoS) and Resource Exhaustion
Unexpected behavior (e.g., panics, crashes, excessive resource consumption, or
infinite loops) triggered by malformed or adversarial input is generally
considered a **bug**, not a security vulnerability, unless it is
**exploitable** and could allow an attacker to


* Execute arbitrary code (Remote Code Execution);
* Exfiltrate sensitive information from process memory (Information Disclosure);

If that exploitation path is unclear, the issue should likely be reported as a
bug.

## Rust Safety, Soundness, and Undefined Behavior

Rust has a very [specific definition of unsafe]. When unsafe behavior results
from using safe code, the code is unsound and can lead to undefined behavior
(UB), which may be exploitable.

However, not all soundness issues are exploitable. In general, issues that
result in undefined behavior using safe APIs are considered bugs unless they
meet the exploitability bar defined above.

We therefore avoid classifying all unsoundness bugs as security
vulnerabilities (e.g. filing [RUSTSEC] and/or [CVE] advisories), which helps
avoid unnecessary downstream churn and keeps our focus on the most critical
issues.

[specific definition of unsafe]: https://doc.rust-lang.org/book/ch20-01-unsafe-rust.html
[rustsec]: https://rustsec.org/
[cve]: https://cve.mitre.org/
[`sqloptions::with_allow_dml`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SQLOptions.html#method.with_allow_dml
[arrow validation apis]: https://docs.rs/arrow/latest/arrow/array/struct.ArrayData.html#method.validate_full

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
