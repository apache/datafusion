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

DataFusion is a query engine that executes SQL and DataFrame queries, which
may include reading data from untrusted sources (e.g., files, network
connections, and user supplied queries). DataFusion is expected to reject
malformed input with an error.

## Input Validation

DataFusion is a low level library designed to be embedded in applications that
have their own security model.

DataFusion will run the queries that are passed to it, and it is the
responsibility of the embedding application to ensure that the queries are safe
to execute (for example, verify any externally supplied queries or URLs).
DataFusion contains some APIs such as [`with_no_dml`] to help the application
validate input, but it does not guarantee that all input is safe to execute.
This is the responsibility of the embedding application.

Given a valid DataFusion DataFrame or SQL query, DataFusion is expected to execute it
correctly without memory safety issues.

Example validation steps that an embedding application may take include:
* Validate that external URLs are safe to read from (e.g., not a local file or a sensitive system file).
* Validate that the SQL query does not contain any DML statements (e.g., `INSERT`, `UPDATE`, `DELETE`) if the application does not want to allow them.
* Validate Arrow data using the [arrow validation APIs]


It is the responsibility of the embedding application to validate
user input

and to safely execute queries
without compromising the security of the host system.

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
