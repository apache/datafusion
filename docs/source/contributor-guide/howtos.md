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

# HOWTOs

## How to add a new scalar function

Below is a checklist of what you need to do to add a new scalar function to DataFusion:

- Add the actual implementation of the function to a new module file within:
  - [here](https://github.com/apache/datafusion/tree/main/datafusion/functions-array) for array functions
  - [here](https://github.com/apache/datafusion/tree/main/datafusion/functions/src/crypto) for crypto functions
  - [here](https://github.com/apache/datafusion/tree/main/datafusion/functions/src/datetime) for datetime functions
  - [here](https://github.com/apache/datafusion/tree/main/datafusion/functions/src/encoding) for encoding functions
  - [here](https://github.com/apache/datafusion/tree/main/datafusion/functions/src/math) for math functions
  - [here](https://github.com/apache/datafusion/tree/main/datafusion/functions/src/regex) for regex functions
  - [here](https://github.com/apache/datafusion/tree/main/datafusion/functions/src/string) for string functions
  - [here](https://github.com/apache/datafusion/tree/main/datafusion/functions/src/unicode) for unicode functions
  - create a new module [here](https://github.com/apache/datafusion/tree/main/datafusion/functions/src/) for other functions.
- New function modules - for example a `vector` module, should use a [rust feature](https://doc.rust-lang.org/cargo/reference/features.html) (for example `vector_expressions`) to allow DataFusion
  users to enable or disable the new module as desired.
- The implementation of the function is done via implementing `ScalarUDFImpl` trait for the function struct.
  - See the [advanced_udf.rs] example for an example implementation
  - Add tests for the new function
- To connect the implementation of the function add to the mod.rs file:
  - a `mod xyz;` where xyz is the new module file
  - a call to `make_udf_function!(..);`
  - an item in `export_functions!(..);`
- In [sqllogictest/test_files], add new `sqllogictest` integration tests where the function is called through SQL against well known data and returns the expected result.
  - Documentation for `sqllogictest` [here](https://github.com/apache/datafusion/blob/main/datafusion/sqllogictest/README.md)
- Add SQL reference documentation [here](https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql/scalar_functions.md)

[advanced_udf.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udaf.rs
[sqllogictest/test_files]: https://github.com/apache/datafusion/tree/main/datafusion/sqllogictest/test_files

## How to add a new aggregate function

Below is a checklist of what you need to do to add a new aggregate function to DataFusion:

- Add the actual implementation of an `Accumulator` and `AggregateExpr`:
- In [datafusion/expr/src](../../../datafusion/expr/src/aggregate_function.rs), add:
  - a new variant to `AggregateFunction`
  - a new entry to `FromStr` with the name of the function as called by SQL
  - a new line in `return_type` with the expected return type of the function, given an incoming type
  - a new line in `signature` with the signature of the function (number and types of its arguments)
  - a new line in `create_aggregate_expr` mapping the built-in to the implementation
  - tests to the function.
- In [sqllogictest/test_files], add new `sqllogictest` integration tests where the function is called through SQL against well known data and returns the expected result.
  - Documentation for `sqllogictest` [here](https://github.com/apache/datafusion/blob/main/datafusion/sqllogictest/README.md)
- Add SQL reference documentation [here](https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql/aggregate_functions.md)

## How to display plans graphically

The query plans represented by `LogicalPlan` nodes can be graphically
rendered using [Graphviz](https://www.graphviz.org/).

To do so, save the output of the `display_graphviz` function to a file.:

```rust
// Create plan somehow...
let mut output = File::create("/tmp/plan.dot")?;
write!(output, "{}", plan.display_graphviz());
```

Then, use the `dot` command line tool to render it into a file that
can be displayed. For example, the following command creates a
`/tmp/plan.pdf` file:

```bash
dot -Tpdf < /tmp/plan.dot > /tmp/plan.pdf
```

## How to format `.md` document

We are using `prettier` to format `.md` files.

You can either use `npm i -g prettier` to install it globally or use `npx` to run it as a standalone binary. Using `npx` required a working node environment. Upgrading to the latest prettier is recommended (by adding `--upgrade` to the `npm` command).

```bash
$ prettier --version
2.3.0
```

After you've confirmed your prettier version, you can format all the `.md` files:

```bash
prettier -w {datafusion,datafusion-cli,datafusion-examples,dev,docs}/**/*.md
```

## How to format `.toml` files

We use `taplo` to format `.toml` files.

For Rust developers, you can install it via:

```sh
cargo install taplo-cli --locked
```

> Refer to the [Installation section][doc] on other ways to install it.
>
> [doc]: https://taplo.tamasfe.dev/cli/installation/binary.html

```bash
$ taplo --version
taplo 0.9.0
```

After you've confirmed your `taplo` version, you can format all the `.toml` files:

```bash
taplo fmt
```
