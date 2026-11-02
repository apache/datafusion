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

## How to update the version of Rust used in CI tests

Make a PR to update the [rust-toolchain] file in the root of the repository.

[rust-toolchain]: https://github.com/apache/datafusion/blob/main/rust-toolchain.toml

## Adding new functions

**Implementation**

| Function type | Location to implement     | Trait to implement                             | Macros to use                                    | Example              |
| ------------- | ------------------------- | ---------------------------------------------- | ------------------------------------------------ | -------------------- |
| Scalar        | [functions][df-functions] | [`ScalarUDFImpl`]                              | `make_udf_function!()` and `export_functions!()` | [`advanced_udf.rs`]  |
| Nested        | [functions-nested]        | [`ScalarUDFImpl`]                              | `make_udf_expr_and_func!()`                      |                      |
| Aggregate     | [functions-aggregate]     | [`AggregateUDFImpl`] and an [`Accumulator`]    | `make_udaf_expr_and_func!()`                     | [`advanced_udaf.rs`] |
| Window        | [functions-window]        | [`WindowUDFImpl`] and a [`PartitionEvaluator`] | `define_udwf_and_expr!()`                        | [`advanced_udwf.rs`] |
| Table         | [functions-table]         | [`TableFunctionImpl`] and a [`TableProvider`]  | `create_udtf_function!()`                        | [`simple_udtf.rs`]   |

- The macros are to simplify some boilerplate such as ensuring a DataFrame API compatible function is also created
- Ensure new functions are properly exported through the subproject
  `mod.rs` or `lib.rs`.
- Functions should preferably provide documentation via the `#[user_doc(...)]` attribute so their documentation
  can be included in the SQL reference documentation (see below section)
- Scalar functions are further grouped into modules for families of functions (e.g. string, math, datetime).
  Functions should be added to the relevant module; if a new module needs to be created then a new [Rust feature]
  should also be added to allow DataFusion users to conditionally compile the modules as needed
- Aggregate functions can optionally implement a [`GroupsAccumulator`] for better performance

Spark compatible functions are [located in separate crate][df-spark] but otherwise follow the same steps, though all
function types (e.g. scalar, nested, aggregate) are grouped together in the single location.

[df-functions]: https://github.com/apache/datafusion/tree/main/datafusion/functions
[functions-nested]: https://github.com/apache/datafusion/tree/main/datafusion/functions-nested
[functions-aggregate]: https://github.com/apache/datafusion/tree/main/datafusion/functions-aggregate
[functions-window]: https://github.com/apache/datafusion/tree/main/datafusion/functions-window
[functions-table]: https://github.com/apache/datafusion/tree/main/datafusion/functions-table
[df-spark]: https://github.com/apache/datafusion/tree/main/datafusion/spark
[`scalarudfimpl`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html
[`aggregateudfimpl`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.AggregateUDFImpl.html
[`accumulator`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.Accumulator.html
[`groupsaccumulator`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.GroupsAccumulator.html
[`windowudfimpl`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.WindowUDFImpl.html
[`partitionevaluator`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.PartitionEvaluator.html
[`tablefunctionimpl`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableFunctionImpl.html
[`tableprovider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
[`advanced_udf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udf.rs
[`advanced_udaf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udaf.rs
[`advanced_udwf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/advanced_udwf.rs
[`simple_udtf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/udf/simple_udtf.rs
[rust feature]: https://doc.rust-lang.org/cargo/reference/features.html

**Testing**

Prefer adding `sqllogictest` integration tests where the function is called via SQL against
well known data and returns an expected result. See the existing [test files][slt-test-files] if
there is an appropriate file to add test cases to, otherwise create a new file. See the
[`sqllogictest` documentation][slt-readme] for details on how to construct these tests.
Ensure edge case, `null` input cases are considered in these tests.

If a behaviour cannot be tested via `sqllogictest` (e.g. testing `simplify()`, needs to be
tested in isolation from the optimizer, difficult to construct exact input via `sqllogictest`)
then tests can be added as Rust unit tests in the implementation module, though these should be
kept minimal where possible

[slt-test-files]: https://github.com/apache/datafusion/tree/main/datafusion/sqllogictest/test_files
[slt-readme]: https://github.com/apache/datafusion/blob/main/datafusion/sqllogictest/README.md

**Documentation**

Run documentation update script `./dev/update_function_docs.sh` which will update the relevant
markdown document [here][fn-doc-home] (see the documents for [scalar][fn-doc-scalar],
[aggregate][fn-doc-aggregate] and [window][fn-doc-window] functions)

- You _should not_ manually update the markdown document after running the script as those manual
  changes would be overwritten on next execution
- Reference [GitHub issue] which introduced this behaviour

[fn-doc-home]: https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql
[fn-doc-scalar]: https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql/scalar_functions.md
[fn-doc-aggregate]: https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql/aggregate_functions.md
[fn-doc-window]: https://github.com/apache/datafusion/blob/main/docs/source/user-guide/sql/window_functions.md
[github issue]: https://github.com/apache/datafusion/issues/12740

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

## How to format `.md` documents

We use [`prettier`] to format `.md` files.

You can either use `npm i -g prettier` to install it globally or use `npx` to run it as a standalone binary.
Using `npx` requires a working node environment. Upgrading to the latest prettier is recommended (by adding
`--upgrade` to the `npm` command).

```bash
$ prettier --version
2.3.0
```

After you've confirmed your prettier version, you can format all the `.md` files:

```bash
prettier -w {datafusion,datafusion-cli,datafusion-examples,dev,docs}/**/*.md
```

[`prettier`]: https://prettier.io/

## How to format `.toml` files

We use [`taplo`] to format `.toml` files.

To install via cargo:

```sh
cargo install taplo-cli --locked
```

> Refer to the [taplo installation documentation][taplo-install] for other ways to install it.

```bash
$ taplo --version
taplo 0.9.0
```

After you've confirmed your `taplo` version, you can format all the `.toml` files:

```bash
taplo fmt
```

[`taplo`]: https://taplo.tamasfe.dev/
[taplo-install]: https://taplo.tamasfe.dev/cli/installation/binary.html

## How to update protobuf/gen dependencies

For the `proto` and `proto-common` crates, the prost/tonic code is generated by running their respective `./regen.sh` scripts,
which in turn invokes the Rust binary located in `./gen`.

This is necessary after modifying the protobuf definitions or altering the dependencies of `./gen`, and requires a
valid installation of [protoc] (see [installation instructions] for details).

```bash
# From repository root
# proto-common
./datafusion/proto-common/regen.sh
# proto
./datafusion/proto/regen.sh
```

[protoc]: https://github.com/protocolbuffers/protobuf#protocol-compiler-installation
[installation instructions]: https://datafusion.apache.org/contributor-guide/development_environment.html#protoc-installation
