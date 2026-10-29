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

# Apache DataFusion Substrait

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate is a submodule of DataFusion that provides a [Substrait] producer and consumer for DataFusion
plans. See [API Docs] for details and examples.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[substrait]: https://substrait.io
[api docs]: https://docs.rs/datafusion-substrait/latest

## Migrating custom consumers to explicit scope context

Relation and expression callbacks on `SubstraitConsumer` now take a final
`&SubstraitConsumerContext` argument. Forward that context when delegating to the
standard conversion helpers or consuming child relations and expressions.
`from_substrait_plan` and `from_substrait_plan_with_consumer` still create the root
context for you. Direct calls to conversion helpers should start with
`SubstraitConsumerContext::default()`.

Remove implementations of `push_outer_schema`, `pop_outer_schema`,
`get_outer_schema`, `push_lambda_parameters`, `pop_lambda_parameters`, and
`lambda_variable`, and remove embedded `DefaultSubstraitLambdaConsumer` values.
The standard handlers resolve scopes through the context. Custom handlers that
introduce scopes should use `context.with_outer_schema(...)` or
`context.with_lambda_parameters(...)` and pass the resulting child context to
nested conversions. There is no matching pop operation; the parent context stays
unchanged on success, error, and cancellation.

`consume_rel_inputs` now consumes JOIN and UNION inputs concurrently on the same
consumer. Keep traversal scopes in the context. Shared services and caches must
support concurrent calls. Consumers requiring sequential input processing can
override `consume_rel_inputs` and await each `consume_rel(rel, context)` in order.

Generated lambda parameter names are now local to lexical scopes, so sibling
lambdas may reuse names. Logical plan displays can change without changing query
results.
