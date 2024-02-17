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

# Apache Arrow DataFusion Substrait

This crate contains a [Substrait] producer and consumer for Apache Arrow
[DataFusion] plans. See [API Docs] for details and examples.

[API Docs]: https://docs.rs/datafusion-substrait/latest/datafusion_substrait

[DataFusion] is an extensible query execution framework,
written in Rust, that uses Apache Arrow as its in-memory format.

[Substrait](https://substrait.io/) provides a cross-language serialization format for relational algebra, based on
protocol buffers.

This crate provides support format for serializing (the producer) and deserializing the
following structures to and from Substrait protobuf:

1. [`LogicalPlan`]'s (including [`Expr`]),
2. [`ExecutionPlan`]s (including [`PhysiscalExpr`])


Potential uses of this crate:
- Build systems that run Substrait plans using DataFusion.
- Connect DataFusion based systems to the broader ecosystem
- Replace the current [DataFusion protobuf definition](https://github.com/apache/arrow-datafusion/blob/main/datafusion/proto/proto/datafusion.proto) used in Ballista for passing query plan fragments to executors
- Make it easier to pass query plans over FFI boundaries, such as from Python to Rust
- Run plans created by Apache Calcite to run in DataFusion

## See Also

Substrait does not (yet) support the full range of plans and expressions than
that DataFusion offers. See [datafusion-proto] for a crate which can 


[substrait]: https://substrait.io/
[datafusion]: https://arrow.apache.org/datafusion
[`logicalplan`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html
[`expr`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr/enum.Expr.html
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`physiscalexpr`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/trait.PhysicalExpr.html
