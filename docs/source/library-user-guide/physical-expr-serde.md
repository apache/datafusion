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

# Serializing `PhysicalExpr`

DataFusion has a format-agnostic serialization layer for
`Arc<dyn PhysicalExpr>`. It is built on `serde`, so any `serde::Serializer` /
`serde::Deserializer` works — JSON for debugging, bincode for binary, a serde
adapter on top of protobuf, etc.

This page describes how to make a custom `PhysicalExpr` round-trippable.
For the **stable wire format** that the Ballista distributed-execution layer
uses across DataFusion versions, see the
[`datafusion-proto`](https://docs.rs/datafusion-proto/) crate. The
serde-based path documented here is **not** wire-stable across versions —
use it for in-process tools, debugging, and intra-cluster ephemeral
serialization.

## Overview

There are four moving parts:

| Concept | Where it lives | Purpose |
| --- | --- | --- |
| `PhysicalExpr::serde_tag` | trait method, default `""` | stable identifier (`"Column"`, `"BinaryExpr"`, …) used to dispatch deserialization |
| `PhysicalExpr::erased_serialize` | trait method, default errors | returns a type-erased `serde::Serialize` view of `self` |
| `PhysicalExprDeserialize` | separate trait | constructor that rebuilds `Self` from a `DeserializeContext` |
| `PhysicalExprRegistry` | builder-style map | tag → constructor lookup; built up explicitly with `.with::<T>()` |

The encoded form is a `{tag, data}` envelope. `tag` is the string returned
from `serde_tag`; `data` is whatever `erased_serialize` produces.

## Implementing for a custom expression

Walk through `datafusion-examples/examples/physical_expr_serde/main.rs` for
a runnable end-to-end demo. The shape is:

```rust,ignore
use std::sync::Arc;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::serde::{
    DeserializeContext, PhysicalExprDeserialize, PhysicalExprRegistry,
    default_registry,
};
use serde::Serialize;

#[derive(Debug, Eq, PartialEq, Hash, Serialize)]
pub struct MyExpr {
    name: String,
    child: Arc<dyn PhysicalExpr>,
}

impl PhysicalExpr for MyExpr {
    // ... usual evaluate, data_type, fmt_sql, etc ...

    fn serde_tag(&self) -> &'static str {
        <Self as PhysicalExprDeserialize>::TAG
    }

    fn erased_serialize(&self) -> Box<dyn erased_serde::Serialize + '_> {
        Box::new(self)
    }
}

impl PhysicalExprDeserialize for MyExpr {
    const TAG: &'static str = "myproject.MyExpr";

    fn deserialize(
        ctx: &mut DeserializeContext<'_, '_>,
    ) -> datafusion::common::Result<Self> {
        // For a leaf expression with no trait-object children, just delegate
        // to the auto-derived `serde::Deserialize` impl:
        //
        //     ctx.deserialize::<Self>()
        //
        // Expressions with `Arc<dyn PhysicalExpr>` fields (children) need a
        // hand-rolled `Visitor` that uses `ctx.registry().expr_seed()` for
        // each child. See the example file for a complete walkthrough.
        unimplemented!("see example")
    }
}
```

Then layer your type onto a registry and round-trip:

```rust,ignore
let registry = default_registry().with::<MyExpr>();
let json = serde_json::to_string(&expr)?;
let back: Arc<dyn PhysicalExpr> = registry.deserialize_json(&json)?;
```

## Children that are themselves `PhysicalExpr`

`Arc<dyn PhysicalExpr>` deliberately doesn't implement `serde::Deserialize`
— deserialization needs the registry, and `Deserialize` doesn't carry
context. To recurse on a child, use the registry's `DeserializeSeed`:

```rust,ignore
fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<MyExpr, A::Error> {
    let mut child: Option<Arc<dyn PhysicalExpr>> = None;
    while let Some(k) = map.next_key::<String>()? {
        match k.as_str() {
            "child" => child = Some(map.next_value_seed(self.registry.expr_seed())?),
            _ => { let _: serde::de::IgnoredAny = map.next_value()?; }
        }
    }
    // ...
}
```

The unary case (single child of type `Arc<dyn PhysicalExpr>`) is common
enough that there's a built-in helper: `ctx.deserialize_unary("arg")`.

## Tags

Tags must be globally unique. Pick something specific to your project:
prefix with your crate name (`"myproject.MyExpr"`) rather than just
`"MyExpr"`, since DataFusion's built-ins occupy short names like
`"Column"` and `"BinaryExpr"`.

`PhysicalExprRegistry::with::<T>()` panics on duplicate tag registration —
this is almost always a programming error and failing loudly during
registry construction is safer than silently shadowing. If you need to
register from runtime-discovered plugins, use `try_with::<T>()` which
returns `Result`.

A tag of `""` (the default `serde_tag` returns) is treated as "not
serializable"; serialization fails with a descriptive error. This means
adding a new `PhysicalExpr` impl is non-breaking — existing types simply
won't be serializable until they opt in.

## Choosing a format

The `PhysicalExprRegistry::deserialize` method accepts any
`serde::Deserializer`, so any format works. Common choices:

- **JSON** (`serde_json`) — readable, great for tests and debugging. Use
  `Registry::deserialize_json(s)` for convenience.
- **Bincode / postcard** — compact binary for over-the-wire ephemeral use.
- **Protobuf via a serde adapter** — if you want to reuse existing `.proto`
  schemas; note this is distinct from `datafusion-proto`'s downcast-based
  path.

Wire stability across DataFusion versions is **not** a goal of this layer.
Use `datafusion-proto` for that.

## What's currently registered

`default_registry()` currently ships with `Column` and `NotExpr` — the
two expressions wired up in this initial PR to validate the trait surface
end-to-end. The rest of the built-ins will be added in follow-ups, in
batches grouped by what they need from the rest of the codebase
(`Operator`, `ScalarValue`, `arrow::DataType`, etc., all need their own
`serde` impls before the expressions that hold them can be registered).
Expressions that haven't opted in fall back to the default `serde_tag`
(empty string) and serialization fails with a "not implemented" error.

## See also

- `datafusion-examples/examples/physical_expr_serde/main.rs` — runnable
  custom-expression round-trip.
- `datafusion-physical-expr-common::serde` — module-level docs covering the
  trait and registry.
- `datafusion-proto` — wire-stable, version-portable proto serialization
  (separate path).
