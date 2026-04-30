// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Round-trip a custom `PhysicalExpr` through JSON.
//!
//! This example shows the four moving parts an extension author touches to
//! make their own `PhysicalExpr` round-trippable:
//!
//! 1. Pick a stable `TAG` and implement [`PhysicalExprDeserialize`] for it.
//! 2. Override `serde_tag` and `erased_serialize` in `PhysicalExpr` so the
//!    type opts in to the serialization layer.
//! 3. Add the type to a `PhysicalExprRegistry` (here we layer it on top of
//!    DataFusion's default registry).
//! 4. Serialize via `serde_json::to_string`; decode via
//!    `registry.deserialize_json`.
//!
//! Run with:
//! ```bash
//! cargo run --example physical_expr_serde
//! ```

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::serde::{
    DeserializeContext, PhysicalExprDeserialize, PhysicalExprRegistry, default_registry,
};
use datafusion_expr::ColumnarValue;
use serde::{Deserialize, Serialize};

/// A toy `PhysicalExpr` that delegates to a child but flips its boolean
/// output. Self-contained, no children, holds a `String` tag and a child
/// expression — enough to exercise both `ctx.deserialize::<Self>()` for
/// owned fields and `ctx.registry().expr_seed()` for trait-object children.
#[derive(Debug, Eq, Serialize)]
struct AnnotatedNot {
    label: String,
    child: Arc<dyn PhysicalExpr>,
}

impl PartialEq for AnnotatedNot {
    fn eq(&self, other: &Self) -> bool {
        self.label == other.label && self.child.eq(&other.child)
    }
}

impl Hash for AnnotatedNot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.child.hash(state);
    }
}

impl std::fmt::Display for AnnotatedNot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AnnotatedNot[{}]({})", self.label, self.child)
    }
}

impl PhysicalExpr for AnnotatedNot {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let child = self.child.evaluate(batch)?;
        match child {
            ColumnarValue::Array(arr) => {
                let bools = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("AnnotatedNot child must yield Boolean");
                let flipped = arrow::compute::kernels::boolean::not(bools)?;
                Ok(ColumnarValue::Array(Arc::new(flipped) as ArrayRef))
            }
            ColumnarValue::Scalar(_) => unimplemented!("scalar path omitted"),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(AnnotatedNot {
            label: self.label.clone(),
            child: Arc::clone(&children[0]),
        }))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ANNOTATED_NOT({})", self.label)
    }

    // Serialization opt-in: match TAG with the deserialize impl below.
    fn serde_tag(&self) -> &'static str {
        <Self as PhysicalExprDeserialize>::TAG
    }

    fn erased_serialize(&self) -> Box<dyn erased_serde::Serialize + '_> {
        Box::new(self)
    }
}

impl PhysicalExprDeserialize for AnnotatedNot {
    const TAG: &'static str = "example.AnnotatedNot";

    fn deserialize(ctx: &mut DeserializeContext<'_, '_>) -> Result<Self> {
        // Hand-rolled because `child: Arc<dyn PhysicalExpr>` isn't
        // `Deserialize` — it has to recurse through the registry's seed.
        use serde::de::{Deserializer as _, Error as DeError, MapAccess, Visitor};

        struct V<'r> {
            registry: &'r PhysicalExprRegistry,
        }
        impl<'de, 'r> Visitor<'de> for V<'r> {
            type Value = AnnotatedNot;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("AnnotatedNot {label, child}")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<AnnotatedNot, A::Error> {
                let mut label: Option<String> = None;
                let mut child: Option<Arc<dyn PhysicalExpr>> = None;
                while let Some(k) = map.next_key::<String>()? {
                    match k.as_str() {
                        "label" => label = Some(map.next_value()?),
                        "child" => {
                            child = Some(map.next_value_seed(self.registry.expr_seed())?);
                        }
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(AnnotatedNot {
                    label: label.ok_or_else(|| A::Error::missing_field("label"))?,
                    child: child.ok_or_else(|| A::Error::missing_field("child"))?,
                })
            }
        }
        let registry = ctx.registry();
        ctx.deserializer()
            .deserialize_map(V { registry })
            .map_err(|e| {
                datafusion_common::exec_datafusion_err!("AnnotatedNot deserialize: {e}")
            })
    }
}

// I'm using a plain `Deserialize` derive for completeness even though the
// custom `PhysicalExprDeserialize` impl above does the actual work. The
// derive is harmless because the trait method bypasses it entirely.
impl<'de> Deserialize<'de> for AnnotatedNot {
    fn deserialize<D: serde::Deserializer<'de>>(
        _deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "AnnotatedNot must be deserialized through PhysicalExprRegistry",
        ))
    }
}

fn main() -> Result<()> {
    // Build an expression: AnnotatedNot { label: "demo", child: a@0 }
    let expr: Arc<dyn PhysicalExpr> = Arc::new(AnnotatedNot {
        label: "demo".to_string(),
        child: Arc::new(Column::new("a", 0)),
    });
    println!("expr: {expr}");

    // Serialize as JSON. No registry needed for serialization — the
    // serde_tag + erased_serialize hooks live on the type itself.
    let json = serde_json::to_string_pretty(&expr).unwrap();
    println!("\njson:\n{json}");

    // Deserialize. We layer our custom registration on top of the default
    // registry (which knows about Column, BinaryExpr, etc.).
    let registry = default_registry().with::<AnnotatedNot>();
    let back = registry.deserialize_json(&json)?;
    println!("\nback: {back}");

    // Round-tripped expressions should compare equal.
    assert!(expr.dyn_eq(back.as_ref()));
    println!("\nround-trip OK");

    // Sanity check: drop our type and the same JSON should be rejected.
    let stripped_registry = default_registry();
    match stripped_registry.deserialize_json(&json) {
        Ok(_) => panic!("expected error from registry without AnnotatedNot"),
        Err(e) => println!("\nexpected error without registration: {e}"),
    }

    Ok(())
}
