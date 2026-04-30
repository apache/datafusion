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

//! [`PhysicalExprRegistry::new`] — a registry pre-populated with all of
//! DataFusion's built-in physical expressions.
//!
//! See [`datafusion_physical_expr_common::serde`] for the underlying
//! serialization/deserialization machinery and the
//! [`PhysicalExprDeserialize`] trait.

pub use datafusion_physical_expr_common::serde::{
    DeserializeContext, PhysicalExprDeserialize, PhysicalExprRegistry,
};

use crate::expressions::{Column, NotExpr};

/// Returns a [`PhysicalExprRegistry`] with the subset of DataFusion's
/// built-in physical expressions that have been wired up to the new serde
/// hook so far.
///
/// Currently registered:
///
/// - [`Column`]
/// - [`NotExpr`]
///
/// More built-ins will be added in follow-up PRs as their internal types
/// (notably [`datafusion_common::ScalarValue`] and `arrow::DataType`) gain
/// `serde::{Serialize, Deserialize}` impls.
///
/// Use [`PhysicalExprRegistry::with`] to layer additional custom expressions
/// on top.
pub fn default_registry() -> PhysicalExprRegistry {
    PhysicalExprRegistry::empty()
        .with::<Column>()
        .with::<NotExpr>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    fn round_trip(expr: Arc<dyn PhysicalExpr>) {
        let json = serde_json::to_string(&expr).unwrap();
        let registry = default_registry();
        let back = registry.deserialize_json(&json).unwrap_or_else(|e| {
            panic!("deserialize failed for {json}: {e}");
        });
        assert!(
            expr.dyn_eq(back.as_ref()),
            "round-trip mismatch:\n  before: {expr}\n  after:  {back}\n  json:   {json}"
        );
    }

    #[test]
    fn column_round_trip() {
        round_trip(Arc::new(Column::new("a", 3)));
    }

    #[test]
    fn not_expr_round_trip() {
        round_trip(Arc::new(NotExpr::new(Arc::new(Column::new("a", 0)))));
    }

    #[test]
    fn nested_not_round_trip() {
        // NOT(NOT(a))
        let inner = NotExpr::new(Arc::new(Column::new("a", 0)));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(Arc::new(inner)));
        round_trip(expr);
    }

    #[test]
    fn unknown_tag_errors() {
        let registry = PhysicalExprRegistry::empty();
        let json = r#"{"tag":"Column","data":{"name":"a","index":0}}"#;
        let err = registry.deserialize_json(json).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no PhysicalExpr registered under tag"),
            "{msg}"
        );
    }

    #[test]
    fn try_with_duplicate_tag_returns_err() {
        let result = PhysicalExprRegistry::empty()
            .with::<Column>()
            .try_with::<Column>();
        let err = match result {
            Ok(_) => panic!("expected error on duplicate registration"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("duplicate registration"), "{err}");
    }

    #[test]
    #[should_panic(expected = "duplicate registration")]
    fn with_duplicate_tag_panics() {
        let _ = PhysicalExprRegistry::empty()
            .with::<Column>()
            .with::<Column>();
    }
}
