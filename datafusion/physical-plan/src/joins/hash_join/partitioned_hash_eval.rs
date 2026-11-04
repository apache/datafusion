// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Hash computation expression for dynamic filtering

use std::{fmt::Display, hash::Hash, sync::Arc};

use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::create_hashes;
use datafusion_expr::ColumnarValue;
use datafusion_expr_common::dyn_eq::DynHash;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, PhysicalExprRef};

/// RandomState wrapper that preserves the seed used to create it.
///
/// This is needed because `RandomState` doesn't expose its seed after creation,
/// but we need them for serialization (e.g., protobuf serde).
#[derive(Clone, Debug)]
pub struct SeededRandomState {
    random_state: RandomState,
    seed: u64,
}

impl SeededRandomState {
    /// Create a new SeededRandomState with the given seed.
    pub const fn with_seed(k: u64) -> Self {
        Self {
            random_state: RandomState::with_seed(k),
            seed: k,
        }
    }

    /// Get the inner RandomState.
    pub fn random_state(&self) -> &RandomState {
        &self.random_state
    }

    /// Get the seed used to create this RandomState.
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

/// Physical expression that computes hash values for a set of columns
///
/// This expression computes the hash of join key columns using a specific RandomState.
/// It returns a UInt64Array containing the hash values.
///
/// This is used for:
/// - Computing routing hashes (with RepartitionExec's 0,0,0,0 seeds)
/// - Computing lookup hashes (with HashJoin's 'J','O','I','N' seeds)
pub struct HashExpr {
    /// Columns to hash
    on_columns: Vec<PhysicalExprRef>,
    /// Random state for hashing (with seeds preserved for serialization)
    random_state: SeededRandomState,
    /// Description for display
    description: String,
}

impl HashExpr {
    /// Create a new HashExpr
    ///
    /// # Arguments
    /// * `on_columns` - Columns to hash
    /// * `random_state` - SeededRandomState for hashing
    /// * `description` - Description for debugging (e.g., "hash_repartition", "hash_join")
    pub fn new(
        on_columns: Vec<PhysicalExprRef>,
        random_state: SeededRandomState,
        description: String,
    ) -> Self {
        Self {
            on_columns,
            random_state,
            description,
        }
    }

    /// Get the columns being hashed.
    pub fn on_columns(&self) -> &[PhysicalExprRef] {
        &self.on_columns
    }

    /// Get the seed used for hashing.
    pub fn seed(&self) -> u64 {
        self.random_state.seed()
    }

    /// Get the description.
    pub fn description(&self) -> &str {
        &self.description
    }
}

impl std::fmt::Debug for HashExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .on_columns
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let seed = self.seed();
        write!(f, "{}({cols}, [{seed}])", self.description)
    }
}

impl Hash for HashExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.on_columns.dyn_hash(state);
        self.description.hash(state);
        self.seed().hash(state);
    }
}

impl PartialEq for HashExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_columns == other.on_columns
            && self.description == other.description
            && self.seed() == other.seed()
    }
}

impl Eq for HashExpr {}

impl Display for HashExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl PhysicalExpr for HashExpr {
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.on_columns.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(HashExpr::new(
            children,
            self.random_state.clone(),
            self.description.clone(),
        )))
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();

        // Evaluate columns
        let keys_values = evaluate_columns(&self.on_columns, batch)?;

        // Compute hashes
        let mut hashes_buffer = vec![0; num_rows];
        create_hashes(
            &keys_values,
            self.random_state.random_state(),
            &mut hashes_buffer,
        )?;

        Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(
            hashes_buffer,
        ))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        // Destructure exhaustively (no `..`) so that a newly added field is a
        // compile error here instead of being silently left out of the proto.
        let Self {
            on_columns,
            random_state,
            description,
        } = self;

        let on_columns = ctx.encode_children_expressions(on_columns)?;
        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::HashExpr(
                protobuf::PhysicalHashExprNode {
                    on_columns,
                    // only the seed is serialized; `RandomState` is rebuilt
                    // from it by `SeededRandomState::with_seed` on decode
                    seed0: random_state.seed(),
                    description: description.clone(),
                },
            )),
        }))
    }
}

#[cfg(feature = "proto")]
impl HashExpr {
    /// Reconstruct a [`HashExpr`] from its protobuf representation.
    ///
    /// Takes the whole [`PhysicalExprNode`], the exact inverse of what
    /// [`PhysicalExpr::try_to_proto`] produces, so every expression's
    /// `try_from_proto` shares one signature. Child sub-expressions are
    /// decoded recursively via [`PhysicalExprDecodeCtx::decode`].
    ///
    /// [`PhysicalExprNode`]: datafusion_proto_models::protobuf::PhysicalExprNode
    /// [`PhysicalExpr::try_to_proto`]: datafusion_physical_expr_common::physical_expr::PhysicalExpr::try_to_proto
    /// [`PhysicalExprDecodeCtx::decode`]: datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx::decode
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_proto_models::protobuf;
        let Some(protobuf::physical_expr_node::ExprType::HashExpr(hash_expr)) =
            &node.expr_type
        else {
            return datafusion_common::internal_err!(
                "PhysicalExprNode is not a HashExpr"
            );
        };
        // Destructure exhaustively (no `..`) so that a newly added proto field
        // is a compile error here instead of being silently ignored.
        let protobuf::PhysicalHashExprNode {
            on_columns,
            seed0,
            description,
        } = hash_expr;

        let on_columns = ctx.decode_children_expressions(on_columns)?;
        Ok(Arc::new(HashExpr::new(
            on_columns,
            SeededRandomState::with_seed(*seed0),
            description.clone(),
        )))
    }
}

fn evaluate_columns(
    columns: &[PhysicalExprRef],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    let num_rows = batch.num_rows();
    columns
        .iter()
        .map(|c| c.evaluate(batch)?.into_array(num_rows))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_physical_expr::expressions::Column;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    fn compute_hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_hash_expr_eq_same() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));
        let col_b: PhysicalExprRef = Arc::new(Column::new("b", 1));

        let expr1 = HashExpr::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            SeededRandomState::with_seed(1),
            "test_hash".to_string(),
        );

        let expr2 = HashExpr::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            SeededRandomState::with_seed(1),
            "test_hash".to_string(),
        );

        assert_eq!(expr1, expr2);
    }

    #[test]
    fn test_hash_expr_eq_different_columns() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));
        let col_b: PhysicalExprRef = Arc::new(Column::new("b", 1));
        let col_c: PhysicalExprRef = Arc::new(Column::new("c", 2));

        let expr1 = HashExpr::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            SeededRandomState::with_seed(1),
            "test_hash".to_string(),
        );

        let expr2 = HashExpr::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_c)],
            SeededRandomState::with_seed(1),
            "test_hash".to_string(),
        );

        assert_ne!(expr1, expr2);
    }

    #[test]
    fn test_hash_expr_eq_different_description() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));

        let expr1 = HashExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            "hash_one".to_string(),
        );

        let expr2 = HashExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            "hash_two".to_string(),
        );

        assert_ne!(expr1, expr2);
    }

    #[test]
    fn test_hash_expr_eq_different_seeds() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));

        let expr1 = HashExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            "test_hash".to_string(),
        );

        let expr2 = HashExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(5),
            "test_hash".to_string(),
        );

        assert_ne!(expr1, expr2);
    }

    #[test]
    fn test_hash_expr_hash_consistency() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));
        let col_b: PhysicalExprRef = Arc::new(Column::new("b", 1));

        let expr1 = HashExpr::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            SeededRandomState::with_seed(1),
            "test_hash".to_string(),
        );

        let expr2 = HashExpr::new(
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            SeededRandomState::with_seed(1),
            "test_hash".to_string(),
        );

        // Equal expressions should have equal hashes
        assert_eq!(expr1, expr2);
        assert_eq!(compute_hash(&expr1), compute_hash(&expr2));
    }

    #[cfg(feature = "proto")]
    mod proto_tests {
        use super::*;
        use arrow::datatypes::{DataType, Field};
        use datafusion_common::internal_datafusion_err;
        use datafusion_physical_expr_common::physical_expr::proto_decode::{
            PhysicalExprDecode, PhysicalExprDecodeCtx,
        };
        use datafusion_physical_expr_common::physical_expr::proto_encode::{
            PhysicalExprEncode, PhysicalExprEncodeCtx,
        };
        use datafusion_proto_models::protobuf;

        struct TestEncoder;

        impl PhysicalExprEncode for TestEncoder {
            fn encode(
                &self,
                expr: &Arc<dyn PhysicalExpr>,
            ) -> Result<protobuf::PhysicalExprNode> {
                let ctx = PhysicalExprEncodeCtx::new(self);
                expr.try_to_proto(&ctx)?.ok_or_else(|| {
                    internal_datafusion_err!("test encoder cannot encode {expr:?}")
                })
            }
        }

        struct TestDecoder;

        impl PhysicalExprDecode for TestDecoder {
            fn decode(
                &self,
                node: &protobuf::PhysicalExprNode,
                schema: &Schema,
            ) -> Result<Arc<dyn PhysicalExpr>> {
                let ctx = PhysicalExprDecodeCtx::new(schema, self);
                match &node.expr_type {
                    Some(protobuf::physical_expr_node::ExprType::Column(_)) => {
                        Column::try_from_proto(node, &ctx)
                    }
                    _ => datafusion_common::internal_err!(
                        "test decoder cannot decode {node:?}"
                    ),
                }
            }
        }

        fn test_decode_ctx<'a>(
            schema: &'a Schema,
            decoder: &'a TestDecoder,
        ) -> PhysicalExprDecodeCtx<'a> {
            PhysicalExprDecodeCtx::new(schema, decoder)
        }

        #[test]
        fn hash_expr_try_to_proto() {
            let expr = HashExpr::new(
                vec![Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 1))],
                SeededRandomState::with_seed(42),
                "hash_join".to_string(),
            );
            let encoder = TestEncoder;
            let ctx = PhysicalExprEncodeCtx::new(&encoder);

            let proto = expr.try_to_proto(&ctx).unwrap().unwrap();

            assert_eq!(proto.expr_id, None);
            let hash_expr = match proto.expr_type.unwrap() {
                protobuf::physical_expr_node::ExprType::HashExpr(hash_expr) => hash_expr,
                other => panic!("expected HashExpr, got {other:?}"),
            };
            assert_eq!(hash_expr.seed0, 42);
            assert_eq!(hash_expr.description, "hash_join");
            assert_eq!(hash_expr.on_columns.len(), 2);
            assert!(
                hash_expr
                    .on_columns
                    .iter()
                    .all(|expr| expr.expr_id.is_none())
            );
        }

        #[test]
        fn hash_expr_try_from_proto() {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ]);
            let decoder = TestDecoder;
            let ctx = test_decode_ctx(&schema, &decoder);
            let proto = protobuf::PhysicalExprNode {
                expr_id: None,
                expr_type: Some(protobuf::physical_expr_node::ExprType::HashExpr(
                    protobuf::PhysicalHashExprNode {
                        on_columns: vec![
                            protobuf::PhysicalExprNode {
                                expr_id: None,
                                expr_type: Some(
                                    protobuf::physical_expr_node::ExprType::Column(
                                        protobuf::PhysicalColumn {
                                            name: "a".to_string(),
                                            index: 0,
                                        },
                                    ),
                                ),
                            },
                            protobuf::PhysicalExprNode {
                                expr_id: None,
                                expr_type: Some(
                                    protobuf::physical_expr_node::ExprType::Column(
                                        protobuf::PhysicalColumn {
                                            name: "b".to_string(),
                                            index: 1,
                                        },
                                    ),
                                ),
                            },
                        ],
                        seed0: 42,
                        description: "hash_join".to_string(),
                    },
                )),
            };

            let expr = HashExpr::try_from_proto(&proto, &ctx).unwrap();
            let expr = expr.downcast_ref::<HashExpr>().unwrap();

            assert_eq!(expr.seed(), 42);
            assert_eq!(expr.description(), "hash_join");
            assert_eq!(expr.on_columns().len(), 2);
            assert_eq!(
                expr.on_columns()[0]
                    .downcast_ref::<Column>()
                    .map(|col| (col.name(), col.index())),
                Some(("a", 0))
            );
            assert_eq!(
                expr.on_columns()[1]
                    .downcast_ref::<Column>()
                    .map(|col| (col.name(), col.index())),
                Some(("b", 1))
            );
        }

        #[test]
        fn hash_expr_try_from_proto_rejects_wrong_node_type() {
            let schema = Schema::empty();
            let decoder = TestDecoder;
            let ctx = test_decode_ctx(&schema, &decoder);
            let proto = protobuf::PhysicalExprNode {
                expr_id: None,
                expr_type: Some(protobuf::physical_expr_node::ExprType::Column(
                    protobuf::PhysicalColumn {
                        name: "a".to_string(),
                        index: 0,
                    },
                )),
            };

            let err = HashExpr::try_from_proto(&proto, &ctx).unwrap_err();
            assert!(
                err.to_string()
                    .contains("PhysicalExprNode is not a HashExpr"),
                "{err}"
            );
        }
    }
}
