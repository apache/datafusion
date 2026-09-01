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

//! Hash computation and hash table lookup expressions for dynamic filtering

use std::{fmt::Display, hash::Hash, sync::Arc};

use arrow::array::BooleanArray;
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::{create_hashes, with_hashes};
#[cfg(feature = "proto")]
use datafusion_common::internal_err;
use datafusion_expr::ColumnarValue;
use datafusion_expr_common::dyn_eq::DynHash;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, PhysicalExprRef};
use hashbrown::HashTable;

use crate::joins::Map;
use crate::joins::array_map::ArrayMap;

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
            return internal_err!("PhysicalExprNode is not a HashExpr");
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

/// Physical expression that checks join keys in a [`Map`] (hash table or array map).
///
/// Returns a [`BooleanArray`] indicating if join keys (from `on_columns`) exist in the map.
// TODO: rename to MapLookupExpr
pub struct HashTableLookupExpr {
    /// Columns in the ON clause used to compute the join key for lookups
    on_columns: Vec<PhysicalExprRef>,
    /// Random state for hashing (with seeds preserved for serialization)
    random_state: SeededRandomState,
    /// Map to check against. Deserialized expressions hold a membership-only
    /// variant of [`HashTableLookupExprMap`], which supports nothing beyond
    /// membership checks.
    map: HashTableLookupExprMap,
    /// Description for display
    description: String,
}
impl HashTableLookupExpr {
    /// Create a new HashTableLookupExpr
    ///
    /// # Arguments
    /// * `on_columns` - Columns in the ON clause used to compute the join key
    /// * `random_state` - SeededRandomState for hashing
    /// * `map` - Map to check membership (hash table or array map)
    /// * `description` - Description for debugging
    /// # Note
    /// This is public for internal testing purposes only and is not
    /// guaranteed to be stable across versions.
    pub fn new(
        on_columns: Vec<PhysicalExprRef>,
        random_state: SeededRandomState,
        map: Arc<Map>,
        description: String,
    ) -> Self {
        Self {
            on_columns,
            random_state,
            map: HashTableLookupExprMap::Normal(map),
            description,
        }
    }
}
impl std::fmt::Debug for HashTableLookupExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .on_columns
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let seed = self.random_state.seed();
        write!(f, "{}({cols}, [{seed}])", self.description)
    }
}

impl Hash for HashTableLookupExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.on_columns.dyn_hash(state);
        self.description.hash(state);
        self.random_state.seed().hash(state);
        self.map.hash(state);
    }
}

impl PartialEq for HashTableLookupExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_columns == other.on_columns
            && self.description == other.description
            && self.random_state.seed() == other.random_state.seed()
            && self.map == other.map
    }
}

impl Eq for HashTableLookupExpr {}

impl Display for HashTableLookupExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl PhysicalExpr for HashTableLookupExpr {
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.on_columns.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(HashTableLookupExpr {
            on_columns: children,
            random_state: self.random_state.clone(),
            map: self.map.clone(),
            description: self.description.clone(),
        }))
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // Evaluate columns
        let join_keys = evaluate_columns(&self.on_columns, batch)?;

        match &self.map {
            HashTableLookupExprMap::Normal(normal) => match &**normal {
                Map::HashMap(map) => {
                    with_hashes(&join_keys, self.random_state.random_state(), |hashes| {
                        let array = map.contain_hashes(hashes);
                        Ok(ColumnarValue::Array(Arc::new(array)))
                    })
                }
                Map::ArrayMap(map) => {
                    let array = map.contain_keys(&join_keys)?;
                    Ok(ColumnarValue::Array(Arc::new(array)))
                }
            },
            HashTableLookupExprMap::MembershipOnlyHashMap(map) => {
                with_hashes(&join_keys, self.random_state.random_state(), |hashes| {
                    let array = map.contain_hashes(hashes);
                    Ok(ColumnarValue::Array(Arc::new(array)))
                })
            }
            HashTableLookupExprMap::MembershipOnlyArrayMap(map) => {
                let array = map.contain_keys(&join_keys)?;
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
        }
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;
        use datafusion_proto_models::protobuf::physical_expr_node::ExprType;

        // Destructure exhaustively (no `..`) so that a newly added field is a
        // compile error here, forcing a decision about whether the lit(true)
        // replacement below is still the right thing to emit.
        let Self {
            // deliberately not serialized: the whole expression is replaced
            // with lit(true), see the comment below
            on_columns: _,
            random_state: _,
            map: _,
            description: _,
        } = self;

        // HashTableLookupExpr holds a runtime Arc<Map> (the build-side hash
        // table). This can be serialized, but only in a way that maintains
        // the set membership of the Map. A round-tripped map is not useable for
        // anything beyond expression evaluation

        let on_columns = ctx.encode_children_expressions(&self.on_columns)?;
        let map = try_map_to_proto_membership_only(&self.map)?;

        let expr =
            ExprType::HashTableLookupExpr(protobuf::PhysicalHashTableLookupExprNode {
                on_columns,
                seed0: self.random_state.seed,
                description: self.description.clone(),
                map: Some(map),
            });

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(expr),
        }))
    }
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

/// Encode the map in its membership-only proto form.
#[cfg(feature = "proto")]
fn try_map_to_proto_membership_only(
    map: &HashTableLookupExprMap,
) -> Result<datafusion_proto_models::protobuf::physical_hash_table_lookup_expr_node::Map>
{
    use datafusion_proto_models::protobuf;

    match map {
        HashTableLookupExprMap::Normal(normal) => match &**normal {
            Map::ArrayMap(array_map) => Ok(
                protobuf::physical_hash_table_lookup_expr_node::Map::ArrayMapMembership(
                    array_map.to_proto_membership_only(),
                ),
            ),
            Map::HashMap(hash_map) => Ok(
                protobuf::physical_hash_table_lookup_expr_node::Map::HashMapMembership(
                    protobuf::HashMapMembership {
                        build_hashes: hash_map.hashes(),
                    },
                ),
            ),
        },
        HashTableLookupExprMap::MembershipOnlyHashMap(hash_map) => Ok(
            protobuf::physical_hash_table_lookup_expr_node::Map::HashMapMembership(
                protobuf::HashMapMembership {
                    build_hashes: hash_map.hashes(),
                },
            ),
        ),
        HashTableLookupExprMap::MembershipOnlyArrayMap(array_map) => Ok(
            protobuf::physical_hash_table_lookup_expr_node::Map::ArrayMapMembership(
                array_map.0.to_proto_membership_only(),
            ),
        ),
    }
}

#[cfg(feature = "proto")]
impl HashTableLookupExpr {
    /// Reconstruct a [`HashTableLookupExpr`] from its protobuf representation.
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
        use datafusion_proto_models::protobuf::{
            physical_expr_node::ExprType, physical_hash_table_lookup_expr_node::Map,
        };

        let Some(ExprType::HashTableLookupExpr(hash_table_lookup_expr)) = &node.expr_type
        else {
            return internal_err!("PhysicalExprNode is not a HashTableLookupExpr");
        };
        let on_columns =
            ctx.decode_children_expressions(&hash_table_lookup_expr.on_columns)?;

        let map = match &hash_table_lookup_expr.map {
            Some(Map::HashMapMembership(membership)) => {
                HashTableLookupExprMap::MembershipOnlyHashMap(Arc::new(
                    JoinHashMembershipMap::new(&membership.build_hashes),
                ))
            }
            Some(Map::ArrayMapMembership(membership)) => {
                HashTableLookupExprMap::MembershipOnlyArrayMap(Arc::new(
                    JoinMembershipArrayMap(ArrayMap::try_from_proto_membership_only(
                        membership,
                    )?),
                ))
            }
            None => return internal_err!("HashTableLookupExpr has no map"),
        };

        Ok(Arc::new(HashTableLookupExpr {
            on_columns,
            random_state: SeededRandomState::with_seed(hash_table_lookup_expr.seed0),
            map,
            description: hash_table_lookup_expr.description.clone(),
        }))
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

#[derive(Clone)]
enum HashTableLookupExprMap {
    /// A regular build-side hash map
    Normal(Arc<Map>),
    /// A membership-checks only version of a hashmap, only constructed via expression deserialization
    MembershipOnlyHashMap(Arc<JoinHashMembershipMap>),
    /// A membership-checks only version of an array map, only constructed via expression deserialization
    MembershipOnlyArrayMap(Arc<JoinMembershipArrayMap>),
}
impl Hash for HashTableLookupExprMap {
    // Note that we compare hash_map by pointer equality.
    // Actually comparing the contents of the hash maps would be expensive.
    // The way these hash maps are used in actuality is that HashJoinExec creates
    // one per partition per query execution, thus it is never possible for two different
    // hash maps to have the same content in practice.
    // Theoretically this is a public API and users could create identical hash maps,
    // but that seems unlikely and not worth paying the cost of deep comparison all the time.

    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Normal(a) => Arc::as_ptr(a).hash(state),
            Self::MembershipOnlyHashMap(a) => Arc::as_ptr(a).hash(state),
            Self::MembershipOnlyArrayMap(a) => Arc::as_ptr(a).hash(state),
        }
    }
}
impl PartialEq for HashTableLookupExprMap {
    // Note that we compare hash_map by pointer equality.
    // Actually comparing the contents of the hash maps would be expensive.
    // The way these hash maps are used in actuality is that HashJoinExec creates
    // one per partition per query execution, thus it is never possible for two different
    // hash maps to have the same content in practice.
    // Theoretically this is a public API and users could create identical hash maps,
    // but that seems unlikely and not worth paying the cost of deep comparison all the time.

    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Normal(a), Self::Normal(b)) => Arc::ptr_eq(a, b),
            (Self::MembershipOnlyHashMap(a), Self::MembershipOnlyHashMap(b)) => {
                Arc::ptr_eq(a, b)
            }
            (Self::MembershipOnlyArrayMap(a), Self::MembershipOnlyArrayMap(b)) => {
                Arc::ptr_eq(a, b)
            }
            _ => false,
        }
    }
}

/// Membership-only join map reconstructed from serialized distinct hashes.
/// Supports `contain_hashes` lookups only; it has no build-side rows.
struct JoinHashMembershipMap {
    map: HashTable<(u64, ())>,
}
impl JoinHashMembershipMap {
    fn new(hashes: &[u64]) -> Self {
        let mut map = HashTable::with_capacity(hashes.len());

        // Wire input is trusted to contain distinct hashes. A duplicate would
        // insert a duplicate entry, which wastes memory and skews `len()` but
        // cannot affect membership results.
        for h in hashes {
            map.insert_unique(*h, (*h, ()), |(h, _)| *h);
        }

        Self { map }
    }

    fn contain_hashes(&self, hashes: &[u64]) -> BooleanArray {
        crate::joins::join_hash_map::contain_hashes(&self.map, hashes)
    }

    fn hashes(&self) -> Vec<u64> {
        self.map.iter().map(|(h, _)| *h).collect()
    }
}

/// Wrapper type for ArrayMap to restrict it to membership checks only
struct JoinMembershipArrayMap(ArrayMap);
impl JoinMembershipArrayMap {
    #[inline]
    fn contain_keys(&self, keys: &[ArrayRef]) -> Result<BooleanArray> {
        self.0.contain_keys(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::joins::join_hash_map::JoinHashMapU32;
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
        use crate::joins::join_hash_map::JoinHashMapU64;
        use arrow::array::Int64Array;
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
                    _ => internal_err!("test decoder cannot decode {node:?}"),
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

        #[test]
        fn hash_table_lookup_expr_try_to_proto_hash_map_membership() {
            let build_hashes = [7u64, 42, 9999];
            let map = Arc::new(Map::HashMap(Box::new(join_hash_map_with_hashes(
                &build_hashes,
            ))));
            let expr = HashTableLookupExpr::new(
                vec![Arc::new(Column::new("a", 0))],
                SeededRandomState::with_seed(42),
                map,
                "hash_lookup".to_string(),
            );
            let encoder = TestEncoder;
            let ctx = PhysicalExprEncodeCtx::new(&encoder);

            let proto = expr.try_to_proto(&ctx).unwrap().unwrap();

            assert_eq!(proto.expr_id, None);
            let node = match proto.expr_type.unwrap() {
                protobuf::physical_expr_node::ExprType::HashTableLookupExpr(node) => node,
                other => panic!("expected HashTableLookupExpr, got {other:?}"),
            };
            assert_eq!(node.seed0, 42);
            assert_eq!(node.description, "hash_lookup");
            assert_eq!(node.on_columns.len(), 1);
            let membership = match node.map.unwrap() {
                protobuf::physical_hash_table_lookup_expr_node::Map::HashMapMembership(
                    m,
                ) => m,
                other => panic!("expected HashMapMembership, got {other:?}"),
            };
            // Hash table iteration order is arbitrary; compare as a sorted set.
            let mut got = membership.build_hashes;
            got.sort_unstable();
            assert_eq!(got, build_hashes);
        }

        #[test]
        fn hash_table_lookup_expr_try_to_proto_array_map_membership() {
            let build: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 12, 10, 15]));
            let array_map = ArrayMap::try_new(&build, 10, 15).unwrap();
            let expr = HashTableLookupExpr::new(
                vec![Arc::new(Column::new("a", 0))],
                SeededRandomState::with_seed(42),
                Arc::new(Map::ArrayMap(array_map)),
                "hash_lookup".to_string(),
            );
            let encoder = TestEncoder;
            let ctx = PhysicalExprEncodeCtx::new(&encoder);

            let proto = expr.try_to_proto(&ctx).unwrap().unwrap();

            let node = match proto.expr_type.unwrap() {
                protobuf::physical_expr_node::ExprType::HashTableLookupExpr(node) => node,
                other => panic!("expected HashTableLookupExpr, got {other:?}"),
            };
            let membership = match node.map.unwrap() {
                protobuf::physical_hash_table_lookup_expr_node::Map::ArrayMapMembership(
                    m,
                ) => m,
                other => panic!("expected ArrayMapMembership, got {other:?}"),
            };
            assert_eq!(membership.offset, 10);
            assert_eq!(membership.num_slots, 6);
            // Keys 10, 12, 15 occupy slots 0, 2, 5 (LSB-first bit order).
            assert_eq!(membership.presence, vec![0b0010_0101u8]);
        }

        #[test]
        fn hash_table_lookup_expr_try_from_proto_hash_map_membership() {
            let schema = lookup_schema();
            let decoder = TestDecoder;
            let ctx = test_decode_ctx(&schema, &decoder);
            let proto = lookup_expr_proto(
                protobuf::physical_hash_table_lookup_expr_node::Map::HashMapMembership(
                    protobuf::HashMapMembership {
                        build_hashes: hashes_for(&[1, 3], 42),
                    },
                ),
            );

            let expr = HashTableLookupExpr::try_from_proto(&proto, &ctx).unwrap();
            let expr = expr.downcast_ref::<HashTableLookupExpr>().unwrap();

            assert_eq!(expr.random_state.seed(), 42);
            assert_eq!(expr.description, "hash_lookup");
            assert_eq!(expr.on_columns.len(), 1);
            assert!(matches!(
                expr.map,
                HashTableLookupExprMap::MembershipOnlyHashMap(_)
            ));
            // Probe keys are hashed with the deserialized seed, so 1 and 3
            // (whose hashes were serialized) match and 2 and 4 do not.
            // Under force_hash_collisions every value hashes identically,
            // so the filter degenerates to all-true and this doesn't hold.
            #[cfg(not(feature = "force_hash_collisions"))]
            assert_eq!(
                eval_lookup(expr, &probe_batch(&[1, 2, 3, 4])),
                [true, false, true, false]
            );
        }

        #[test]
        fn hash_table_lookup_expr_try_from_proto_array_map_membership() {
            let schema = lookup_schema();
            let decoder = TestDecoder;
            let ctx = test_decode_ctx(&schema, &decoder);
            let proto = lookup_expr_proto(
                protobuf::physical_hash_table_lookup_expr_node::Map::ArrayMapMembership(
                    protobuf::ArrayMapMembership {
                        offset: 10,
                        num_slots: 6,
                        presence: vec![0b0010_0101u8],
                    },
                ),
            );

            let expr = HashTableLookupExpr::try_from_proto(&proto, &ctx).unwrap();
            let expr = expr.downcast_ref::<HashTableLookupExpr>().unwrap();

            assert!(matches!(
                expr.map,
                HashTableLookupExprMap::MembershipOnlyArrayMap(_)
            ));
            // In-range hits (10, 12, 15), in-range misses (11, 14), and
            // out-of-range probes on both sides (9, 16).
            assert_eq!(
                eval_lookup(expr, &probe_batch(&[9, 10, 11, 12, 14, 15, 16])),
                [false, true, false, true, false, true, false]
            );
        }

        #[test]
        fn hash_table_lookup_expr_try_from_proto_rejects_wrong_node_type() {
            let schema = Schema::empty();
            let decoder = TestDecoder;
            let ctx = test_decode_ctx(&schema, &decoder);
            let proto = column_node("a", 0);

            let err = HashTableLookupExpr::try_from_proto(&proto, &ctx).unwrap_err();
            assert!(
                err.to_string()
                    .contains("PhysicalExprNode is not a HashTableLookupExpr"),
                "{err}"
            );
        }

        #[test]
        fn hash_table_lookup_expr_try_from_proto_rejects_missing_map() {
            let schema = lookup_schema();
            let decoder = TestDecoder;
            let ctx = test_decode_ctx(&schema, &decoder);
            let proto = protobuf::PhysicalExprNode {
                expr_id: None,
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::HashTableLookupExpr(
                        protobuf::PhysicalHashTableLookupExprNode {
                            on_columns: vec![column_node("a", 0)],
                            seed0: 42,
                            description: "hash_lookup".to_string(),
                            map: None,
                        },
                    ),
                ),
            };

            let err = HashTableLookupExpr::try_from_proto(&proto, &ctx).unwrap_err();
            assert!(
                err.to_string().contains("HashTableLookupExpr has no map"),
                "{err}"
            );
        }

        #[test]
        fn hash_table_lookup_expr_try_from_proto_rejects_bad_presence_length() {
            let schema = lookup_schema();
            let decoder = TestDecoder;
            let ctx = test_decode_ctx(&schema, &decoder);
            // 6 slots need exactly 1 presence byte; send 2.
            let proto = lookup_expr_proto(
                protobuf::physical_hash_table_lookup_expr_node::Map::ArrayMapMembership(
                    protobuf::ArrayMapMembership {
                        offset: 10,
                        num_slots: 6,
                        presence: vec![0b0010_0101u8, 0],
                    },
                ),
            );

            assert!(HashTableLookupExpr::try_from_proto(&proto, &ctx).is_err());
        }

        #[test]
        fn hash_table_lookup_expr_roundtrip_hash_map() {
            let build_hashes = hashes_for(&[1, 3, 5], 42);
            let map = Arc::new(Map::HashMap(Box::new(join_hash_map_with_hashes(
                &build_hashes,
            ))));
            let expr = HashTableLookupExpr::new(
                vec![Arc::new(Column::new("a", 0))],
                SeededRandomState::with_seed(42),
                map,
                "hash_lookup".to_string(),
            );

            let encoder = TestEncoder;
            let proto = expr
                .try_to_proto(&PhysicalExprEncodeCtx::new(&encoder))
                .unwrap()
                .unwrap();
            let schema = lookup_schema();
            let decoder = TestDecoder;
            let decoded = HashTableLookupExpr::try_from_proto(
                &proto,
                &test_decode_ctx(&schema, &decoder),
            )
            .unwrap();

            let batch = probe_batch(&[0, 1, 2, 3, 4, 5, 6]);
            #[cfg(not(feature = "force_hash_collisions"))]
            assert_eq!(
                eval_lookup(&expr, &batch),
                [false, true, false, true, false, true, false]
            );
            assert_eq!(
                eval_lookup(&expr, &batch),
                eval_lookup(decoded.as_ref(), &batch)
            );
        }

        #[test]
        fn hash_table_lookup_expr_roundtrip_array_map_sign_crossing_range() {
            // A build-side range that crosses zero wraps mid-range in the
            // u64 key domain (-5 maps to a slot below 0's slot); roundtrip
            // must preserve membership across the wrap.
            let build: ArrayRef = Arc::new(Int64Array::from(vec![-5i64, 0, 5]));
            let array_map = ArrayMap::try_new(&build, (-5i64) as u64, 5).unwrap();
            let expr = HashTableLookupExpr::new(
                vec![Arc::new(Column::new("a", 0))],
                SeededRandomState::with_seed(42),
                Arc::new(Map::ArrayMap(array_map)),
                "hash_lookup".to_string(),
            );

            let encoder = TestEncoder;
            let proto = expr
                .try_to_proto(&PhysicalExprEncodeCtx::new(&encoder))
                .unwrap()
                .unwrap();
            let schema = lookup_schema();
            let decoder = TestDecoder;
            let decoded = HashTableLookupExpr::try_from_proto(
                &proto,
                &test_decode_ctx(&schema, &decoder),
            )
            .unwrap();

            let batch = probe_batch(&[-6, -5, -1, 0, 1, 5, 6]);
            assert_eq!(
                eval_lookup(&expr, &batch),
                [false, true, false, true, false, true, false]
            );
            assert_eq!(
                eval_lookup(&expr, &batch),
                eval_lookup(decoded.as_ref(), &batch)
            );

            // Re-encoding the membership-only map is byte-identical.
            let reencoded = decoded
                .try_to_proto(&PhysicalExprEncodeCtx::new(&encoder))
                .unwrap()
                .unwrap();
            assert_eq!(proto, reencoded);
        }

        #[test]
        fn hash_table_lookup_expr_roundtrip_hash_map_u64() {
            let build_hashes = hashes_for(&[2, 4], 42);
            let map = Arc::new(Map::HashMap(Box::new(join_hash_map_u64_with_hashes(
                &build_hashes,
            ))));
            let expr = HashTableLookupExpr::new(
                vec![Arc::new(Column::new("a", 0))],
                SeededRandomState::with_seed(42),
                map,
                "hash_lookup".to_string(),
            );

            let encoder = TestEncoder;
            let proto = expr
                .try_to_proto(&PhysicalExprEncodeCtx::new(&encoder))
                .unwrap()
                .unwrap();
            let schema = lookup_schema();
            let decoder = TestDecoder;
            let decoded = HashTableLookupExpr::try_from_proto(
                &proto,
                &test_decode_ctx(&schema, &decoder),
            )
            .unwrap();

            let batch = probe_batch(&[1, 2, 3, 4, 5]);
            #[cfg(not(feature = "force_hash_collisions"))]
            assert_eq!(
                eval_lookup(&expr, &batch),
                [false, true, false, true, false]
            );
            assert_eq!(
                eval_lookup(&expr, &batch),
                eval_lookup(decoded.as_ref(), &batch)
            );
        }

        #[test]
        fn hash_table_lookup_expr_roundtrip_multi_column() {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ]);
            // Build side has key pairs (1, 10) and (7, 70)
            let build_a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 7]));
            let build_b: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 70]));
            let mut build_hashes = vec![0u64; 2];
            create_hashes(
                &[build_a, build_b],
                SeededRandomState::with_seed(42).random_state(),
                &mut build_hashes,
            )
            .unwrap();
            let map = Arc::new(Map::HashMap(Box::new(join_hash_map_with_hashes(
                &build_hashes,
            ))));
            let expr = HashTableLookupExpr::new(
                vec![Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 1))],
                SeededRandomState::with_seed(42),
                map,
                "hash_lookup".to_string(),
            );

            let encoder = TestEncoder;
            let proto = expr
                .try_to_proto(&PhysicalExprEncodeCtx::new(&encoder))
                .unwrap()
                .unwrap();
            match proto.expr_type.as_ref().unwrap() {
                protobuf::physical_expr_node::ExprType::HashTableLookupExpr(node) => {
                    assert_eq!(node.on_columns.len(), 2)
                }
                other => panic!("expected HashTableLookupExpr, got {other:?}"),
            }
            let decoder = TestDecoder;
            let decoded = HashTableLookupExpr::try_from_proto(
                &proto,
                &test_decode_ctx(&schema, &decoder),
            )
            .unwrap();

            // Present pairs (1, 10) and (7, 70) match; the cross-pairings
            // (1, 70) and (7, 10) must not.
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(Int64Array::from(vec![1i64, 1, 7, 7])),
                    Arc::new(Int64Array::from(vec![10i64, 70, 10, 70])),
                ],
            )
            .unwrap();
            #[cfg(not(feature = "force_hash_collisions"))]
            assert_eq!(eval_lookup(&expr, &batch), [true, false, false, true]);
            assert_eq!(
                eval_lookup(&expr, &batch),
                eval_lookup(decoded.as_ref(), &batch)
            );
        }

        fn lookup_schema() -> Schema {
            Schema::new(vec![Field::new("a", DataType::Int64, false)])
        }

        fn probe_batch(values: &[i64]) -> RecordBatch {
            RecordBatch::try_new(
                Arc::new(lookup_schema()),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )
            .unwrap()
        }

        fn eval_lookup(expr: &dyn PhysicalExpr, batch: &RecordBatch) -> Vec<bool> {
            let array = expr
                .evaluate(batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            let bools = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            bools.iter().map(|v| v.unwrap()).collect()
        }

        /// Build a `JoinHashMapU32` containing exactly the given distinct hashes.
        fn join_hash_map_with_hashes(hashes: &[u64]) -> JoinHashMapU32 {
            let mut table = HashTable::with_capacity(hashes.len());
            for (i, h) in hashes.iter().enumerate() {
                table.insert_unique(*h, (*h, i as u32 + 1), |(h, _)| *h);
            }
            JoinHashMapU32::new(table, vec![0; hashes.len()])
        }

        /// Build a `JoinHashMapU64` containing exactly the given distinct hashes.
        fn join_hash_map_u64_with_hashes(hashes: &[u64]) -> JoinHashMapU64 {
            let mut table = HashTable::with_capacity(hashes.len());
            for (i, h) in hashes.iter().enumerate() {
                table.insert_unique(*h, (*h, i as u64 + 1), |(h, _)| *h);
            }
            JoinHashMapU64::new(table, vec![0; hashes.len()])
        }

        /// Hash `values` the same way `HashTableLookupExpr::evaluate` hashes
        /// probe keys, so tests can construct build hashes that match.
        fn hashes_for(values: &[i64], seed: u64) -> Vec<u64> {
            let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
            let mut buf = vec![0u64; values.len()];
            create_hashes(
                &[array],
                SeededRandomState::with_seed(seed).random_state(),
                &mut buf,
            )
            .unwrap();
            buf
        }

        fn column_node(name: &str, index: u32) -> protobuf::PhysicalExprNode {
            protobuf::PhysicalExprNode {
                expr_id: None,
                expr_type: Some(protobuf::physical_expr_node::ExprType::Column(
                    protobuf::PhysicalColumn {
                        name: name.to_string(),
                        index,
                    },
                )),
            }
        }

        fn lookup_expr_proto(
            map: protobuf::physical_hash_table_lookup_expr_node::Map,
        ) -> protobuf::PhysicalExprNode {
            protobuf::PhysicalExprNode {
                expr_id: None,
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::HashTableLookupExpr(
                        protobuf::PhysicalHashTableLookupExprNode {
                            on_columns: vec![column_node("a", 0)],
                            seed0: 42,
                            description: "hash_lookup".to_string(),
                            map: Some(map),
                        },
                    ),
                ),
            }
        }
    }

    #[test]
    fn test_hash_table_lookup_expr_eq_same() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));
        let hash_map =
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(10))));

        let expr1 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
        );

        assert_eq!(expr1, expr2);
    }

    #[test]
    fn test_hash_table_lookup_expr_eq_different_columns() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));
        let col_b: PhysicalExprRef = Arc::new(Column::new("b", 1));

        let hash_map =
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(10))));

        let expr1 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_b)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
        );

        assert_ne!(expr1, expr2);
    }

    #[test]
    fn test_hash_table_lookup_expr_eq_different_description() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));
        let hash_map =
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(10))));

        let expr1 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup_one".to_string(),
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup_two".to_string(),
        );

        assert_ne!(expr1, expr2);
    }

    #[test]
    fn test_hash_table_lookup_expr_eq_different_hash_map() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));

        // Two different Arc pointers (even with same content) should not be equal
        let hash_map1 =
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(10))));
        let hash_map2 =
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(10))));
        let expr1 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            hash_map1,
            "lookup".to_string(),
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            hash_map2,
            "lookup".to_string(),
        );

        // Different Arc pointers means not equal (uses Arc::ptr_eq)
        assert_ne!(expr1, expr2);
    }

    #[test]
    fn test_hash_table_lookup_expr_hash_consistency() {
        let col_a: PhysicalExprRef = Arc::new(Column::new("a", 0));
        let hash_map =
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(10))));

        let expr1 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
        );

        // Equal expressions should have equal hashes
        assert_eq!(expr1, expr2);
        assert_eq!(compute_hash(&expr1), compute_hash(&expr2));
    }
}
