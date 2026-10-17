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

use arrow::{
    array::{ArrayRef, BooleanArray, UInt64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::{create_hashes, with_hashes};
use datafusion_common::{Result, internal_err};
use datafusion_expr::ColumnarValue;
use datafusion_expr_common::dyn_eq::DynHash;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, PhysicalExprRef};

use crate::joins::Map;
use crate::joins::key_range_bitmap::KeyRangeBitmap;

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
/// Returns a [`BooleanArray`](arrow::array::BooleanArray) indicating if join keys (from `on_columns`) exist in the map.
// TODO: rename to MapLookupExpr
pub struct HashTableLookupExpr {
    /// Columns in the ON clause used to compute the join key for lookups
    on_columns: Vec<PhysicalExprRef>,
    /// Random state for hashing (with seeds preserved for serialization)
    random_state: SeededRandomState,
    /// Map to check against (hash table or array map)
    map: Arc<Map>,
    /// Description for display
    description: String,
    /// Shared pruning-only view of the build side's key range
    pruning_bitmap: Option<Arc<KeyRangeBitmap>>,
}
impl HashTableLookupExpr {
    /// Create a new HashTableLookupExpr
    ///
    /// # Arguments
    /// * `on_columns` - Columns in the ON clause used to compute the join key
    /// * `random_state` - SeededRandomState for hashing
    /// * `map` - Map to check membership (hash table or array map)
    /// * `description` - Description for debugging
    /// * `pruning_bitmap` - key-range summary for pruning only, or `None`
    ///
    /// # Public Only for Internal Use:
    /// `datafusion-proto` tests require this constructor, but it is not part of
    /// the supported public API.
    #[doc(hidden)]
    pub fn new(
        on_columns: Vec<PhysicalExprRef>,
        random_state: SeededRandomState,
        map: Arc<Map>,
        description: String,
        pruning_bitmap: Option<Arc<KeyRangeBitmap>>,
    ) -> Self {
        Self {
            on_columns,
            random_state,
            map,
            description,
            pruning_bitmap,
        }
    }

    /// Which parts of this build side's key range hold keys, for container pruning.
    pub fn pruning_bitmap(&self) -> Option<&Arc<KeyRangeBitmap>> {
        self.pruning_bitmap.as_ref()
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
        // Hash immutable map identity, consistently with pointer equality below.
        Arc::as_ptr(&self.map).hash(state);
    }
}

impl PartialEq for HashTableLookupExpr {
    fn eq(&self, other: &Self) -> bool {
        // Sharing an immutable map implies equal contents, including across
        // prepared-build consumers. Distinct maps need no expensive deep comparison.
        self.on_columns == other.on_columns
            && self.description == other.description
            && self.random_state.seed() == other.random_state.seed()
            && Arc::ptr_eq(&self.map, &other.map)
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
            map: Arc::clone(&self.map),
            description: self.description.clone(),
            pruning_bitmap: self.pruning_bitmap.clone(),
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

        match self.map.as_ref() {
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
        }
    }
    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        _ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
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
            pruning_bitmap: _,
        } = self;

        // HashTableLookupExpr holds a runtime Arc<Map> (the build-side hash
        // table) that cannot be serialized, so it is replaced with lit(true).
        //
        // Dynamic filtering is a performance optimisation only — replacing the
        // lookup with lit(true) preserves correctness by allowing all rows
        // through.
        //
        // If a plan is serialized before execution, HashTableLookupExpr is not
        // yet present in the dynamic filter expression.
        //
        // If a plan is serialized after execution, any runtime-created
        // HashTableLookupExpr is replaced during serialization. Re-executing
        // the plan requires reset_state(), after which HashJoinExec rebuilds
        // fresh dynamic filters at runtime.
        let value = datafusion_proto_common::ScalarValue {
            value: Some(datafusion_proto_common::scalar_value::Value::BoolValue(
                true,
            )),
        };
        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(ExprType::Literal(value)),
        }))
    }
    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

/// Build-side lookup for one partition of a `Partitioned` hash join.
pub(crate) enum PartitionLookup {
    /// The partition's hash table; probe rows routed here are looked up in it.
    Map(Arc<Map>),
    /// The partition is known empty (`false`) or its contents are unknown (`true`).
    Const(bool),
}

/// Routes each probe row to the hash table of the build partition that would
/// contain it and looks the row up there.
///
/// This replaces `CASE hash(keys) % n WHEN i THEN lookup_i ... END` for
/// partitioned joins. `CaseExpr` evaluates each branch by filtering the batch
/// down to that branch's rows and merging the partial results, which copies
/// every column of the batch once per branch. This expression computes the
/// routing and lookup hashes once, buckets row indices by partition and only
/// gathers the 8-byte hashes, so the batch columns are never copied.
pub(crate) struct PartitionedLookupExpr {
    on_columns: Vec<PhysicalExprRef>,
    routing_state: SeededRandomState,
    lookup_state: SeededRandomState,
    partitions: Vec<PartitionLookup>,
}

impl PartitionedLookupExpr {
    pub(crate) fn new(
        on_columns: Vec<PhysicalExprRef>,
        routing_state: SeededRandomState,
        lookup_state: SeededRandomState,
        partitions: Vec<PartitionLookup>,
    ) -> Self {
        Self {
            on_columns,
            routing_state,
            lookup_state,
            partitions,
        }
    }
}

impl std::fmt::Debug for PartitionedLookupExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .on_columns
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "partitioned_hash_lookup({cols}, {} partitions)",
            self.partitions.len()
        )
    }
}

impl Display for PartitionedLookupExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partitioned_hash_lookup({})", self.partitions.len())
    }
}

impl Hash for PartitionedLookupExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.on_columns.dyn_hash(state);
        self.routing_state.seed().hash(state);
        self.lookup_state.seed().hash(state);
        for partition in &self.partitions {
            match partition {
                PartitionLookup::Map(map) => Arc::as_ptr(map).hash(state),
                PartitionLookup::Const(value) => value.hash(state),
            }
        }
    }
}

impl PartialEq for PartitionedLookupExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_columns == other.on_columns
            && self.routing_state.seed() == other.routing_state.seed()
            && self.lookup_state.seed() == other.lookup_state.seed()
            && self.partitions.len() == other.partitions.len()
            && self
                .partitions
                .iter()
                .zip(&other.partitions)
                .all(|(a, b)| match (a, b) {
                    (PartitionLookup::Map(a), PartitionLookup::Map(b)) => {
                        Arc::ptr_eq(a, b)
                    }
                    (PartitionLookup::Const(a), PartitionLookup::Const(b)) => a == b,
                    _ => false,
                })
    }
}

impl Eq for PartitionedLookupExpr {}

impl PhysicalExpr for PartitionedLookupExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let keys = evaluate_columns(&self.on_columns, batch)?;
        let num_rows = batch.num_rows();
        let num_partitions = self.partitions.len() as u64;

        // Same routing as RepartitionExec: hash(keys) % partitions.
        let mut routing_hashes = vec![0u64; num_rows];
        create_hashes(
            &keys,
            self.routing_state.random_state(),
            &mut routing_hashes,
        )?;
        let mut rows_by_partition: Vec<Vec<usize>> =
            vec![Vec::new(); self.partitions.len()];
        for (row, hash) in routing_hashes.iter().enumerate() {
            rows_by_partition[(hash % num_partitions) as usize].push(row);
        }

        with_hashes(&keys, self.lookup_state.random_state(), |lookup_hashes| {
            let mut result = vec![false; num_rows];
            for (partition, rows) in self.partitions.iter().zip(&rows_by_partition) {
                match partition {
                    PartitionLookup::Const(value) => {
                        for &row in rows {
                            result[row] = *value;
                        }
                    }
                    PartitionLookup::Map(map) => {
                        let Map::HashMap(map) = map.as_ref() else {
                            return internal_err!(
                                "PartitionedLookupExpr only supports hash map partitions"
                            );
                        };
                        let hashes: Vec<u64> =
                            rows.iter().map(|&row| lookup_hashes[row]).collect();
                        let hits = map.contain_hashes(&hashes);
                        for (i, &row) in rows.iter().enumerate() {
                            result[row] = hits.value(i);
                        }
                    }
                }
            }
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(result))))
        })
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.on_columns.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let partitions = self
            .partitions
            .iter()
            .map(|p| match p {
                PartitionLookup::Map(map) => PartitionLookup::Map(Arc::clone(map)),
                PartitionLookup::Const(value) => PartitionLookup::Const(*value),
            })
            .collect();
        Ok(Arc::new(PartitionedLookupExpr {
            on_columns: children,
            routing_state: self.routing_state.clone(),
            lookup_state: self.lookup_state.clone(),
            partitions,
        }))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        _ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;
        use datafusion_proto_models::protobuf::physical_expr_node::ExprType;

        // Like HashTableLookupExpr: the hash tables cannot be serialized, and
        // dynamic filters are an optimization only, so emit lit(true).
        let value = datafusion_proto_common::ScalarValue {
            value: Some(datafusion_proto_common::scalar_value::Value::BoolValue(
                true,
            )),
        };
        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(ExprType::Literal(value)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::joins::join_hash_map::{JoinHashMapType, JoinHashMapU32};
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::Column;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    fn compute_hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// The pruning bitmap must survive `with_new_children`, which a scan calls
    /// once per file.
    #[test]
    fn test_pruning_bitmap_shared_with_derived_children() {
        let keys: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![0, 1_000_000]));
        let bitmap = Arc::new(
            KeyRangeBitmap::try_new(
                &keys,
                &ScalarValue::Int64(Some(0)),
                &ScalarValue::Int64(Some(1_000_000)),
                2,
            )
            .expect("two keys a million apart leave prunable gaps"),
        );
        let expr = Arc::new(HashTableLookupExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            SeededRandomState::with_seed(1),
            Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(10)))),
            "hash_lookup".to_string(),
            Some(Arc::clone(&bitmap)),
        ));

        let derived = expr
            .with_new_children(vec![Arc::new(Column::new("a", 7))])
            .unwrap();
        let derived = derived.downcast_ref::<HashTableLookupExpr>().unwrap();
        assert_eq!(derived.children()[0].to_string(), "a@7");
        assert!(Arc::ptr_eq(&bitmap, derived.pruning_bitmap().unwrap()));
    }

    /// Rows must be looked up in the table of the partition RepartitionExec
    /// would send them to, and constant partitions must apply to their rows only.
    #[test]
    fn partitioned_lookup_routes_rows_to_their_partition() {
        let routing = SeededRandomState::with_seed(1);
        let lookup = SeededRandomState::with_seed(2);
        let num_partitions = 3u64;
        let keys: ArrayRef =
            Arc::new(arrow::array::Int32Array::from((0..300).collect::<Vec<_>>()));
        let mut routing_hashes = vec![0u64; 300];
        create_hashes(
            &[Arc::clone(&keys)],
            routing.random_state(),
            &mut routing_hashes,
        )
        .unwrap();
        let mut lookup_hashes = vec![0u64; 300];
        create_hashes(
            &[Arc::clone(&keys)],
            lookup.random_state(),
            &mut lookup_hashes,
        )
        .unwrap();

        // Partition 0 and 1 hold the even keys routed to them; partition 2 is a constant.
        let partitions = (0..num_partitions)
            .map(|p| {
                if p == 2 {
                    return PartitionLookup::Const(true);
                }
                let build: Vec<u64> = (0..300)
                    .filter(|k| k % 2 == 0 && routing_hashes[*k] % num_partitions == p)
                    .map(|k| lookup_hashes[k])
                    .collect();
                let mut map = JoinHashMapU32::with_capacity(build.len());
                map.update_from_iter(Box::new(build.iter().enumerate()), 0);
                PartitionLookup::Map(Arc::new(Map::HashMap(Box::new(map))))
            })
            .collect();
        let expr = PartitionedLookupExpr::new(
            vec![Arc::new(Column::new("k", 0))],
            routing,
            lookup,
            partitions,
        );

        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "k",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![keys]).unwrap();
        let result = expr.evaluate(&batch).unwrap().into_array(300).unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        for (k, routing_hash) in routing_hashes.iter().enumerate() {
            let expected = routing_hash % num_partitions == 2 || k % 2 == 0;
            assert_eq!(result.value(k), expected, "key {k}");
        }
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
            None,
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
            None,
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
            None,
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_b)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
            None,
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
            None,
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup_two".to_string(),
            None,
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
            None,
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            hash_map2,
            "lookup".to_string(),
            None,
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
            None,
        );

        let expr2 = HashTableLookupExpr::new(
            vec![Arc::clone(&col_a)],
            SeededRandomState::with_seed(1),
            Arc::clone(&hash_map),
            "lookup".to_string(),
            None,
        );

        // Equal expressions should have equal hashes
        assert_eq!(expr1, expr2);
        assert_eq!(compute_hash(&expr1), compute_hash(&expr2));
    }
}
