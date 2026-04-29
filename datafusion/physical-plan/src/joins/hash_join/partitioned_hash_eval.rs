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
    array::{ArrayRef, BooleanArray, BooleanBufferBuilder, UInt64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::{create_hashes, with_hashes};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::{
    DynHash, PhysicalExpr, PhysicalExprRef,
};

use crate::joins::Map;

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
            map,
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
        // Note that we compare hash_map by pointer equality.
        // Actually comparing the contents of the hash maps would be expensive.
        // The way these hash maps are used in actuality is that HashJoinExec creates
        // one per partition per query execution, thus it is never possible for two different
        // hash maps to have the same content in practice.
        // Theoretically this is a public API and users could create identical hash maps,
        // but that seems unlikely and not worth paying the cost of deep comparison all the time.
        Arc::as_ptr(&self.map).hash(state);
    }
}

impl PartialEq for HashTableLookupExpr {
    fn eq(&self, other: &Self) -> bool {
        // Note that we compare hash_map by pointer equality.
        // Actually comparing the contents of the hash maps would be expensive.
        // The way these hash maps are used in actuality is that HashJoinExec creates
        // one per partition per query execution, thus it is never possible for two different
        // hash maps to have the same content in practice.
        // Theoretically this is a public API and users could create identical hash maps,
        // but that seems unlikely and not worth paying the cost of deep comparison all the time.
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
        Ok(Arc::new(HashTableLookupExpr::new(
            children,
            self.random_state.clone(),
            Arc::clone(&self.map),
            self.description.clone(),
        )))
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

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

/// Physical expression that probes the same join keys against multiple [`Map`]s
/// and returns `true` for any row whose join keys match at least one map.
///
/// Equivalent to `OR`-ing several [`HashTableLookupExpr`]s but evaluates
/// `create_hashes` exactly once for the whole batch — every [`Map::HashMap`]
/// probe shares that hash buffer. Used for the global-first dynamic filter
/// when every reported partition uses a hash-table pushdown strategy.
pub struct MultiMapLookupExpr {
    /// Columns in the ON clause used to compute the join key for lookups
    on_columns: Vec<PhysicalExprRef>,
    /// Random state for hashing — every map must have been built with the
    /// same `RandomState`, otherwise the shared hash buffer is meaningless.
    random_state: SeededRandomState,
    /// Maps to OR over (each is one partition's build-side data)
    maps: Vec<Arc<Map>>,
    /// Description for display
    description: String,
}

impl MultiMapLookupExpr {
    /// Create a new MultiMapLookupExpr.
    ///
    /// `maps` is the (per-partition) sequence of build-side maps to probe.
    /// All `Map::HashMap` entries are expected to use the same `random_state`;
    /// `Map::ArrayMap` entries do not consume hashes and are queried via
    /// `contain_keys`.
    pub fn new(
        on_columns: Vec<PhysicalExprRef>,
        random_state: SeededRandomState,
        maps: Vec<Arc<Map>>,
        description: String,
    ) -> Self {
        Self {
            on_columns,
            random_state,
            maps,
            description,
        }
    }
}

impl std::fmt::Debug for MultiMapLookupExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self
            .on_columns
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            "{}({cols}, [{}], maps={})",
            self.description,
            self.random_state.seed(),
            self.maps.len()
        )
    }
}

impl Hash for MultiMapLookupExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.on_columns.dyn_hash(state);
        self.description.hash(state);
        self.random_state.seed().hash(state);
        // See HashTableLookupExpr — pointer identity is what we use for Maps.
        for map in &self.maps {
            Arc::as_ptr(map).hash(state);
        }
    }
}

impl PartialEq for MultiMapLookupExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_columns == other.on_columns
            && self.description == other.description
            && self.random_state.seed() == other.random_state.seed()
            && self.maps.len() == other.maps.len()
            && self
                .maps
                .iter()
                .zip(other.maps.iter())
                .all(|(a, b)| Arc::ptr_eq(a, b))
    }
}

impl Eq for MultiMapLookupExpr {}

impl Display for MultiMapLookupExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl PhysicalExpr for MultiMapLookupExpr {
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.on_columns.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(MultiMapLookupExpr::new(
            children,
            self.random_state.clone(),
            self.maps.clone(),
            self.description.clone(),
        )))
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let join_keys = evaluate_columns(&self.on_columns, batch)?;

        if self.maps.is_empty() || num_rows == 0 {
            // No maps to probe — this should not happen in practice but
            // returning all-false matches the semantics of an empty `OR`.
            let buffer = BooleanBufferBuilder::new(num_rows);
            let mut buffer = buffer;
            buffer.append_n(num_rows, false);
            return Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
                buffer.finish(),
                None,
            ))));
        }

        // Whether any map needs hashes. We only compute hashes if at least
        // one map is a HashMap.
        let needs_hashes = self
            .maps
            .iter()
            .any(|m| matches!(m.as_ref(), Map::HashMap(_)));

        // Result buffer accumulates the OR of every map's `contain_*` result.
        let mut result = vec![false; num_rows];

        let process_one_map =
            |result: &mut [bool], map: &Map, hashes: Option<&[u64]>| -> Result<()> {
                match map {
                    Map::HashMap(hm) => {
                        let hashes = hashes
                            .expect("hashes computed when at least one map is a HashMap");
                        let arr = hm.contain_hashes(hashes);
                        // OR into the running result. `arr` has no nulls
                        // (`contain_hashes` returns a non-nullable BooleanArray).
                        for (slot, hit) in result.iter_mut().zip(arr.values().iter()) {
                            *slot |= hit;
                        }
                    }
                    Map::ArrayMap(am) => {
                        let arr = am.contain_keys(&join_keys)?;
                        for (slot, hit) in result.iter_mut().zip(arr.values().iter()) {
                            *slot |= hit;
                        }
                    }
                }
                Ok(())
            };

        if needs_hashes {
            // Compute the join-key hashes ONCE, then probe every HashMap
            // against the same buffer. ArrayMap probes ignore the hashes.
            with_hashes(&join_keys, self.random_state.random_state(), |hashes| {
                for map in &self.maps {
                    process_one_map(&mut result, map, Some(hashes))?;
                }
                Ok::<(), datafusion_common::DataFusionError>(())
            })?;
        } else {
            for map in &self.maps {
                process_one_map(&mut result, map, None)?;
            }
        }

        let mut buffer = BooleanBufferBuilder::new(num_rows);
        for hit in &result {
            buffer.append(*hit);
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::new(
            buffer.finish(),
            None,
        ))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
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
