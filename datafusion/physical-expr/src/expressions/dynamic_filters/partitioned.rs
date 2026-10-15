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

//! Partition metadata retained alongside a dynamic filter's total expression.

use std::sync::Arc;

use crate::{
    LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr, RangePartitioning,
};
use datafusion_common::{Result, internal_datafusion_err, internal_err};

/// The partition predicates and their routing contract. The total expression is
/// stored separately in `Inner::expr`, so unpartitioned consumers keep using it.
#[derive(Clone, Debug)]
pub(super) struct PartitionedFilterExpr {
    pub partitioning: Partitioning,
    pub filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl PartitionedFilterExpr {
    pub fn try_new(
        partitioning: Partitioning,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        if !matches!(
            partitioning,
            Partitioning::Hash(_, _) | Partitioning::Range(_)
        ) {
            return internal_err!(
                "Partitioned dynamic filters require Hash or Range partitioning"
            );
        }
        if filters.is_empty() || partitioning.partition_count() != filters.len() {
            return internal_err!(
                "Dynamic filter partition count {} does not match partitioning {}",
                filters.len(),
                partitioning.partition_count()
            );
        }
        Ok(Self {
            partitioning,
            filters,
        })
    }
}

/// Compare the actual routing, ignoring unused samples retained for scaling.
/// Unknown and round-robin partitioning never prove where an individual row is.
pub(super) fn same_partitioning(left: &Partitioning, right: &Partitioning) -> bool {
    match (left, right) {
        (Partitioning::Range(left), Partitioning::Range(right)) => {
            left.has_same_layout(right)
        }
        (Partitioning::Hash(_, _), Partitioning::Hash(_, _)) => left == right,
        _ => false,
    }
}

pub(super) fn map_partitioning(
    partitioning: &Partitioning,
    mut map: impl FnMut(Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>>,
) -> Result<Partitioning> {
    Ok(match partitioning {
        Partitioning::Hash(exprs, count) => Partitioning::Hash(
            exprs
                .iter()
                .map(|expr| map(Arc::clone(expr)))
                .collect::<Result<_>>()?,
            *count,
        ),
        Partitioning::Range(range) => {
            Partitioning::Range(RangePartitioning::try_new_with_samples(
                LexOrdering::new(
                    range
                        .ordering()
                        .iter()
                        .map(|sort| {
                            Ok(PhysicalSortExpr::new(
                                map(Arc::clone(&sort.expr))?,
                                sort.options,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?,
                )
                .ok_or_else(|| internal_datafusion_err!("Missing range ordering"))?,
                range.samples().to_vec(),
                range.partition_count(),
            )?)
        }
        other => other.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BinaryExpr, Column, DynamicFilterPhysicalExpr, lit};
    use datafusion_common::{ScalarValue, SplitPoint};
    use datafusion_expr::Operator;

    fn range(key: Arc<dyn PhysicalExpr>, split: i64) -> Partitioning {
        Partitioning::Range(
            RangePartitioning::try_new(
                [PhysicalSortExpr::new_default(key)].into(),
                vec![SplitPoint::new(vec![ScalarValue::Int64(Some(split))])],
            )
            .unwrap(),
        )
    }

    #[test]
    fn partition_views_observe_updates_and_completion() -> Result<()> {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let partitioning = range(Arc::clone(&key), 10);
        let filter = DynamicFilterPhysicalExpr::new_partitioned(
            vec![key],
            partitioning.clone(),
            lit(true),
        )?;
        let first = filter.for_partition(0)?;
        let empty = filter.for_partition(1)?;
        assert_eq!(empty.current()?.to_string(), "true");
        filter.update_partitioned(
            partitioning,
            vec![lit(true), lit(false)],
            lit(true),
        )?;
        assert_eq!(first.current()?.to_string(), "true");
        assert_eq!(empty.current()?.to_string(), "false");
        assert_eq!(filter.current()?.to_string(), "true");
        assert_eq!(first.expression_id(), filter.expression_id());
        assert_ne!(first, empty);
        // A uniform update must also invalidate each partition view's cache.
        filter.update(lit(false))?;
        assert_eq!(first.current()?.to_string(), "false");
        assert_eq!(empty.current()?.to_string(), "false");
        filter.mark_complete();
        assert!(first.is_complete());
        assert!(empty.is_complete());
        Ok(())
    }

    #[test]
    fn partition_metadata_and_predicates_follow_projection() -> Result<()> {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 1));
        let partitioning = range(Arc::clone(&key), 10);
        let filter = Arc::new(DynamicFilterPhysicalExpr::new_partitioned(
            vec![Arc::clone(&key)],
            partitioning.clone(),
            lit(true),
        )?);
        let projected_key: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new("projected_k", 0));
        // Project before binding the view, as happens during pushdown.
        let projected =
            Arc::clone(&filter).with_new_children(vec![Arc::clone(&projected_key)])?;
        let projected = projected
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .unwrap();
        assert_eq!(
            projected.partitioning()?,
            Some(range(Arc::clone(&projected_key), 10))
        );
        let view = Arc::new(projected.for_partition(1)?);
        // Adapt again after binding, as happens with per-file schema adaptation.
        let file_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("file_k", 2));
        let adapted = view.with_new_children(vec![Arc::clone(&file_key)])?;
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(key, Operator::Eq, lit(12_i64)));
        filter.update_partitioned(
            partitioning,
            vec![lit(false), Arc::clone(&predicate)],
            predicate,
        )?;
        let adapted = adapted.downcast_ref::<DynamicFilterPhysicalExpr>().unwrap();
        assert_eq!(adapted.partitioning()?, Some(range(file_key, 10)));
        assert_eq!(adapted.current()?.to_string(), "file_k@2 = 12");
        assert_eq!(projected.current()?.to_string(), "projected_k@0 = 12");
        Ok(())
    }

    #[test]
    fn routing_cannot_change_after_consumers_bind() -> Result<()> {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let range_filter = DynamicFilterPhysicalExpr::new_partitioned(
            vec![Arc::clone(&key)],
            range(Arc::clone(&key), 10),
            lit(true),
        )?;
        let view = range_filter.for_partition(0)?;
        assert!(
            range_filter
                .update_partitioned(
                    range(Arc::clone(&key), 20),
                    vec![lit(false), lit(false)],
                    lit(false),
                )
                .is_err()
        );
        assert_eq!(view.current()?.to_string(), "true");
        let hash_filter = DynamicFilterPhysicalExpr::new_partitioned(
            vec![Arc::clone(&key)],
            Partitioning::Hash(vec![Arc::clone(&key)], 2),
            lit(true),
        )?;
        assert!(
            hash_filter
                .update_partitioned(
                    Partitioning::Hash(vec![key], 4),
                    vec![lit(false); 4],
                    lit(false),
                )
                .is_err()
        );
        assert!(range_filter.for_partition(2).is_err());
        assert!(
            DynamicFilterPhysicalExpr::new(vec![], lit(true))
                .for_partition(0)
                .is_err()
        );
        assert!(
            DynamicFilterPhysicalExpr::new_partitioned(
                vec![],
                Partitioning::UnknownPartitioning(2),
                lit(true),
            )
            .is_err()
        );
        Ok(())
    }
}
