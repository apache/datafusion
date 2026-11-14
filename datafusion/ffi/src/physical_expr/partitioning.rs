// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use datafusion_common::{DataFusionError, ScalarValue, SplitPoint};
use datafusion_physical_expr::{
    Distribution, LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::InputDistributionRequirements;
use stabby::vec::Vec as SVec;

use crate::arrow_wrappers::WrappedArray;
use crate::physical_expr::FFI_PhysicalExpr;
use crate::physical_expr::sort::FFI_PhysicalSortExpr;

/// A stable struct for sharing [`RangePartitioning`] across FFI boundaries.
/// See [`RangePartitioning`] for the descriptions of each field.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_RangePartitioning {
    samples: SVec<SVec<WrappedArray>>,
    ordering: SVec<FFI_PhysicalSortExpr>,
    partition_count: usize,
}

/// A stable struct for sharing [`Partitioning`] across FFI boundaries.
/// See [`Partitioning`] for the meaning of each variant.
#[repr(C)]
#[derive(Debug)]
pub enum FFI_Partitioning {
    RoundRobinBatch(usize),
    Hash(SVec<FFI_PhysicalExpr>, usize),
    UnknownPartitioning(usize),
    Range(FFI_RangePartitioning),
}

impl From<&Partitioning> for FFI_Partitioning {
    fn from(value: &Partitioning) -> Self {
        match value {
            Partitioning::RoundRobinBatch(size) => Self::RoundRobinBatch(*size),
            Partitioning::Hash(exprs, size) => {
                let exprs = exprs
                    .iter()
                    .map(Arc::clone)
                    .map(FFI_PhysicalExpr::from)
                    .collect();
                Self::Hash(exprs, *size)
            }
            Partitioning::Range(range) => {
                // Producer-side conversion should be infallible at ABI boundary
                let samples = range
                    .samples()
                    .iter()
                    .map(|split_point| {
                        split_point
                            .values()
                            .iter()
                            .map(|value| {
                                WrappedArray::try_from(value).expect(
                                    "ScalarValue in RangePartitioning should convert to WrappedArray",
                                )
                            })
                            .collect()
                    })
                    .collect();
                let ordering = range
                    .ordering()
                    .iter()
                    .map(FFI_PhysicalSortExpr::from)
                    .collect();
                Self::Range(FFI_RangePartitioning {
                    samples,
                    ordering,
                    partition_count: range.partition_count(),
                })
            }
            Partitioning::UnknownPartitioning(size) => Self::UnknownPartitioning(*size),
        }
    }
}

impl TryFrom<FFI_Partitioning> for Partitioning {
    type Error = DataFusionError;

    fn try_from(value: FFI_Partitioning) -> Result<Self, Self::Error> {
        Ok(match value {
            FFI_Partitioning::RoundRobinBatch(size) => {
                Partitioning::RoundRobinBatch(size)
            }
            FFI_Partitioning::Hash(exprs, size) => {
                let exprs = exprs.iter().map(<Arc<dyn PhysicalExpr>>::from).collect();
                Self::Hash(exprs, size)
            }
            FFI_Partitioning::Range(range) => {
                let samples = range
                    .samples
                    .into_iter()
                    .map(|split_point| {
                        split_point
                            .into_iter()
                            .map(ScalarValue::try_from)
                            .collect::<Result<Vec<_>, _>>()
                            .map(SplitPoint::new)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let ordering =
                    LexOrdering::new(range.ordering.iter().map(PhysicalSortExpr::from))
                        .ok_or_else(|| {
                        DataFusionError::Internal(
                            "FFI Range partitioning ordering must be non-empty"
                                .to_string(),
                        )
                    })?;

                Self::Range(RangePartitioning::try_new_with_samples(
                    ordering,
                    samples,
                    range.partition_count,
                )?)
            }
            FFI_Partitioning::UnknownPartitioning(size) => {
                Self::UnknownPartitioning(size)
            }
        })
    }
}

/// A stable enum for sharing a [`Distribution`] across FFI boundaries.
///
/// The deprecated [`Distribution::HashPartitioned`] is sent as
/// `KeyPartitioned`, which the optimizer treats identically.
#[repr(C)]
#[derive(Debug)]
pub enum FFI_Distribution {
    UnspecifiedDistribution,
    SinglePartition,
    KeyPartitioned(SVec<FFI_PhysicalExpr>),
}

impl From<&Distribution> for FFI_Distribution {
    #[expect(
        deprecated,
        reason = "HashPartitioned is accepted during the KeyPartitioned migration"
    )]
    fn from(value: &Distribution) -> Self {
        match value {
            Distribution::UnspecifiedDistribution => Self::UnspecifiedDistribution,
            Distribution::SinglePartition => Self::SinglePartition,
            Distribution::HashPartitioned(exprs)
            | Distribution::KeyPartitioned(exprs) => Self::KeyPartitioned(
                exprs
                    .iter()
                    .map(Arc::clone)
                    .map(FFI_PhysicalExpr::from)
                    .collect(),
            ),
        }
    }
}

impl From<&FFI_Distribution> for Distribution {
    fn from(value: &FFI_Distribution) -> Self {
        match value {
            FFI_Distribution::UnspecifiedDistribution => Self::UnspecifiedDistribution,
            FFI_Distribution::SinglePartition => Self::SinglePartition,
            FFI_Distribution::KeyPartitioned(exprs) => Self::KeyPartitioned(
                exprs.iter().map(<Arc<dyn PhysicalExpr>>::from).collect(),
            ),
        }
    }
}

/// A stable struct for sharing [`InputDistributionRequirements`] across FFI
/// boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_InputDistributionRequirements {
    /// One distribution per child.
    distributions: SVec<FFI_Distribution>,
    /// Whether all children must be co-partitioned.
    co_partitioned: bool,
}

impl From<&InputDistributionRequirements> for FFI_InputDistributionRequirements {
    fn from(value: &InputDistributionRequirements) -> Self {
        Self {
            distributions: value
                .per_child_distributions()
                .map(FFI_Distribution::from)
                .collect(),
            co_partitioned: value.is_co_partitioned(),
        }
    }
}

impl From<&FFI_InputDistributionRequirements> for InputDistributionRequirements {
    fn from(value: &FFI_InputDistributionRequirements) -> Self {
        let distributions = value.distributions.iter().map(Distribution::from).collect();
        if value.co_partitioned {
            Self::co_partitioned(distributions)
        } else {
            Self::new(distributions)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::SortOptions;
    use datafusion_common::{Result, ScalarValue, SplitPoint};
    use datafusion_physical_expr::expressions::{Column, lit};
    use datafusion_physical_expr::{
        LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning,
    };
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use stabby::vec::Vec as SVec;

    use crate::physical_expr::partitioning::{FFI_Partitioning, FFI_RangePartitioning};

    fn range_partitioning() -> Result<Partitioning> {
        let a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let b = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let ordering = LexOrdering::new([
            PhysicalSortExpr::new(a, SortOptions::default()),
            PhysicalSortExpr::new(b, SortOptions::new(true, false)),
        ])
        .expect("non-empty ordering");
        let split_points = vec![
            SplitPoint::new(vec![
                ScalarValue::Int64(Some(10)),
                ScalarValue::Utf8(Some("a".to_string())),
            ]),
            SplitPoint::new(vec![
                ScalarValue::Int64(Some(20)),
                ScalarValue::Utf8(Some("b".to_string())),
            ]),
        ];
        Ok(Partitioning::Range(RangePartitioning::try_new(
            ordering,
            split_points,
        )?))
    }

    #[test]
    fn round_trip_ffi_partitioning() -> Result<()> {
        for partitioning in [
            Partitioning::RoundRobinBatch(10),
            Partitioning::Hash(vec![lit(1)], 10),
            Partitioning::UnknownPartitioning(10),
            range_partitioning()?,
        ] {
            let ffi_partitioning: FFI_Partitioning = (&partitioning).into();
            let returned: Partitioning = ffi_partitioning.try_into()?;

            if let Partitioning::UnknownPartitioning(return_size) = returned {
                let Partitioning::UnknownPartitioning(original_size) = partitioning
                else {
                    panic!("Expected unknown partitioning")
                };
                assert_eq!(return_size, original_size);
            } else {
                assert_eq!(partitioning, returned);
            }
        }

        Ok(())
    }

    #[test]
    fn round_trip_ffi_range_partitioning_compound_key() -> Result<()> {
        let partitioning = range_partitioning()?;

        let ffi_partitioning: FFI_Partitioning = (&partitioning).into();
        let returned: Partitioning = ffi_partitioning.try_into()?;
        assert_eq!(partitioning, returned);

        Ok(())
    }

    #[test]
    fn round_trip_ffi_sampled_range_partitioning() -> Result<()> {
        let ordering = LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(
            Column::new("a", 0),
        ))])
        .expect("non-empty ordering");
        let samples = [10, 20, 30, 40, 50]
            .into_iter()
            .map(|value| SplitPoint::new(vec![ScalarValue::Int64(Some(value))]))
            .collect();
        let partitioning = Partitioning::Range(RangePartitioning::try_new_with_samples(
            ordering, samples, 3,
        )?);

        let ffi_partitioning: FFI_Partitioning = (&partitioning).into();
        let returned: Partitioning = ffi_partitioning.try_into()?;
        let Partitioning::Range(returned) = returned else {
            panic!("expected range partitioning");
        };
        let Partitioning::Range(original) = partitioning else {
            panic!("expected range partitioning");
        };

        assert_eq!(returned, original);
        assert_eq!(returned.samples(), original.samples());
        assert_eq!(returned.max_partition_count(), 6);
        assert_eq!(returned.partition_count(), 3);

        Ok(())
    }

    #[test]
    fn ffi_range_partitioning_rejects_empty_ordering() {
        let ffi_partitioning = FFI_Partitioning::Range(FFI_RangePartitioning {
            samples: SVec::new(),
            ordering: SVec::new(),
            partition_count: 1,
        });

        let err = Partitioning::try_from(ffi_partitioning).unwrap_err();
        assert!(
            err.to_string().contains("ordering must be non-empty"),
            "{err}"
        );
    }

    #[test]
    #[expect(
        deprecated,
        reason = "test covers deprecated HashPartitioned being sent as KeyPartitioned"
    )]
    fn round_trip_ffi_input_distribution_requirements() {
        use datafusion_physical_expr::Distribution;
        use datafusion_physical_plan::InputDistributionRequirements;

        use crate::physical_expr::partitioning::FFI_InputDistributionRequirements;

        let a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let b = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let round_trip = |reqs: &InputDistributionRequirements| {
            let ffi = FFI_InputDistributionRequirements::from(reqs);
            InputDistributionRequirements::from(&ffi)
        };

        let single = InputDistributionRequirements::new(vec![
            Distribution::UnspecifiedDistribution,
            Distribution::SinglePartition,
            Distribution::KeyPartitioned(vec![Arc::clone(&a), Arc::clone(&b)]),
        ]);
        let returned = round_trip(&single);
        assert!(!returned.is_co_partitioned());
        assert_eq!(format!("{single:?}"), format!("{returned:?}"));

        let co_partitioned = InputDistributionRequirements::co_partitioned(vec![
            Distribution::KeyPartitioned(vec![Arc::clone(&a)]),
            Distribution::KeyPartitioned(vec![Arc::clone(&b)]),
        ]);
        let returned = round_trip(&co_partitioned);
        assert!(returned.is_co_partitioned());
        assert_eq!(format!("{co_partitioned:?}"), format!("{returned:?}"));

        // The deprecated HashPartitioned is sent as the equivalent KeyPartitioned.
        let hash =
            InputDistributionRequirements::new(vec![Distribution::HashPartitioned(
                vec![Arc::clone(&a)],
            )]);
        let returned = round_trip(&hash);
        assert!(matches!(
            returned.child_distribution(0),
            Some(Distribution::KeyPartitioned(exprs)) if exprs.len() == 1
        ));
    }
}
