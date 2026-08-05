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

use std::sync::Arc;

use datafusion_common::{DataFusionError, ScalarValue, SplitPoint};
use datafusion_physical_expr::{
    LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use stabby::vec::Vec as SVec;

use crate::arrow_wrappers::WrappedArray;
use crate::physical_expr::FFI_PhysicalExpr;
use crate::physical_expr::sort::FFI_PhysicalSortExpr;

/// A stable struct for sharing [`RangePartitioning`] across FFI boundaries.
/// See [`RangePartitioning`] for the descriptions of each field.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_RangePartitioning {
    split_points: SVec<SVec<WrappedArray>>,
    ordering: SVec<FFI_PhysicalSortExpr>,
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
                let split_points = range
                    .split_points()
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
                    split_points,
                    ordering,
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
                let split_points = range
                    .split_points
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

                Self::Range(RangePartitioning::try_new(ordering, split_points)?)
            }
            FFI_Partitioning::UnknownPartitioning(size) => {
                Self::UnknownPartitioning(size)
            }
        })
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
    fn ffi_range_partitioning_rejects_empty_ordering() {
        let ffi_partitioning = FFI_Partitioning::Range(FFI_RangePartitioning {
            split_points: SVec::new(),
            ordering: SVec::new(),
        });

        let err = Partitioning::try_from(ffi_partitioning).unwrap_err();
        assert!(
            err.to_string().contains("ordering must be non-empty"),
            "{err}"
        );
    }
}
