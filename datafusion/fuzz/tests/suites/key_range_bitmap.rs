// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Randomized partitions against [`KeyRangeBitmap::union`]'s contract: it
//! never excludes a key any part may hold, equals the bitmap built from all
//! parts' keys when no part brings its own bitmap, and only widens that
//! bitmap's buckets when one does.

use std::future::Future;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::buffer::BooleanBuffer;
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_plan::joins::key_range_bitmap::{KeyRangeBitmap, KeyRangePart};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_utils::fuzz::FuzzTest;

pub(crate) struct KeyRangeBitmapFuzzer;

fn parts_of(b: &KeyRangeBitmap) -> (u64, u64, u32, &BooleanBuffer) {
    (b.offset(), b.span(), b.shift(), b.bits())
}

enum Part {
    Keys(ArrayRef),
    Range(ScalarValue, ScalarValue),
    Bitmap(KeyRangeBitmap),
}

impl FuzzTest for KeyRangeBitmapFuzzer {
    type Dataset = ();

    fn name(&self) -> &str {
        "key_range_bitmap_union"
    }

    fn prepare_dataset(&self, _seed: u64) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }

    fn run_case(&self, _dataset: &(), seed: u64) -> impl Future<Output = Result<()>> {
        let mut rng = StdRng::seed_from_u64(seed);
        // Domain widths from one key per bucket to many keys per bucket, so
        // the union's buckets are sometimes exact and sometimes coarse.
        let width = [64i64, 1 << 12, 1 << 24, 1 << 40][rng.random_range(0..4)];
        let start = rng.random_range(-(1i64 << 50)..(1i64 << 50));
        let key = |rng: &mut StdRng| start + rng.random_range(0..width);
        let keys_array =
            |keys: &[i64]| -> ArrayRef { Arc::new(Int64Array::from(keys.to_vec())) };
        let scalar = |key: i64| ScalarValue::Int64(Some(key));

        // Each part, and every key any part may hold.
        let mut parts = vec![];
        let mut all_keys = vec![];
        let mut has_bitmap = false;
        for _ in 0..rng.random_range(1..5) {
            if rng.random_bool(1.0 / 3.0) {
                // Narrow, so every key it may hold can be listed.
                let lo = key(&mut rng);
                let hi = lo + rng.random_range(0..64);
                parts.push(Part::Range(scalar(lo), scalar(hi)));
                all_keys.extend(lo..=hi);
                continue;
            }
            let keys: Vec<i64> = (0..rng.random_range(1..50))
                .map(|_| key(&mut rng))
                .collect();
            let (lo, hi) = (*keys.iter().min().unwrap(), *keys.iter().max().unwrap());
            let bitmap = rng
                .random_bool(0.5)
                .then(|| {
                    KeyRangeBitmap::try_new(
                        &keys_array(&keys),
                        &scalar(lo),
                        &scalar(hi),
                        0,
                    )
                })
                .flatten();
            parts.push(match bitmap {
                Some(bitmap) => {
                    has_bitmap = true;
                    Part::Bitmap(bitmap)
                }
                None => Part::Keys(keys_array(&keys)),
            });
            all_keys.extend(keys);
        }
        let min = ScalarValue::Int64(all_keys.iter().min().copied());
        let max = ScalarValue::Int64(all_keys.iter().max().copied());
        let context = format!(
            "seed {seed}: width {width}, {} parts, {} keys",
            parts.len(),
            all_keys.len()
        );

        let union = KeyRangeBitmap::union(
            &min,
            &max,
            &parts
                .iter()
                .map(|part| match part {
                    Part::Keys(keys) => KeyRangePart::Keys(keys.as_ref()),
                    Part::Range(min, max) => KeyRangePart::Range { min, max },
                    Part::Bitmap(bitmap) => KeyRangePart::Bitmap(bitmap),
                })
                .collect::<Vec<_>>(),
        );
        // Reference: one bitmap built straight from every key any part may hold.
        let exact = KeyRangeBitmap::try_new(&keys_array(&all_keys), &min, &max, 0);

        // Never excludes a key any part may hold.
        if let Some(union) = &union {
            for key in &all_keys {
                assert!(
                    union.may_contain_range(*key as u64, *key as u64),
                    "{context}: dropped key {key}"
                );
            }
        }
        if has_bitmap {
            // A part's bitmap can only widen buckets: every bucket the exact
            // bitmap sets, the union sets too.
            match (&union, &exact) {
                (_, None) => assert!(union.is_none(), "{context}"),
                (None, Some(_)) => {}
                (Some(union), Some(exact)) => {
                    assert_eq!(
                        (union.offset(), union.span(), union.shift()),
                        (exact.offset(), exact.span(), exact.shift()),
                        "{context}"
                    );
                    for bucket in exact.bits().set_indices() {
                        assert!(union.bits().value(bucket), "{context}: bucket {bucket}");
                    }
                }
            }
            // Each set bucket of a part's bitmap stands for its whole key range,
            // so the union equals the one over those ranges given as `Range`s.
            let ranges: Vec<(ScalarValue, ScalarValue)> = parts
                .iter()
                .flat_map(|part| match part {
                    Part::Bitmap(bitmap) => bitmap
                        .bits()
                        .set_indices()
                        .map(|bucket| {
                            let lo = (bucket as u64) << bitmap.shift();
                            let hi = lo
                                .saturating_add((1u64 << bitmap.shift()) - 1)
                                .min(bitmap.span());
                            let key =
                                |at: u64| scalar(bitmap.offset().wrapping_add(at) as i64);
                            (key(lo), key(hi))
                        })
                        .collect(),
                    _ => vec![],
                })
                .collect();
            let expanded = KeyRangeBitmap::union(
                &min,
                &max,
                &parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Keys(keys) => Some(KeyRangePart::Keys(keys.as_ref())),
                        Part::Range(min, max) => Some(KeyRangePart::Range { min, max }),
                        Part::Bitmap(_) => None,
                    })
                    .chain(
                        ranges
                            .iter()
                            .map(|(min, max)| KeyRangePart::Range { min, max }),
                    )
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                union.as_ref().map(parts_of),
                expanded.as_ref().map(parts_of),
                "{context}: bitmap parts differ from their bucket ranges"
            );
        } else {
            // Keys and ranges mark exactly the buckets their keys fall in.
            assert_eq!(
                union.as_ref().map(parts_of),
                exact.as_ref().map(parts_of),
                "{context}"
            );
        }

        std::future::ready(Ok(()))
    }
}
