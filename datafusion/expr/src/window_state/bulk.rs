// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Window frames of many rows at once.
//!
//! [`WindowFrameContext::calculate_range`] computes the frame of one row. The functions here compute the frames of a
//! run of rows in one pass ([`WindowFrameContext::calculate_ranges`]) or of the rows of several complete partitions
//! ([`WindowFrameContext::calculate_ranges_segmented`]), which is what the bulk evaluation of window aggregates and
//! of `BoundedWindowAggExec` builds on.

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};

use super::{WindowFrameContext, WindowFrameStateRange};
use crate::{WindowFrame, WindowFrameBound};

/// Widening of the native integer types of an ORDER BY key to `i128`, so that `key +- offset` cannot overflow.
trait ToI128: Copy {
    const MIN_I128: i128;
    const MAX_I128: i128;
    fn to_i128(self) -> i128;
}
macro_rules! impl_to_i128 {
    ($($t:ty),*) => {$(impl ToI128 for $t {
        const MIN_I128: i128 = <$t>::MIN as i128;
        const MAX_I128: i128 = <$t>::MAX as i128;
        #[inline(always)]
        fn to_i128(self) -> i128 { self as i128 }
    })*};
}
impl_to_i128!(i8, i16, i32, i64, u8, u16, u32, u64);

/// ORDER BY column types whose peers the bulk paths compare with the arrow `distinct` kernel.
fn peers_supported(data_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        data_type,
        Int8 | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Boolean
            | Utf8
            | LargeUtf8
            | Utf8View
            | Binary
            | LargeBinary
            | BinaryView
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Duration(_)
            | Timestamp(_, _)
            | Decimal128(_, _)
            | Decimal256(_, _)
    )
}

/// `result[k]` is true when row `k` and row `k + 1` differ in at least one of `columns`
/// (NULL equals NULL, as for peers). `None` when `columns` is empty, or when a column has a type the
/// bulk paths do not compare (see `peers_supported`).
///
/// # Public Only for Internal Use:
///
/// This is not a public API and is for internal use only; see [API policy] for details.
///
/// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
#[doc(hidden)]
pub fn adjacent_distinct(columns: &[ArrayRef]) -> Option<arrow::array::BooleanArray> {
    use arrow::compute::kernels::cmp::distinct;
    if columns.is_empty() || !columns.iter().all(|c| peers_supported(c.data_type())) {
        return None;
    }
    let n = columns[0].len();
    if n < 2 {
        return Some(arrow::array::BooleanArray::from(Vec::<bool>::new()));
    }
    let mut neq: Option<arrow::array::BooleanArray> = None;
    for c in columns {
        let a = c.slice(0, n - 1);
        let b = c.slice(1, n - 1);
        let d = distinct(&a, &b).ok()?;
        neq = Some(match neq {
            Some(x) => arrow::compute::or(&x, &d).ok()?,
            None => d,
        });
    }
    neq
}

/// The bounds of a `ROWS` frame, resolved once.
#[derive(Clone, Copy)]
struct RowsBounds {
    start: RowsBound,
    end: RowsBound,
}

#[derive(Clone, Copy)]
enum RowsBound {
    Unbounded,
    Preceding(usize),
    Current,
    Following(usize),
}

impl RowsBounds {
    fn resolve(window_frame: &WindowFrame) -> Result<Self> {
        fn bound(bound: &WindowFrameBound) -> Result<RowsBound> {
            Ok(match bound {
                WindowFrameBound::Preceding(ScalarValue::UInt64(None))
                | WindowFrameBound::Following(ScalarValue::UInt64(None)) => {
                    RowsBound::Unbounded
                }
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => {
                    RowsBound::Preceding(*n as usize)
                }
                WindowFrameBound::CurrentRow => RowsBound::Current,
                WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                    RowsBound::Following(*n as usize)
                }
                _ => return internal_err!("Rows should be UInt64"),
            })
        }
        let start_unbounded_preceding = matches!(
            window_frame.start_bound,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None))
        );
        let end_unbounded_following = matches!(
            window_frame.end_bound,
            WindowFrameBound::Following(ScalarValue::UInt64(None))
        );
        let start = bound(&window_frame.start_bound)?;
        let end = bound(&window_frame.end_bound)?;
        if matches!(start, RowsBound::Unbounded) && !start_unbounded_preceding {
            return internal_err!(
                "Frame start cannot be UNBOUNDED FOLLOWING '{window_frame:?}'"
            );
        }
        if matches!(end, RowsBound::Unbounded) && !end_unbounded_following {
            return internal_err!(
                "Frame end cannot be UNBOUNDED PRECEDING '{window_frame:?}'"
            );
        }
        Ok(Self { start, end })
    }

    /// Frames of the rows `idx..length` of a partition of `length` rows that starts at row
    /// `base` of the columns, appended to `ranges` as absolute indices.
    fn push_frames(
        &self,
        length: usize,
        idx: usize,
        stop_at_length: bool,
        base: usize,
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<()> {
        let additional = length.saturating_sub(idx);
        ranges.try_reserve(additional).map_err(|e| {
            exec_datafusion_err!("Failed to allocate {additional} window frames: {e}")
        })?;
        for i in idx..length {
            let start = match self.start {
                RowsBound::Unbounded => 0,
                RowsBound::Preceding(n) => i.saturating_sub(n),
                RowsBound::Current => i,
                RowsBound::Following(n) => std::cmp::min(i.saturating_add(n), length),
            };
            let end = match self.end {
                RowsBound::Unbounded => length,
                RowsBound::Preceding(n) => {
                    if i >= n {
                        i - n + 1
                    } else {
                        0
                    }
                }
                RowsBound::Current => i + 1,
                RowsBound::Following(n) => {
                    std::cmp::min(i.saturating_add(n).saturating_add(1), length)
                }
            };
            if stop_at_length && end == length {
                break;
            }
            ranges.push(Range {
                start: base + start,
                end: base + end,
            });
        }
        Ok(())
    }
}

impl WindowFrameContext {
    /// The frames of the rows `idx..length` in one pass, appended to `ranges`. Row by row this is
    /// [`Self::calculate_range`]; the result is the same.
    ///
    /// With `stop_at_length` the pass ends before the first row whose frame reaches
    /// `length` (the caller treats those tail rows one at a time, because their frames may
    /// still grow when more data arrives).
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn calculate_ranges(
        &mut self,
        range_columns: &[ArrayRef],
        last_range: &Range<usize>,
        length: usize,
        idx: usize,
        stop_at_length: bool,
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<()> {
        if let WindowFrameContext::Rows(window_frame) = self {
            return Self::calculate_ranges_rows(
                window_frame,
                length,
                idx,
                stop_at_length,
                ranges,
            );
        }
        if let WindowFrameContext::Range {
            window_frame,
            state,
        } = self
            && state.calculate_ranges_bulk(
                window_frame,
                range_columns,
                last_range,
                length,
                idx,
                stop_at_length,
                ranges,
            )?
        {
            return Ok(());
        }
        // GROUPS frames, and the RANGE frames without a bulk implementation or with a key outside
        // the range in which the per-row code is exact, one row at a time
        let mut last = last_range.clone();
        for i in idx..length {
            let r = self.calculate_range(range_columns, &last, length, i)?;
            if stop_at_length && r.end == length {
                break;
            }
            last = r.clone();
            ranges.push(r);
        }
        Ok(())
    }

    fn calculate_ranges_rows(
        window_frame: &Arc<WindowFrame>,
        length: usize,
        idx: usize,
        stop_at_length: bool,
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<()> {
        let bounds = RowsBounds::resolve(window_frame)?;
        bounds.push_frames(length, idx, stop_at_length, 0, ranges)
    }

    /// The frames of every row of `segments` (consecutive complete partitions that cover
    /// the columns), appended to `ranges` as absolute row indices.
    ///
    /// Returns `Ok(false)` (nothing pushed) when this frame / column combination has no segmented
    /// implementation, or when a `RANGE` key or its search target lies outside the range in which the
    /// per-row code is exact.
    ///
    /// # Public Only for Internal Use:
    ///
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn calculate_ranges_segmented(
        &mut self,
        range_columns: &[ArrayRef],
        segments: &[Range<usize>],
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<bool> {
        match self {
            WindowFrameContext::Rows(window_frame) => {
                let bounds = RowsBounds::resolve(window_frame)?;
                for seg in segments {
                    bounds.push_frames(
                        seg.end - seg.start,
                        0,
                        false,
                        seg.start,
                        ranges,
                    )?;
                }
                Ok(true)
            }
            WindowFrameContext::Range {
                window_frame,
                state,
            } => state.calculate_ranges_segmented(
                window_frame,
                range_columns,
                segments,
                ranges,
            ),
            WindowFrameContext::Groups { .. } => Ok(false),
        }
    }
}

impl WindowFrameStateRange {
    /// Bulk frames for RANGE. Returns `Ok(false)` (nothing pushed) when the frame /
    /// column combination has no bulk implementation, or when a key or its search target lies
    /// outside the range in which the per-row code is exact.
    #[expect(clippy::too_many_arguments)]
    fn calculate_ranges_bulk(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        last_range: &Range<usize>,
        length: usize,
        idx: usize,
        stop_at_length: bool,
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<bool> {
        if idx >= length {
            return Ok(true);
        }
        let simple = |b: &WindowFrameBound| match b {
            WindowFrameBound::Preceding(n) | WindowFrameBound::Following(n) => {
                n.is_null()
            }
            WindowFrameBound::CurrentRow => true,
        };
        if simple(&window_frame.start_bound) && simple(&window_frame.end_bound) {
            return self.bulk_peers(
                window_frame,
                range_columns,
                last_range,
                length,
                idx,
                stop_at_length,
                ranges,
            );
        }
        if range_columns.len() == 1 && self.sort_options.len() == 1 {
            return self.bulk_numeric(
                window_frame,
                &range_columns[0],
                last_range,
                length,
                idx,
                stop_at_length,
                0,
                ranges,
            );
        }
        Ok(false)
    }

    /// [`WindowFrameContext::calculate_ranges_segmented`] for RANGE frames.
    fn calculate_ranges_segmented(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        segments: &[Range<usize>],
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<bool> {
        let simple = |b: &WindowFrameBound| match b {
            WindowFrameBound::Preceding(n) | WindowFrameBound::Following(n) => {
                n.is_null()
            }
            WindowFrameBound::CurrentRow => true,
        };
        if simple(&window_frame.start_bound) && simple(&window_frame.end_bound) {
            return self.bulk_peers_segmented(
                window_frame,
                range_columns,
                segments,
                ranges,
            );
        }
        if range_columns.len() == 1 && self.sort_options.len() == 1 {
            let before = ranges.len();
            for seg in segments {
                if !self.bulk_numeric(
                    window_frame,
                    &range_columns[0],
                    &(seg.start..seg.start),
                    seg.end,
                    seg.start,
                    false,
                    seg.start,
                    ranges,
                )? {
                    ranges.truncate(before);
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Frames made only of UNBOUNDED / CURRENT ROW bounds, for several complete partitions: the peer
    /// groups of the whole run are found in one pass over the ORDER BY columns.
    fn bulk_peers_segmented(
        &self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        segments: &[Range<usize>],
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<bool> {
        let start_cur = matches!(window_frame.start_bound, WindowFrameBound::CurrentRow);
        let end_cur = matches!(window_frame.end_bound, WindowFrameBound::CurrentRow);
        match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) if n.is_null() => {}
            WindowFrameBound::CurrentRow => {}
            _ => return Ok(false),
        }
        match window_frame.end_bound {
            WindowFrameBound::Following(ref n) if n.is_null() => {}
            WindowFrameBound::CurrentRow => {}
            _ => return Ok(false),
        }
        let n = segments.last().map_or(0, |s| s.end);
        // an unsupported type must not become a query error: use the per-partition path
        let Some(neq) = adjacent_distinct(range_columns) else {
            return Ok(false);
        };
        let differs = |k: usize| neq.value(k);
        let base = ranges.len();
        ranges.resize(base + n, Range { start: 0, end: 0 });
        for seg in segments {
            if start_cur {
                let mut group_start = seg.start;
                for i in seg.start..seg.end {
                    if i > seg.start && differs(i - 1) {
                        group_start = i;
                    }
                    ranges[base + i].start = group_start;
                }
            } else {
                for i in seg.start..seg.end {
                    ranges[base + i].start = seg.start;
                }
            }
            if end_cur {
                let mut group_end = seg.end;
                for i in (seg.start..seg.end).rev() {
                    if i + 1 < seg.end && differs(i) {
                        group_end = i + 1;
                    }
                    ranges[base + i].end = group_end;
                }
            } else {
                for i in seg.start..seg.end {
                    ranges[base + i].end = seg.end;
                }
            }
        }
        Ok(true)
    }

    /// Frames made only of UNBOUNDED / CURRENT ROW bounds depend on the peer groups alone.
    #[expect(clippy::too_many_arguments)]
    fn bulk_peers(
        &self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        last_range: &Range<usize>,
        length: usize,
        idx: usize,
        stop_at_length: bool,
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<bool> {
        let start_cur = matches!(window_frame.start_bound, WindowFrameBound::CurrentRow);
        let end_cur = matches!(window_frame.end_bound, WindowFrameBound::CurrentRow);
        match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) if n.is_null() => {}
            WindowFrameBound::CurrentRow => {}
            _ => return Ok(false),
        }
        match window_frame.end_bound {
            WindowFrameBound::Following(ref n) if n.is_null() => {}
            WindowFrameBound::CurrentRow => {}
            _ => return Ok(false),
        }
        let lo = last_range.start.min(idx);
        let n = length - lo;
        // neq[k]: row lo+k and row lo+k+1 are not peers
        let rows: Vec<ArrayRef> = range_columns.iter().map(|c| c.slice(lo, n)).collect();
        let Some(neq) = adjacent_distinct(&rows) else {
            return Ok(false);
        };
        let differs = |k: usize| k < neq.len() && neq.value(k);
        let m = length - idx;
        let mut group_end: Vec<usize> = Vec::new();
        if end_cur {
            group_end = vec![length; m];
            let mut e = length;
            for i in (idx..length).rev() {
                if i + 1 < length && differs(i - lo) {
                    e = i + 1;
                }
                group_end[i - idx] = e;
            }
        }
        let mut gstart = lo;
        if start_cur {
            for i in (lo + 1)..=idx {
                if differs(i - 1 - lo) {
                    gstart = i;
                }
            }
        }
        ranges.try_reserve(m).map_err(|e| {
            exec_datafusion_err!("Failed to allocate {m} window frames: {e}")
        })?;
        for i in idx..length {
            if start_cur && i > idx && differs(i - 1 - lo) {
                gstart = i;
            }
            let start = if start_cur { gstart } else { 0 };
            let end = if end_cur { group_end[i - idx] } else { length };
            if stop_at_length && end == length {
                break;
            }
            ranges.push(Range { start, end });
        }
        Ok(true)
    }

    /// A single integer or timestamp ORDER BY column with numeric / interval offsets.
    ///
    /// `base` is the first row of the partition: where an UNBOUNDED PRECEDING frame starts.
    #[expect(clippy::too_many_arguments)]
    fn bulk_numeric(
        &self,
        window_frame: &Arc<WindowFrame>,
        column: &ArrayRef,
        last_range: &Range<usize>,
        length: usize,
        idx: usize,
        stop_at_length: bool,
        base: usize,
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<bool> {
        use arrow::array::AsArray;
        use arrow::datatypes::{
            Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit,
            TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
            TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
        };
        macro_rules! go {
            ($t:ty, $unit:expr) => {
                self.bulk_numeric_typed(
                    window_frame,
                    column.as_primitive::<$t>(),
                    $unit,
                    last_range,
                    length,
                    idx,
                    stop_at_length,
                    base,
                    ranges,
                )
            };
        }
        match column.data_type() {
            DataType::Int8 => go!(Int8Type, None),
            DataType::Int16 => go!(Int16Type, None),
            DataType::Int32 => go!(Int32Type, None),
            DataType::Int64 => go!(Int64Type, None),
            DataType::UInt8 => go!(UInt8Type, None),
            DataType::UInt16 => go!(UInt16Type, None),
            DataType::UInt32 => go!(UInt32Type, None),
            DataType::UInt64 => go!(UInt64Type, None),
            // a fixed-offset zone makes "+ N days" a plain 86400 s step; named zones do not
            DataType::Timestamp(unit, tz)
                if tz.as_deref().is_none_or(|z| matches!(z, "UTC" | "+00:00")) =>
            {
                match unit {
                    TimeUnit::Second => go!(TimestampSecondType, Some(*unit)),
                    TimeUnit::Millisecond => go!(TimestampMillisecondType, Some(*unit)),
                    TimeUnit::Microsecond => go!(TimestampMicrosecondType, Some(*unit)),
                    TimeUnit::Nanosecond => go!(TimestampNanosecondType, Some(*unit)),
                }
            }
            _ => Ok(false),
        }
    }

    /// `unit` is `Some` for timestamp keys: their offsets are intervals and are turned into
    /// whole ticks of the key's unit (anything inexact falls back to the per-row path).
    #[expect(clippy::too_many_arguments)]
    fn bulk_numeric_typed<T: arrow::datatypes::ArrowPrimitiveType>(
        &self,
        window_frame: &Arc<WindowFrame>,
        col: &arrow::array::PrimitiveArray<T>,
        unit: Option<arrow::datatypes::TimeUnit>,
        last_range: &Range<usize>,
        length: usize,
        idx: usize,
        stop_at_length: bool,
        base: usize,
        ranges: &mut Vec<Range<usize>>,
    ) -> Result<bool>
    where
        T::Native: ToI128,
    {
        use arrow::array::Array;
        use arrow::datatypes::TimeUnit;
        #[derive(Clone, Copy)]
        enum B {
            Unb,
            Cur,
            Off { delta: i128, preceding: bool },
        }
        fn interval_ticks(
            days: i128,
            nanos: i128,
            months: i128,
            unit: TimeUnit,
        ) -> Option<i128> {
            if months != 0 || days < 0 || nanos < 0 {
                return None;
            }
            let ns_per_tick: i128 = match unit {
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1_000,
                TimeUnit::Nanosecond => 1,
            };
            if nanos % ns_per_tick != 0 {
                return None;
            }
            Some(days * (86_400_000_000_000 / ns_per_tick) + nanos / ns_per_tick)
        }
        fn delta_of(v: &ScalarValue, unit: Option<TimeUnit>) -> Option<i128> {
            let d: i128 = match (v, unit) {
                (ScalarValue::Int8(Some(x)), None) => *x as i128,
                (ScalarValue::Int16(Some(x)), None) => *x as i128,
                (ScalarValue::Int32(Some(x)), None) => *x as i128,
                (ScalarValue::Int64(Some(x)), None) => *x as i128,
                (ScalarValue::UInt8(Some(x)), None) => *x as i128,
                (ScalarValue::UInt16(Some(x)), None) => *x as i128,
                (ScalarValue::UInt32(Some(x)), None) => *x as i128,
                (ScalarValue::UInt64(Some(x)), None) => *x as i128,
                (ScalarValue::IntervalMonthDayNano(Some(i)), Some(unit)) => {
                    interval_ticks(
                        i.days as i128,
                        i.nanoseconds as i128,
                        i.months as i128,
                        unit,
                    )?
                }
                (ScalarValue::IntervalDayTime(Some(i)), Some(unit)) => interval_ticks(
                    i.days as i128,
                    i.milliseconds as i128 * 1_000_000,
                    0,
                    unit,
                )?,
                _ => return None,
            };
            (d >= 0).then_some(d)
        }
        fn resolve(b: &WindowFrameBound, unit: Option<TimeUnit>) -> Option<B> {
            Some(match b {
                WindowFrameBound::Preceding(n) if n.is_null() => B::Unb,
                WindowFrameBound::Following(n) if n.is_null() => B::Unb,
                WindowFrameBound::CurrentRow => B::Cur,
                WindowFrameBound::Preceding(n) => B::Off {
                    delta: delta_of(n, unit)?,
                    preceding: true,
                },
                WindowFrameBound::Following(n) => B::Off {
                    delta: delta_of(n, unit)?,
                    preceding: false,
                },
            })
        }
        let (Some(start_b), Some(end_b)) = (
            resolve(&window_frame.start_bound, unit),
            resolve(&window_frame.end_bound, unit),
        ) else {
            return Ok(false);
        };
        // UNBOUNDED must be on its natural side
        if matches!(start_b, B::Unb)
            && !matches!(window_frame.start_bound, WindowFrameBound::Preceding(_))
        {
            return Ok(false);
        }
        if matches!(end_b, B::Unb)
            && !matches!(window_frame.end_bound, WindowFrameBound::Following(_))
        {
            return Ok(false);
        }
        let unsigned = matches!(
            T::DATA_TYPE,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        );
        let descending = self.sort_options[0].descending;
        let nulls_first = self.sort_options[0].nulls_first;
        let vals = col.values();
        let nulls = col.nulls();
        let value_at = |r: usize| -> Option<i128> {
            if nulls.is_none_or(|n| n.is_valid(r)) {
                Some(vals[r].to_i128())
            } else {
                None
            }
        };
        // order of row `r` against the search target, in the sort order of the column
        let cmp_row = |r: usize, target: Option<i128>| -> Ordering {
            match (value_at(r), target) {
                (None, Some(_)) => {
                    if nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (Some(_), None) => {
                    if nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (None, None) => Ordering::Equal,
                (Some(a), Some(t)) => {
                    let o = a.cmp(&t);
                    if descending { o.reverse() } else { o }
                }
            }
        };
        // Where `key +- offset` leaves the range of the key type (for timestamps: the range in which
        // the per-row arithmetic is exact), the per-row path takes the edge of the partition instead of
        // searching, which is not the exact answer when NULL keys sit on that side. Such rows are left to it.
        let (lo, hi) = match unit {
            Some(TimeUnit::Second) => (-(1i128 << 42), 1i128 << 42),
            Some(TimeUnit::Millisecond) => (-(1i128 << 52), 1i128 << 52),
            Some(TimeUnit::Microsecond) => (-(1i128 << 62), 1i128 << 62),
            _ => (T::Native::MIN_I128, T::Native::MAX_I128),
        };
        // the search target of row value `v`: the outer `None` when it is out of range, the inner one for a NULL key
        let target = |v: Option<i128>, b: B| -> Option<Option<i128>> {
            let Some(v) = v else {
                return Some(None);
            };
            let B::Off { delta, preceding } = b else {
                return Some(Some(v));
            };
            let t = if preceding == descending {
                v + delta
            } else if unsigned && v < delta {
                // the per-row path clamps an unsigned underflow to zero
                0
            } else {
                v - delta
            };
            ((lo..=hi).contains(&v) && (lo..=hi).contains(&t)).then_some(Some(t))
        };
        let first = ranges.len();
        let (mut s, mut e) = (last_range.start, last_range.end);
        ranges.try_reserve(length - idx).map_err(|e| {
            exec_datafusion_err!("Failed to allocate {} window frames: {e}", length - idx)
        })?;
        for i in idx..length {
            let v = value_at(i);
            let start = if matches!(start_b, B::Unb) {
                base
            } else {
                let Some(t) = target(v, start_b) else {
                    ranges.truncate(first);
                    return Ok(false);
                };
                while s < length && cmp_row(s, t) == Ordering::Less {
                    s += 1;
                }
                s
            };
            let end = if matches!(end_b, B::Unb) {
                length
            } else {
                let Some(t) = target(v, end_b) else {
                    ranges.truncate(first);
                    return Ok(false);
                };
                while e < length && cmp_row(e, t) != Ordering::Greater {
                    e += 1;
                }
                e
            };
            if stop_at_length && end == length {
                break;
            }
            ranges.push(Range { start, end });
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::UInt64Array;
    use arrow::compute::SortOptions;

    use crate::WindowFrameUnits;

    #[test]
    fn segmented_ranges_untouched_when_a_key_plus_offset_overflows() -> Result<()> {
        let frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::Following(ScalarValue::UInt64(Some(5))),
        ));
        let segments = [0..3, 3..6, 6..9];
        let context =
            || WindowFrameContext::new(Arc::clone(&frame), vec![SortOptions::default()]);
        let keys = |last: u64| -> [ArrayRef; 1] {
            [Arc::new(UInt64Array::from(vec![
                0, 1, 2, 0, 1, 2, 0, 1, last,
            ]))]
        };

        let mut ranges = vec![0..1, 1..2];
        assert!(context().calculate_ranges_segmented(
            &keys(2),
            &segments,
            &mut ranges
        )?);
        assert_eq!(ranges[..2], [0..1, 1..2]);
        assert_eq!(
            ranges[2..],
            [0..3, 1..3, 2..3, 3..6, 4..6, 5..6, 6..9, 7..9, 8..9]
        );

        // the last key plus the offset overflows after two partitions have pushed their frames
        let mut ranges = vec![0..1, 1..2];
        assert!(!context().calculate_ranges_segmented(
            &keys(u64::MAX),
            &segments,
            &mut ranges
        )?);
        assert_eq!(ranges, [0..1, 1..2]);
        Ok(())
    }
}
