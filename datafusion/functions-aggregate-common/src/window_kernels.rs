// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Typed kernels that evaluate an aggregate over a run of window frames.
//!
//! Every kernel follows the per-row protocol that `SlidingAggregateWindowExpr` /
//! `PlainAggregateWindowExpr` use (add the rows that entered the frame, remove the rows
//! that left it, read the result), but on typed slices and without a `ScalarValue` or a
//! dynamic call per row. Frames are monotone: start and end never move backwards.
//!
//! # Public Only for Internal Use:
//!
//! This is not a public API and is for internal use only; see [API policy] for details.
//!
//! [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, NullBufferBuilder, PrimitiveArray};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowNativeTypeOp, ArrowNumericType, ArrowPrimitiveType, DataType,
};

/// Number of valid (non-null) rows in `[a, b)`.
#[inline(always)]
pub fn valid_in(nulls: Option<&NullBuffer>, a: usize, b: usize) -> usize {
    match nulls {
        None => b - a,
        Some(n) => match b - a {
            0 => 0,
            1 => n.is_valid(a) as usize,
            len => n.inner().slice(a, len).count_set_bits(),
        },
    }
}

/// `sum += arr[a..b]`, `count += valid rows`. A single row is added directly; longer runs
/// go through `arrow::compute::sum`, exactly like the per-row protocol, so that float
/// results are bit-identical to it.
#[inline(always)]
fn add_rows<T: ArrowNumericType>(
    arr: &PrimitiveArray<T>,
    a: usize,
    b: usize,
    sum: &mut T::Native,
    count: &mut u64,
    added: &mut bool,
) {
    match b - a {
        0 => {}
        1 => {
            if arr.is_valid(a) {
                *sum = sum.add_wrapping(arr.values()[a]);
                *count += 1;
                *added = true;
            }
        }
        n => {
            let s = arr.slice(a, n);
            if let Some(x) = arrow::compute::sum(&s) {
                *sum = sum.add_wrapping(x);
                *added = true;
            }
            *count += (n - s.null_count()) as u64;
        }
    }
}

#[inline(always)]
fn sub_rows<T: ArrowNumericType>(
    arr: &PrimitiveArray<T>,
    a: usize,
    b: usize,
    sum: &mut T::Native,
    count: &mut u64,
) {
    match b - a {
        0 => {}
        1 => {
            if arr.is_valid(a) {
                *sum = sum.sub_wrapping(arr.values()[a]);
                *count -= 1;
            }
        }
        n => {
            let s = arr.slice(a, n);
            if let Some(x) = arrow::compute::sum(&s) {
                *sum = sum.sub_wrapping(x);
            }
            *count -= (n - s.null_count()) as u64;
        }
    }
}

/// Runs the (sum, count) state across `frames`, calling `emit` with `None` for an empty
/// frame and `Some((sum, count))` otherwise. `retract` selects the sliding protocol; the
/// plain (UNBOUNDED PRECEDING) protocol never removes rows.
#[inline(always)]
#[expect(clippy::too_many_arguments)]
pub fn run_sum_frames<T: ArrowNumericType>(
    arr: &PrimitiveArray<T>,
    sum: &mut T::Native,
    count: &mut u64,
    added_any: &mut bool,
    last: &Range<usize>,
    frames: &[Range<usize>],
    retract: bool,
    mut emit: impl FnMut(Option<(T::Native, u64)>),
) {
    let mut last = last.clone();
    let (mut s, mut c) = (*sum, *count);
    for cur in frames {
        if cur.start == cur.end {
            if retract && last.end > last.start {
                sub_rows(arr, last.start, last.end, &mut s, &mut c);
            }
            emit(None);
        } else {
            if cur.end > last.end {
                add_rows(arr, last.end, cur.end, &mut s, &mut c, added_any);
            }
            if retract && cur.start > last.start {
                sub_rows(arr, last.start, cur.start, &mut s, &mut c);
            }
            emit(Some((s, c)));
        }
        last = cur.clone();
    }
    *sum = s;
    *count = c;
}

/// SUM over frames; the output has `data_type` (decimal precision, timestamp zone, ...).
pub fn sum_frames<T: ArrowNumericType>(
    arr: &PrimitiveArray<T>,
    sum: &mut T::Native,
    count: &mut u64,
    last: &Range<usize>,
    frames: &[Range<usize>],
    retract: bool,
    data_type: &DataType,
) -> ArrayRef {
    let mut out: Vec<T::Native> = Vec::with_capacity(frames.len());
    let mut nulls = NullBufferBuilder::new(frames.len());
    let mut added = false;
    run_sum_frames(
        arr,
        sum,
        count,
        &mut added,
        last,
        frames,
        retract,
        |r| match r {
            Some((s, c)) if c != 0 => {
                out.push(s);
                nulls.append_non_null();
            }
            _ => {
                out.push(T::Native::default());
                nulls.append_null();
            }
        },
    );
    Arc::new(
        PrimitiveArray::<T>::new(ScalarBuffer::from(out), nulls.finish())
            .with_data_type(data_type.clone()),
    )
}

/// AVG over Float64 frames. Returns the output and whether any non-null value was added.
pub fn avg_frames(
    arr: &PrimitiveArray<arrow::datatypes::Float64Type>,
    sum: &mut f64,
    count: &mut u64,
    last: &Range<usize>,
    frames: &[Range<usize>],
) -> (ArrayRef, bool) {
    let mut out: Vec<f64> = Vec::with_capacity(frames.len());
    let mut nulls = NullBufferBuilder::new(frames.len());
    let mut added = false;
    run_sum_frames(
        arr,
        sum,
        count,
        &mut added,
        last,
        frames,
        true,
        |r| match r {
            Some((s, c)) if c != 0 => {
                out.push(s / c as f64);
                nulls.append_non_null();
            }
            _ => {
                out.push(0.0);
                nulls.append_null();
            }
        },
    );
    (
        Arc::new(PrimitiveArray::<arrow::datatypes::Float64Type>::new(
            ScalarBuffer::from(out),
            nulls.finish(),
        )),
        added,
    )
}

/// The COUNT protocol over `frames`, appending one count per frame to `out`.
#[inline(always)]
fn run_count_frames(
    nulls: Option<&NullBuffer>,
    count: &mut i64,
    last: &Range<usize>,
    frames: &[Range<usize>],
    out: &mut Vec<i64>,
) {
    let mut c = *count;
    let mut last = last.clone();
    for cur in frames {
        if cur.start == cur.end {
            if last.end > last.start {
                c -= valid_in(nulls, last.start, last.end) as i64;
            }
            // default value of COUNT is 0, not NULL
            out.push(0);
        } else {
            if cur.end > last.end {
                c += valid_in(nulls, last.end, cur.end) as i64;
            }
            if cur.start > last.start {
                c -= valid_in(nulls, last.start, cur.start) as i64;
            }
            out.push(c);
        }
        last = cur.clone();
    }
    *count = c;
}

/// COUNT over frames. `nulls` is the logical validity of the single argument.
/// Returns the output; `count` is updated in place.
pub fn count_frames(
    nulls: Option<&NullBuffer>,
    count: &mut i64,
    last: &Range<usize>,
    frames: &[Range<usize>],
) -> ArrayRef {
    let mut out: Vec<i64> = Vec::with_capacity(frames.len());
    run_count_frames(nulls, count, last, frames, &mut out);
    Arc::new(PrimitiveArray::<arrow::datatypes::Int64Type>::new(
        ScalarBuffer::from(out),
        None,
    ))
}

/// COUNT over the frames of several complete partitions: the count restarts at every segment.
pub fn count_frames_segments(
    nulls: Option<&NullBuffer>,
    segments: &[Range<usize>],
    frames: &[Range<usize>],
) -> ArrayRef {
    let mut out: Vec<i64> = Vec::with_capacity(frames.len());
    for seg in segments {
        let mut c = 0i64;
        run_count_frames(
            nulls,
            &mut c,
            &(seg.start..seg.start),
            &frames[seg.start..seg.end],
            &mut out,
        );
    }
    Arc::new(PrimitiveArray::<arrow::datatypes::Int64Type>::new(
        ScalarBuffer::from(out),
        None,
    ))
}

/// SUM over the frames of several complete partitions: the state restarts at every segment.
pub fn sum_frames_segments<T: ArrowNumericType>(
    arr: &PrimitiveArray<T>,
    segments: &[Range<usize>],
    frames: &[Range<usize>],
    retract: bool,
    data_type: &DataType,
) -> ArrayRef {
    let mut out: Vec<T::Native> = Vec::with_capacity(frames.len());
    let mut nulls = NullBufferBuilder::new(frames.len());
    let mut added = false;
    for seg in segments {
        let (mut s, mut c) = (T::Native::default(), 0u64);
        run_sum_frames(
            arr,
            &mut s,
            &mut c,
            &mut added,
            &(seg.start..seg.start),
            &frames[seg.start..seg.end],
            retract,
            |r| match r {
                Some((s, c)) if c != 0 => {
                    out.push(s);
                    nulls.append_non_null();
                }
                _ => {
                    out.push(T::Native::default());
                    nulls.append_null();
                }
            },
        );
    }
    Arc::new(
        PrimitiveArray::<T>::new(ScalarBuffer::from(out), nulls.finish())
            .with_data_type(data_type.clone()),
    )
}

/// AVG over Float64 frames of several complete partitions: the state restarts at every segment.
pub fn avg_frames_segments(
    arr: &PrimitiveArray<arrow::datatypes::Float64Type>,
    segments: &[Range<usize>],
    frames: &[Range<usize>],
) -> ArrayRef {
    let mut out: Vec<f64> = Vec::with_capacity(frames.len());
    let mut nulls = NullBufferBuilder::new(frames.len());
    let mut added = false;
    for seg in segments {
        let (mut s, mut c) = (0.0f64, 0u64);
        run_sum_frames(
            arr,
            &mut s,
            &mut c,
            &mut added,
            &(seg.start..seg.start),
            &frames[seg.start..seg.end],
            true,
            |r| match r {
                Some((s, c)) if c != 0 => {
                    out.push(s / c as f64);
                    nulls.append_non_null();
                }
                _ => {
                    out.push(0.0);
                    nulls.append_null();
                }
            },
        );
    }
    Arc::new(PrimitiveArray::<arrow::datatypes::Float64Type>::new(
        ScalarBuffer::from(out),
        nulls.finish(),
    ))
}

/// A queue that reports the maximum (`MAX`) or minimum of its elements in O(1):
/// two stacks, each element stored with the extreme of the stack below it.
#[derive(Debug, Default)]
pub struct MovingExtreme<N: ArrowNativeTypeOp, const MAX: bool> {
    push_stack: Vec<(N, N)>,
    pop_stack: Vec<(N, N)>,
}

impl<N: ArrowNativeTypeOp, const MAX: bool> MovingExtreme<N, MAX> {
    pub fn new() -> Self {
        Self {
            push_stack: Vec::new(),
            pop_stack: Vec::new(),
        }
    }

    #[inline(always)]
    fn better(a: N, b: N) -> N {
        if MAX {
            if b.is_gt(a) { b } else { a }
        } else if b.is_lt(a) {
            b
        } else {
            a
        }
    }

    #[inline(always)]
    pub fn push(&mut self, v: N) {
        let e = match self.push_stack.last() {
            Some(&(_, m)) => Self::better(m, v),
            None => v,
        };
        self.push_stack.push((v, e));
    }

    #[inline(always)]
    pub fn pop(&mut self) {
        if self.pop_stack.is_empty() {
            let mut ext: Option<N> = None;
            while let Some((v, _)) = self.push_stack.pop() {
                let e = match ext {
                    Some(m) => Self::better(m, v),
                    None => v,
                };
                ext = Some(e);
                self.pop_stack.push((v, e));
            }
        }
        self.pop_stack.pop();
    }

    #[inline(always)]
    pub fn extreme(&self) -> Option<N> {
        match (self.push_stack.last(), self.pop_stack.last()) {
            (Some(&(_, a)), Some(&(_, b))) => Some(Self::better(a, b)),
            (Some(&(_, a)), None) => Some(a),
            (None, Some(&(_, b))) => Some(b),
            (None, None) => None,
        }
    }

    pub fn len(&self) -> usize {
        self.push_stack.len() + self.pop_stack.len()
    }

    /// Removes every element (the allocations are kept).
    pub fn clear(&mut self) {
        self.push_stack.clear();
        self.pop_stack.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The sliding MIN/MAX protocol over `frames`, appending one result per frame.
#[inline(always)]
fn run_moving_extreme<T: ArrowPrimitiveType, const MAX: bool>(
    arr: &PrimitiveArray<T>,
    state: &mut MovingExtreme<T::Native, MAX>,
    last: &Range<usize>,
    frames: &[Range<usize>],
    out: &mut Vec<T::Native>,
    ob: &mut NullBufferBuilder,
) where
    T::Native: ArrowNativeTypeOp,
{
    let nulls = arr.nulls();
    let vals = arr.values();
    let mut last = last.clone();
    for cur in frames {
        if cur.start == cur.end {
            for _ in 0..valid_in(nulls, last.start, last.end) {
                state.pop();
            }
            out.push(T::Native::default());
            ob.append_null();
        } else {
            match nulls {
                None => {
                    for i in last.end..cur.end {
                        state.push(vals[i]);
                    }
                }
                Some(n) => {
                    for i in last.end..cur.end {
                        if n.is_valid(i) {
                            state.push(vals[i]);
                        }
                    }
                }
            }
            for _ in 0..valid_in(nulls, last.start, cur.start) {
                state.pop();
            }
            match state.extreme() {
                Some(v) => {
                    out.push(v);
                    ob.append_non_null();
                }
                None => {
                    out.push(T::Native::default());
                    ob.append_null();
                }
            }
        }
        last = cur.clone();
    }
}

/// Sliding MIN/MAX over frames on a typed queue. Same protocol as the per-row path:
/// non-null rows are pushed as they enter and popped (oldest first) as they leave.
pub fn moving_extreme_frames<T: ArrowPrimitiveType, const MAX: bool>(
    arr: &PrimitiveArray<T>,
    state: &mut MovingExtreme<T::Native, MAX>,
    last: &Range<usize>,
    frames: &[Range<usize>],
    data_type: &DataType,
) -> ArrayRef
where
    T::Native: ArrowNativeTypeOp,
{
    let mut out: Vec<T::Native> = Vec::with_capacity(frames.len());
    let mut ob = NullBufferBuilder::new(frames.len());
    run_moving_extreme::<T, MAX>(arr, state, last, frames, &mut out, &mut ob);
    Arc::new(
        PrimitiveArray::<T>::new(ScalarBuffer::from(out), ob.finish())
            .with_data_type(data_type.clone()),
    )
}

/// Sliding MIN/MAX over the frames of several complete partitions: the queue is emptied at
/// every segment.
pub fn moving_extreme_frames_segments<T: ArrowPrimitiveType, const MAX: bool>(
    arr: &PrimitiveArray<T>,
    state: &mut MovingExtreme<T::Native, MAX>,
    segments: &[Range<usize>],
    frames: &[Range<usize>],
    data_type: &DataType,
) -> ArrayRef
where
    T::Native: ArrowNativeTypeOp,
{
    let mut out: Vec<T::Native> = Vec::with_capacity(frames.len());
    let mut ob = NullBufferBuilder::new(frames.len());
    for seg in segments {
        state.clear();
        run_moving_extreme::<T, MAX>(
            arr,
            state,
            &(seg.start..seg.start),
            &frames[seg.start..seg.end],
            &mut out,
            &mut ob,
        );
    }
    Arc::new(
        PrimitiveArray::<T>::new(ScalarBuffer::from(out), ob.finish())
            .with_data_type(data_type.clone()),
    )
}

#[inline(always)]
fn better<N: ArrowNativeTypeOp, const MAX: bool>(a: N, b: N) -> N {
    if MAX {
        if b.is_gt(a) { b } else { a }
    } else if b.is_lt(a) {
        b
    } else {
        a
    }
}

/// The running MIN/MAX protocol over `frames` (frames only grow), appending one result per
/// frame. `last_end` and `cur_ext` are the end of the rows added so far and their extreme.
#[inline(always)]
fn run_running_extreme<T: ArrowPrimitiveType, const MAX: bool>(
    arr: &PrimitiveArray<T>,
    last_end: &mut usize,
    cur_ext: &mut Option<T::Native>,
    frames: &[Range<usize>],
    out: &mut Vec<T::Native>,
    ob: &mut NullBufferBuilder,
) where
    T::Native: ArrowNativeTypeOp,
{
    let nulls = arr.nulls();
    let vals = arr.values();
    for cur in frames {
        if cur.start == cur.end {
            out.push(T::Native::default());
            ob.append_null();
        } else {
            match nulls {
                None => {
                    for &v in &vals[*last_end..cur.end] {
                        *cur_ext = Some(match *cur_ext {
                            Some(m) => better::<_, MAX>(m, v),
                            None => v,
                        });
                    }
                }
                Some(n) => {
                    for i in *last_end..cur.end {
                        if n.is_valid(i) {
                            let v = vals[i];
                            *cur_ext = Some(match *cur_ext {
                                Some(m) => better::<_, MAX>(m, v),
                                None => v,
                            });
                        }
                    }
                }
            }
            *last_end = (*last_end).max(cur.end);
            match *cur_ext {
                Some(v) => {
                    out.push(v);
                    ob.append_non_null();
                }
                None => {
                    out.push(T::Native::default());
                    ob.append_null();
                }
            }
        }
    }
}

/// Running MIN/MAX (plain protocol: frames only grow) over frames. `state` is the extreme
/// of everything added so far.
pub fn running_extreme_frames<T: ArrowPrimitiveType, const MAX: bool>(
    arr: &PrimitiveArray<T>,
    state: &mut Option<T::Native>,
    last: &Range<usize>,
    frames: &[Range<usize>],
    data_type: &DataType,
) -> ArrayRef
where
    T::Native: ArrowNativeTypeOp,
{
    let mut out: Vec<T::Native> = Vec::with_capacity(frames.len());
    let mut ob = NullBufferBuilder::new(frames.len());
    let mut last_end = last.end;
    let mut cur_ext = *state;
    run_running_extreme::<T, MAX>(
        arr,
        &mut last_end,
        &mut cur_ext,
        frames,
        &mut out,
        &mut ob,
    );
    *state = cur_ext;
    Arc::new(
        PrimitiveArray::<T>::new(ScalarBuffer::from(out), ob.finish())
            .with_data_type(data_type.clone()),
    )
}

/// Running MIN/MAX over the frames of several complete partitions: the extreme restarts at
/// every segment.
pub fn running_extreme_frames_segments<T: ArrowPrimitiveType, const MAX: bool>(
    arr: &PrimitiveArray<T>,
    segments: &[Range<usize>],
    frames: &[Range<usize>],
    data_type: &DataType,
) -> ArrayRef
where
    T::Native: ArrowNativeTypeOp,
{
    let mut out: Vec<T::Native> = Vec::with_capacity(frames.len());
    let mut ob = NullBufferBuilder::new(frames.len());
    for seg in segments {
        let mut last_end = seg.start;
        let mut cur_ext: Option<T::Native> = None;
        run_running_extreme::<T, MAX>(
            arr,
            &mut last_end,
            &mut cur_ext,
            &frames[seg.start..seg.end],
            &mut out,
            &mut ob,
        );
    }
    Arc::new(
        PrimitiveArray::<T>::new(ScalarBuffer::from(out), ob.finish())
            .with_data_type(data_type.clone()),
    )
}
