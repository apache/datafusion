// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Construction estimates for nested arrays and views. Source data stays alive
//! through interleave; a shared allocation is counted once, in full, even when
//! the selected view only references a slice of it.

use std::mem::size_of;

use arrow::array::{Array, AsArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{ArrowNativeType, DataType};
use arrow_data::{ByteView, MAX_INLINE_VIEW_LEN};

use super::builder::{aligned_buffer_bytes, bitmap_buffer_bytes, fixed_width};

pub(super) fn is_recursive(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8View
            | DataType::BinaryView
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Struct(_)
    )
}

pub(super) fn is_variable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::BinaryView
        | DataType::List(_)
        | DataType::LargeList(_) => true,
        DataType::Struct(fields) => fields.iter().any(|f| is_variable(f.data_type())),
        _ => false,
    }
}

pub(super) fn contains_view(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::List(field) | DataType::LargeList(field) => {
            contains_view(field.data_type())
        }
        DataType::Struct(fields) => {
            fields.iter().any(|field| contains_view(field.data_type()))
        }
        _ => false,
    }
}

/// Find a shared View allocation without creating an accounting container.
#[cfg(debug_assertions)]
pub(super) fn contains_view_buffer(array: &dyn Array, buffer: &Buffer) -> bool {
    let matches = |source: &Buffer| {
        source.data_ptr() == buffer.data_ptr() && source.capacity() >= buffer.capacity()
    };
    match array.data_type() {
        DataType::Utf8View => array.as_string_view().data_buffers().iter().any(matches),
        DataType::BinaryView => array.as_binary_view().data_buffers().iter().any(matches),
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .any(|child| contains_view_buffer(child.as_ref(), buffer)),
        DataType::List(_) => {
            contains_view_buffer(array.as_list::<i32>().values().as_ref(), buffer)
        }
        DataType::LargeList(_) => {
            contains_view_buffer(array.as_list::<i64>().values().as_ref(), buffer)
        }
        _ => false,
    }
}

/// Count newly allocated output buffers and verify shared View provenance.
/// Input reservations must independently cover the source buffers throughout
/// construction; matching pointers alone does not establish that coverage.
#[cfg(any(test, debug_assertions))]
pub(super) fn new_buffer_bytes(
    array: &dyn Array,
    is_input_buffer: &impl Fn(&Buffer) -> bool,
) -> usize {
    let validity = array.nulls().map_or(0, |nulls| nulls.buffer().capacity());
    match array.data_type() {
        DataType::Utf8View | DataType::BinaryView => {
            let (views, buffers) = if matches!(array.data_type(), DataType::Utf8View) {
                let array = array.as_string_view();
                (array.views().inner(), array.data_buffers())
            } else {
                let array = array.as_binary_view();
                (array.views().inner(), array.data_buffers())
            };
            for buffer in buffers.iter() {
                debug_assert!(
                    is_input_buffer(buffer),
                    "shared output buffer does not come from a live input"
                );
            }
            validity + views.capacity()
        }
        DataType::Struct(_) => {
            validity
                + array
                    .as_struct()
                    .columns()
                    .iter()
                    .map(|child| new_buffer_bytes(child.as_ref(), is_input_buffer))
                    .sum::<usize>()
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            validity
                + list.offsets().inner().inner().capacity()
                + new_buffer_bytes(list.values().as_ref(), is_input_buffer)
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            validity
                + list.offsets().inner().inner().capacity()
                + new_buffer_bytes(list.values().as_ref(), is_input_buffer)
        }
        _ => array.get_buffer_memory_size(),
    }
}

/// The allocation identity is the unsliced data pointer, as in
/// `RecordBatchMemoryCounter`. Slices retain the original allocation's capacity.
#[derive(Default)]
struct SharedBuffers {
    // Sorted before scanning rows, so selecting a buffer never allocates.
    allocations: Vec<(usize, usize, bool)>,
    bytes: usize,
}

impl SharedBuffers {
    fn insert(&mut self, buffer: &Buffer) -> Option<()> {
        if buffer.capacity() == 0 {
            return Some(());
        }
        let id = buffer.data_ptr().as_ptr() as usize;
        let index = self.allocations.binary_search_by_key(&id, |a| a.0).ok()?;
        let (_, capacity, selected) = &mut self.allocations[index];
        if !*selected {
            self.bytes = self.bytes.checked_add(*capacity)?;
            *selected = true;
        }
        Some(())
    }
}

// Vec::collect for Option may grow geometrically. Reserve the exact element
// count instead, making the estimator's workspace known before allocation.
fn collect_exact<T>(values: impl ExactSizeIterator<Item = Option<T>>) -> Option<Vec<T>> {
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        result.push(value?);
    }
    Some(result)
}

/// Upper bound for the estimator's own heap workspace. This traversal does not
/// allocate. Input arrays may share buffers; duplicates are deliberately retained
/// in this bound, since the sorted allocation table has that initial capacity.
pub(super) fn workspace_bytes(array: &dyn Array, first_batch: bool) -> Option<usize> {
    let mut bytes = size_of::<&dyn Array>();
    if first_batch {
        bytes = bytes.checked_add(size_of::<ArraySize<'_>>())?;
    }
    match array.data_type() {
        DataType::Utf8View | DataType::BinaryView => {
            let count = if matches!(array.data_type(), DataType::Utf8View) {
                array.as_string_view().data_buffers().len()
            } else {
                array.as_binary_view().data_buffers().len()
            };
            bytes = bytes
                .checked_add((1 + usize::from(first_batch)) * size_of::<usize>())?
                .checked_add(count.checked_mul(
                    size_of::<(usize, usize, bool)>() + size_of::<bool>(),
                )?)?;
        }
        DataType::Struct(_) => {
            for child in array.as_struct().columns() {
                bytes =
                    bytes.checked_add(workspace_bytes(child.as_ref(), first_batch)?)?;
            }
        }
        DataType::List(_) => {
            bytes = bytes.checked_add(workspace_bytes(
                array.as_list::<i32>().values().as_ref(),
                first_batch,
            )?)?
        }
        DataType::LargeList(_) => {
            bytes = bytes.checked_add(workspace_bytes(
                array.as_list::<i64>().values().as_ref(),
                first_batch,
            )?)?
        }
        _ => {}
    }
    Some(bytes)
}

/// New construction buffers and scratch for one contiguous written spill piece,
/// without allocating an estimator. The caller combines this with the post-GC
/// sliced size for serialized input data. View backing data is already covered
/// by the input grant: its pre-GC allocation capacity is not another allocation
/// or the size of the batch that will be read from this file.
pub(super) fn contiguous_bytes(
    array: &dyn Array,
    start: usize,
    end: usize,
) -> Option<usize> {
    content_bytes(array, start, end, false)
}

/// Content for one source row, excluding input-dependent reference/mapping
/// tables. Every selected node allows validity even when this source has no
/// nulls: another input can enable the interleaver's global validity flag.
fn content_bytes(
    array: &dyn Array,
    start: usize,
    end: usize,
    progress: bool,
) -> Option<usize> {
    let rows = end.checked_sub(start)?;
    let validity = if rows != 0 && (progress || array.null_count() != 0) {
        bitmap_buffer_bytes(rows)?
    } else {
        0
    };
    let values = match array.data_type() {
        DataType::Null => return Some(0),
        DataType::Boolean => bitmap_buffer_bytes(rows)?,
        DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary => {
            let (width, bytes) = match array.data_type() {
                DataType::Utf8 => {
                    let o = array.as_string::<i32>().value_offsets();
                    (
                        size_of::<i32>(),
                        o[end].as_usize().checked_sub(o[start].as_usize())?,
                    )
                }
                DataType::Binary => {
                    let o = array.as_binary::<i32>().value_offsets();
                    (
                        size_of::<i32>(),
                        o[end].as_usize().checked_sub(o[start].as_usize())?,
                    )
                }
                DataType::LargeUtf8 => {
                    let o = array.as_string::<i64>().value_offsets();
                    (
                        size_of::<i64>(),
                        o[end].as_usize().checked_sub(o[start].as_usize())?,
                    )
                }
                _ => {
                    let o = array.as_binary::<i64>().value_offsets();
                    (
                        size_of::<i64>(),
                        o[end].as_usize().checked_sub(o[start].as_usize())?,
                    )
                }
            };
            if rows == 0 {
                aligned_buffer_bytes(width)?
            } else {
                rows.checked_add(1)?
                    .checked_mul(width)?
                    .checked_add(bytes)?
            }
        }
        DataType::Utf8View | DataType::BinaryView => {
            if rows == 0 {
                return Some(0);
            }
            let buffers = if matches!(array.data_type(), DataType::Utf8View) {
                array.as_string_view().data_buffers()
            } else {
                array.as_binary_view().data_buffers()
            };
            let descriptors = rows.min(buffers.len());
            let descriptors = if descriptors == 0 {
                0
            } else {
                descriptors
                    .checked_next_power_of_two()?
                    .max(4)
                    .checked_mul(size_of::<Buffer>())?
                    .checked_mul(2)?
            };
            let content = rows
                .checked_mul(size_of::<u128>())?
                .checked_add(descriptors)?;
            if progress {
                content
            } else {
                content
                    .checked_add(buffers.len().checked_mul(size_of::<Option<u32>>())?)?
                    .checked_add(2 * size_of::<usize>())?
            }
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0usize, |n, a| {
                n.checked_add(content_bytes(a.as_ref(), start, end, progress)?)
            })?,
        DataType::List(_) | DataType::LargeList(_) => {
            let (width, child, a, b) = if matches!(array.data_type(), DataType::List(_)) {
                let list = array.as_list::<i32>();
                let o = list.value_offsets();
                (
                    size_of::<i32>(),
                    list.values(),
                    o[start].as_usize(),
                    o[end].as_usize(),
                )
            } else {
                let list = array.as_list::<i64>();
                let o = list.value_offsets();
                (
                    size_of::<i64>(),
                    list.values(),
                    o[start].as_usize(),
                    o[end].as_usize(),
                )
            };
            let offsets = if rows == 0 {
                aligned_buffer_bytes(width)?
            } else {
                rows.checked_add(1)?.checked_mul(width)?
            };
            let scratch = if child.data_type().is_primitive() {
                0
            } else {
                b.checked_sub(a)?.checked_mul(size_of::<(usize, usize)>())?
            };
            offsets
                .checked_add(content_bytes(child.as_ref(), a, b, progress)?)?
                .checked_add(scratch)?
        }
        DataType::FixedSizeBinary(width) => {
            aligned_buffer_bytes(rows.checked_mul(usize::try_from(*width).ok()?)?)?
        }
        dt => rows.checked_mul(fixed_width(dt)?)?,
    };
    values.checked_add(validity)
}

/// Independently maximized per-file bounds for a pure-spill merge. They are
/// computed from actual post-GC written arrays, without constructing an estimator.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProgressStats {
    pub workspace: usize,
    pub row_content: usize,
}

impl ProgressStats {
    pub(crate) fn from_batch(batch: &arrow::array::RecordBatch) -> Option<Self> {
        let workspace = batch.columns().iter().try_fold(0usize, |sum, column| {
            sum.checked_add(workspace_bytes(column.as_ref(), true)?)
        })?;
        let mut row_content = 0;
        for row in 0..batch.num_rows() {
            let bytes = batch.columns().iter().try_fold(0usize, |sum, column| {
                sum.checked_add(content_bytes(column.as_ref(), row, row + 1, true)?)
            })?;
            row_content = row_content.max(bytes);
        }
        Some(Self {
            workspace,
            row_content,
        })
    }

    pub(crate) fn include(&mut self, other: Self) {
        self.workspace = self.workspace.max(other.workspace);
        self.row_content = self.row_content.max(other.row_content);
    }
}

/// Accumulates selected rows without materializing List child indices. Each row
/// and child range is visited once while finding the largest prefix.
pub(super) struct InterleaveSize<'a> {
    columns: Vec<ArraySize<'a>>,
    shared: SharedBuffers,
}

impl<'a> InterleaveSize<'a> {
    pub(super) fn new(
        columns: impl ExactSizeIterator<Item = Vec<&'a dyn Array>>,
    ) -> Option<Self> {
        let columns = collect_exact(columns.map(ArraySize::new))?;
        let count = columns
            .iter()
            .try_fold(0usize, |n, column| n.checked_add(column.source_buffers()?))?;
        let mut allocations = Vec::with_capacity(count);
        for column in &columns {
            column.collect_buffers(&mut allocations);
        }
        allocations.sort_unstable_by_key(|a| a.0);
        // Keep the maximum observed capacity for aliases of the same allocation.
        allocations.dedup_by(|a, b| {
            if a.0 == b.0 {
                b.1 = b.1.max(a.1);
                true
            } else {
                false
            }
        });
        Some(Self {
            columns,
            shared: SharedBuffers {
                allocations,
                bytes: 0,
            },
        })
    }

    pub(super) fn push(&mut self, batch: usize, row: usize) -> Option<()> {
        for column in &mut self.columns {
            column.push(batch, row, &mut self.shared)?;
        }
        Some(())
    }

    pub(super) fn new_bytes(&self) -> Option<usize> {
        self.columns
            .iter()
            .try_fold(0usize, |bytes, column| bytes.checked_add(column.bytes()?))
    }

    /// Monotone lower bound: unselected child arrays contribute zero instead
    /// of new_empty_array's aligned offsets. All remaining terms only increase.
    pub(super) fn prefix_lower_bound(&self) -> Option<usize> {
        self.columns.iter().try_fold(0usize, |bytes, column| {
            bytes.checked_add(column.bytes_with_empty(false)?)
        })
    }

    pub(super) fn shared_bytes(&self) -> usize {
        self.shared.bytes
    }

    pub(super) fn bytes(&self) -> Option<usize> {
        self.new_bytes()?.checked_add(self.shared.bytes)
    }
}

struct ArraySize<'a> {
    arrays: Vec<&'a dyn Array>,
    rows: usize,
    has_nulls: bool,
    kind: Kind<'a>,
}

enum Kind<'a> {
    Null,
    Boolean,
    Fixed {
        width: usize,
        aligned: bool,
    },
    Bytes {
        offset: usize,
        values: usize,
    },
    View {
        offsets: Vec<usize>,
        selected: Vec<bool>,
        selected_count: usize,
    },
    Struct(Vec<ArraySize<'a>>),
    List {
        offset: usize,
        child: Box<ArraySize<'a>>,
        primitive_child: bool,
    },
}

impl<'a> ArraySize<'a> {
    fn new(arrays: Vec<&'a dyn Array>) -> Option<Self> {
        let data_type = arrays.first()?.data_type();
        if arrays.iter().any(|a| a.data_type() != data_type) {
            return None;
        }
        let kind = match data_type {
            DataType::Null => Kind::Null,
            DataType::Boolean => Kind::Boolean,
            DataType::Binary | DataType::Utf8 => Kind::Bytes {
                offset: size_of::<i32>(),
                values: 0,
            },
            DataType::LargeBinary | DataType::LargeUtf8 => Kind::Bytes {
                offset: size_of::<i64>(),
                values: 0,
            },
            DataType::Utf8View | DataType::BinaryView => {
                let mut offsets = Vec::with_capacity(arrays.len() + 1);
                offsets.push(0usize);
                for a in &arrays {
                    let count = if matches!(data_type, DataType::Utf8View) {
                        a.as_string_view().data_buffers().len()
                    } else {
                        a.as_binary_view().data_buffers().len()
                    };
                    offsets.push(offsets.last()?.checked_add(count)?);
                }
                let total = *offsets.last()?;
                Kind::View {
                    offsets,
                    selected: vec![false; total],
                    selected_count: 0,
                }
            }
            DataType::Struct(fields) => {
                Kind::Struct(collect_exact((0..fields.len()).map(|i| {
                    Self::new(
                        arrays
                            .iter()
                            .map(|a| a.as_struct().column(i).as_ref())
                            .collect(),
                    )
                }))?)
            }
            DataType::List(field) | DataType::LargeList(field) => {
                let small = matches!(data_type, DataType::List(_));
                let children = arrays
                    .iter()
                    .map(|a| {
                        if small {
                            a.as_list::<i32>().values().as_ref()
                        } else {
                            a.as_list::<i64>().values().as_ref()
                        }
                    })
                    .collect();
                Kind::List {
                    offset: if small {
                        size_of::<i32>()
                    } else {
                        size_of::<i64>()
                    },
                    child: Box::new(Self::new(children)?),
                    primitive_child: field.data_type().is_primitive(),
                }
            }
            DataType::FixedSizeBinary(width) => Kind::Fixed {
                width: usize::try_from(*width).ok()?,
                aligned: true,
            },
            other => Kind::Fixed {
                width: fixed_width(other)?,
                aligned: false,
            },
        };
        Some(Self {
            has_nulls: arrays.iter().any(|a| a.null_count() > 0),
            arrays,
            rows: 0,
            kind,
        })
    }

    fn source_buffers(&self) -> Option<usize> {
        match &self.kind {
            Kind::View { selected, .. } => Some(selected.len()),
            Kind::Struct(children) => children
                .iter()
                .try_fold(0usize, |n, child| n.checked_add(child.source_buffers()?)),
            Kind::List { child, .. } => child.source_buffers(),
            _ => Some(0),
        }
    }

    fn collect_buffers(&self, allocations: &mut Vec<(usize, usize, bool)>) {
        match &self.kind {
            Kind::View { .. } => {
                for array in &self.arrays {
                    let buffers = if matches!(array.data_type(), DataType::Utf8View) {
                        array.as_string_view().data_buffers()
                    } else {
                        array.as_binary_view().data_buffers()
                    };
                    allocations.extend(
                        buffers.iter().map(|b| {
                            (b.data_ptr().as_ptr() as usize, b.capacity(), false)
                        }),
                    );
                }
            }
            Kind::Struct(children) => {
                for child in children {
                    child.collect_buffers(allocations);
                }
            }
            Kind::List { child, .. } => child.collect_buffers(allocations),
            _ => {}
        }
    }

    fn push(
        &mut self,
        batch: usize,
        row: usize,
        shared: &mut SharedBuffers,
    ) -> Option<()> {
        self.push_range(batch, row, row.checked_add(1)?, shared)
    }

    fn push_range(
        &mut self,
        batch: usize,
        start: usize,
        end: usize,
        shared: &mut SharedBuffers,
    ) -> Option<()> {
        self.rows = self.rows.checked_add(end.checked_sub(start)?)?;
        let array = self.arrays.get(batch)?;
        match &mut self.kind {
            Kind::Bytes { values, .. } => {
                let length = match array.data_type() {
                    DataType::Binary => {
                        let o = array.as_binary::<i32>().value_offsets();
                        o[end].as_usize().checked_sub(o[start].as_usize())?
                    }
                    DataType::LargeBinary => {
                        let o = array.as_binary::<i64>().value_offsets();
                        o[end].as_usize().checked_sub(o[start].as_usize())?
                    }
                    DataType::Utf8 => {
                        let o = array.as_string::<i32>().value_offsets();
                        o[end].as_usize().checked_sub(o[start].as_usize())?
                    }
                    DataType::LargeUtf8 => {
                        let o = array.as_string::<i64>().value_offsets();
                        o[end].as_usize().checked_sub(o[start].as_usize())?
                    }
                    _ => return None,
                };
                *values = values.checked_add(length)?;
            }
            Kind::View {
                offsets,
                selected,
                selected_count,
            } => {
                let (views, buffers) = if matches!(array.data_type(), DataType::Utf8View)
                {
                    let a = array.as_string_view();
                    (a.views(), a.data_buffers())
                } else {
                    let a = array.as_binary_view();
                    (a.views(), a.data_buffers())
                };
                // Arrow interleave follows the raw view even for a null row.
                for &raw in &views[start..end] {
                    if raw as u32 > MAX_INLINE_VIEW_LEN {
                        let index = ByteView::from(raw).buffer_index as usize;
                        shared.insert(buffers.get(index)?)?;
                        let selected =
                            selected.get_mut(offsets[batch].checked_add(index)?)?;
                        if !*selected {
                            *selected = true;
                            *selected_count += 1;
                        }
                    }
                }
            }
            Kind::Struct(children) => {
                for child in children {
                    child.push_range(batch, start, end, shared)?;
                }
            }
            Kind::List { offset, child, .. } => {
                let (child_start, child_end) = if *offset == size_of::<i32>() {
                    let offsets = array.as_list::<i32>().value_offsets();
                    (offsets[start].as_usize(), offsets[end].as_usize())
                } else {
                    let offsets = array.as_list::<i64>().value_offsets();
                    (offsets[start].as_usize(), offsets[end].as_usize())
                };
                // Parent nulls do not suppress copying their physical child ranges.
                child.push_range(batch, child_start, child_end, shared)?;
            }
            _ => {}
        }
        Some(())
    }

    fn bytes(&self) -> Option<usize> {
        self.bytes_with_empty(true)
    }

    fn bytes_with_empty(&self, count_empty: bool) -> Option<usize> {
        if self.rows == 0 {
            if !count_empty {
                return Some(0);
            }
            // interleave with no child indices calls new_empty_array; its offset
            // buffers use MutableBuffer and retain the allocator alignment.
            return match &self.kind {
                Kind::Bytes { offset, .. } => aligned_buffer_bytes(*offset),
                Kind::List { offset, child, .. } => aligned_buffer_bytes(*offset)?
                    .checked_add(child.bytes_with_empty(count_empty)?),
                Kind::Struct(children) => {
                    children.iter().try_fold(0usize, |bytes, child| {
                        bytes.checked_add(child.bytes_with_empty(count_empty)?)
                    })
                }
                _ => Some(0),
            };
        }
        let validity = if self.has_nulls {
            bitmap_buffer_bytes(self.rows)?
        } else {
            0
        };
        let values = match &self.kind {
            Kind::Null => return Some(0),
            Kind::Boolean => bitmap_buffer_bytes(self.rows)?,
            Kind::Fixed { width, aligned } => {
                let bytes = self.rows.checked_mul(*width)?;
                if *aligned {
                    aligned_buffer_bytes(bytes)?
                } else {
                    bytes
                }
            }
            Kind::Bytes { offset, values } => self
                .rows
                .checked_add(1)?
                .checked_mul(*offset)?
                .checked_add(*values)?,
            Kind::View {
                selected,
                selected_count,
                ..
            } => {
                // interleave_views allocates a mapping for ALL source entries,
                // not just the buffers referenced by the selected prefix.
                let mapping = selected.len().checked_mul(size_of::<Option<u32>>())?;
                let offsets = self
                    .arrays
                    .len()
                    .checked_add(1)?
                    .checked_mul(size_of::<usize>())?;
                // The temporary Vec grows by doubling before conversion to Arc.
                let descriptors = if *selected_count == 0 {
                    0
                } else {
                    selected_count
                        .checked_next_power_of_two()?
                        .max(4)
                        .checked_mul(size_of::<Buffer>())?
                        .checked_mul(2)?
                };
                self.rows
                    .checked_mul(size_of::<u128>())?
                    .checked_add(mapping)?
                    .checked_add(offsets)?
                    .checked_add(descriptors)?
            }
            Kind::Struct(children) => {
                children.iter().try_fold(0usize, |bytes, child| {
                    bytes.checked_add(child.bytes_with_empty(count_empty)?)
                })?
            }
            Kind::List {
                offset,
                child,
                primitive_child,
            } => {
                let offsets = self.rows.checked_add(1)?.checked_mul(*offset)?;
                let scratch = if *primitive_child {
                    0
                } else {
                    child.rows.checked_mul(size_of::<(usize, usize)>())?
                };
                offsets
                    .checked_add(child.bytes_with_empty(count_empty)?)?
                    .checked_add(scratch)?
            }
        };
        // Recursive Arrow interleave keeps typed source-reference tables alive
        // while constructing its child arrays. This bounds both those tables
        // and the fat-pointer child-reference vectors (including primitive Lists).
        let references = self.arrays.len().checked_mul(2 * size_of::<&dyn Array>())?;
        values.checked_add(validity)?.checked_add(references)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BinaryViewArray, LargeListArray, ListArray, NullArray, StringArray,
        StringViewArray, StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::compute::interleave;
    use arrow::datatypes::Field;
    use datafusion_common::utils::memory::RecordBatchMemoryCounter;
    use std::sync::Arc;

    #[test]
    fn shared_view_detection_follows_supported_descendants() {
        let view = DataType::BinaryView;
        let structure =
            DataType::Struct(vec![Arc::new(Field::new("v", view.clone(), false))].into());
        for data_type in [
            view,
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8View, false))),
            structure.clone(),
            DataType::LargeList(Arc::new(Field::new_list_field(structure, false))),
        ] {
            assert!(contains_view(&data_type));
        }
        assert!(!contains_view(&DataType::List(Arc::new(
            Field::new_list_field(DataType::Utf8, false)
        ))));
    }

    fn estimate(arrays: &[ArrayRef], indices: &[(usize, usize)]) -> usize {
        let mut size = InterleaveSize::new(std::iter::once(
            arrays.iter().map(|a| a.as_ref()).collect(),
        ))
        .unwrap();
        for &(batch, row) in indices {
            size.push(batch, row).unwrap();
        }
        let bytes = size.bytes().unwrap();
        let output = interleave(
            &arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
            indices,
        )
        .unwrap();
        assert!(RecordBatchMemoryCounter::new().count_array(output.as_ref()) <= bytes);
        bytes
    }

    fn sliced_views(buffer: Buffer, nulls: Option<NullBuffer>) -> ArrayRef {
        let view = ByteView::new(20, b"xxxx").as_u128();
        Arc::new(
            StringViewArray::try_new(vec![view, view].into(), vec![buffer], nulls)
                .unwrap(),
        )
    }

    #[test]
    fn shared_view_slices_count_the_full_allocation_once() {
        let buffer = Buffer::from(vec![b'x'; 64 * 1024]);
        let capacity = buffer.capacity();
        let first = buffer.slice_with_length(100, 30);
        let second = buffer.slice_with_length(1000, 30);
        assert_ne!(first.as_ptr(), second.as_ptr());
        assert_eq!(first.data_ptr(), second.data_ptr());
        let arrays = [
            sliced_views(first, None),
            sliced_views(second, Some(NullBuffer::from(vec![false, true]))),
        ];
        let indices = [(0, 0), (1, 0), (0, 1), (1, 1)];
        let bytes = estimate(&arrays, &indices);
        assert!(bytes >= capacity);
        assert!(
            bytes < capacity * 2,
            "slices must not duplicate retained capacity"
        );
        // Deduplicate the same allocation across columns as well as input batches.
        let mut size = InterleaveSize::new(
            (0..2).map(|_| arrays.iter().map(|a| a.as_ref()).collect()),
        )
        .unwrap();
        size.push(0, 0).unwrap();
        size.push(1, 0).unwrap();
        assert!(size.bytes().unwrap() < capacity * 2);
    }

    #[test]
    fn views_cover_inline_null_and_multiple_source_buffers() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringViewArray::from(vec![
                Some("tiny"),
                None,
                Some("a long string from the first source"),
            ])),
            Arc::new(StringViewArray::from(vec![
                Some("another long string in a separate buffer"),
                Some("b"),
                None,
            ])),
        ];
        estimate(&arrays, &[(1, 0), (0, 2), (0, 0), (1, 2), (0, 1)]);
        let binary: Vec<ArrayRef> = vec![Arc::new(BinaryViewArray::from_iter_values([
            b"long binary value with nonutf8 \xff".as_slice(),
        ]))];
        estimate(&binary, &[(0, 0), (0, 0)]);
    }

    #[test]
    fn nested_lists_include_physical_children_of_null_parents() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "a",
            "long physical null-parent value",
            "end",
        ]));
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        ));
        let arrays = [list.slice(1, 2), list.slice(0, 2)];
        estimate(&arrays, &[(0, 0), (1, 1), (0, 1)]);
        let outer = LargeListArray::new(
            Arc::new(Field::new_list_field(arrays[0].data_type().clone(), true)),
            OffsetBuffer::new(vec![0_i64, 2].into()),
            Arc::clone(&arrays[0]),
            None,
        );
        let fields = vec![Arc::new(Field::new(
            "nested",
            outer.data_type().clone(),
            true,
        ))]
        .into();
        let nested: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![Arc::new(outer)],
            Some(NullBuffer::from(vec![false])),
        ));
        estimate(&[nested], &[(0, 0), (0, 0)]);
    }

    #[test]
    fn list_views_keep_shared_buffers_and_empty_children_aligned() {
        let views = sliced_views(Buffer::from(vec![b'x'; 4096]), None);
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8View, false)),
            OffsetBuffer::new(vec![0, 2].into()),
            views,
            None,
        ));
        estimate(&[list], &[(0, 0), (0, 0)]);
        let empty: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 0, 0].into()),
            empty,
            None,
        ));
        estimate(&[list], &[(0, 1)]);
    }

    #[test]
    fn list_null_children_include_interleave_index_scratch() {
        let rows = 1_000_000;
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Null, true)),
            OffsetBuffer::new(vec![0, rows as i32].into()),
            Arc::new(NullArray::new(rows)),
            None,
        ));
        let mut size =
            InterleaveSize::new(std::iter::once(vec![array.as_ref()])).unwrap();
        size.push(0, 0).unwrap();
        assert_eq!(
            size.bytes(),
            Some(
                2 * size_of::<i32>()
                    + rows * size_of::<(usize, usize)>()
                    + 2 * size_of::<&dyn Array>()
            )
        );
        // Overflow must be reported before allocating Arrow's child-index Vec.
        let Kind::List { child, .. } = &mut size.columns[0].kind else {
            unreachable!()
        };
        child.rows = usize::MAX;
        assert_eq!(size.bytes(), None);
    }
}

#[cfg(test)]
mod progress_tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray, StringViewArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::compute::interleave;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    #[test]
    fn progress_covers_cross_input_validity_empty_children_and_view_descriptors() {
        // A single selected List row contains many View buffer entries, while
        // another row has an empty child range. Preserve both content bounds.
        let value = b"a sufficiently long value";
        let buffers = (0..17)
            .map(|_| Buffer::from(value.as_slice()))
            .collect::<Vec<_>>();
        let entries = (0..17)
            .map(|buffer_index| {
                ByteView {
                    length: value.len() as u32,
                    prefix: u32::from_le_bytes(value[..4].try_into().unwrap()),
                    buffer_index,
                    offset: 0,
                }
                .as_u128()
            })
            .collect::<Vec<_>>();
        let views: ArrayRef =
            Arc::new(StringViewArray::try_new(entries.into(), buffers, None).unwrap());
        let lists = |nullable: bool| -> ArrayRef {
            Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(DataType::Utf8View, false)),
                OffsetBuffer::new(vec![0i32, 0, 17].into()),
                Arc::clone(&views),
                nullable.then(|| vec![true, false].into()),
            ))
        };
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["", ""]));
        let nullable_strings: ArrayRef =
            Arc::new(StringArray::from(vec![None, Some("")]));
        let batches = [
            arrow::array::RecordBatch::try_from_iter(vec![
                ("list", lists(false)),
                ("string", Arc::clone(&strings)),
            ])
            .unwrap(),
            arrow::array::RecordBatch::try_from_iter(vec![
                ("list", lists(true)),
                ("string", nullable_strings),
            ])
            .unwrap(),
        ];
        let stats = batches
            .iter()
            .map(|batch| ProgressStats::from_batch(batch).unwrap())
            .collect::<Vec<_>>();
        let grant = stats.iter().map(|s| s.row_content).max().unwrap()
            + 2 * stats.iter().map(|s| s.workspace).sum::<usize>();
        for source in 0..2 {
            for row in 0..2 {
                let mut estimate =
                    InterleaveSize::new((0..2).map(|col| {
                        batches.iter().map(|b| b.column(col).as_ref()).collect()
                    }))
                    .unwrap();
                estimate.push(source, row).unwrap();
                assert!(estimate.new_bytes().unwrap() <= grant);
                for col in 0..2 {
                    let arrays = batches
                        .iter()
                        .map(|b| b.column(col).as_ref())
                        .collect::<Vec<_>>();
                    let output = interleave(&arrays, &[(source, row)]).unwrap();
                    assert!(new_buffer_bytes(output.as_ref(), &|_| true) <= grant);
                }
            }
        }
        // Universal validity must not disappear just because the selected
        // source has no nulls; empty Utf8 children retain aligned offsets.
        assert!(
            content_bytes(strings.as_ref(), 0, 1, true).unwrap()
                > contiguous_bytes(strings.as_ref(), 0, 1).unwrap()
        );
        let empty: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        assert_eq!(content_bytes(empty.as_ref(), 0, 0, true), Some(64));
        let primitive = Int32Array::from(vec![1]);
        assert_eq!(content_bytes(&primitive, 0, usize::MAX, true), None);
    }
}
