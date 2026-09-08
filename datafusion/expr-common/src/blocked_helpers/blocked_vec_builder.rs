use crate::blocked_helpers::take_n_helpers::BlockBuilder;
use crate::groups_accumulator::BlocksIndex;
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::ArrowNativeType;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Index, IndexMut, Range};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

/// Virtual bytes reserved per builder up front. Untouched pages cost nothing, so this
/// is sized to practically never fill; the live window is relocated (and doubled if
/// needed) if it ever does. Halved until the kernel accepts it on strict overcommit setups
const RESERVED_BYTES: usize = 10 << 30;

/// Smallest reservation worth falling back to
const MIN_RESERVED_BYTES: usize = 64 << 20;

/// Anonymous private mapping shared between a builder and the blocks it handed out.
///
/// Pages are returned to the OS from the front once neither the builder (everything
/// below `head`) nor any handed out block still needs them
#[derive(Debug)]
struct Region {
    base: *mut u8,
    cap: usize,
    page: usize,
    state: Mutex<RegionState>,
}

#[derive(Debug)]
struct RegionState {
    /// Bytes below which the builder no longer needs anything
    head: usize,
    /// Bytes already returned to the OS, always a page multiple
    unmapped: usize,
    /// Start byte of every handed out block that is still alive, with a count
    /// since a zero item take can hand out the same start twice
    live_blocks: BTreeMap<usize, usize>,
}

// The mapping is only reached through `&self` methods that go through the mutex,
// or through the builder that exclusively owns the live window
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    /// Reserve `cap` bytes, settling for less (down to `min`) when the kernel refuses
    fn map(cap: usize, min: usize) -> Arc<Self> {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let mut cap = cap.next_multiple_of(page);
        let base = loop {
            let base = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    cap,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                    -1,
                    0,
                )
            };
            if base != libc::MAP_FAILED {
                break base;
            }
            assert!(cap > min, "mmap of {cap} bytes failed");
            cap = (cap / 2).max(min).next_multiple_of(page);
        };
        // Ask for transparent huge pages. In the default `madvise` THP mode anonymous
        // mappings stay on 4K pages, which costs a page fault per 4K on first touch and
        // many more TLB misses on the random access the hash tables do, while the
        // global allocator's memory is already huge page backed
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(base, cap, libc::MADV_HUGEPAGE);
        }
        Arc::new(Self {
            base: base.cast::<u8>(),
            cap,
            page,
            state: Mutex::new(RegionState {
                head: 0,
                unmapped: 0,
                live_blocks: BTreeMap::new(),
            }),
        })
    }

    /// The builder no longer needs anything below `byte_offset`
    fn set_head(&self, byte_offset: usize) {
        let mut state = self.state.lock().unwrap();
        state.head = byte_offset;
        self.sweep(&mut state);
    }

    /// Claim `[start, ..)` so its pages stay mapped until the guard is dropped
    fn claim(self: &Arc<Self>, start: usize) -> BlockGuard {
        *self
            .state
            .lock()
            .unwrap()
            .live_blocks
            .entry(start)
            .or_default() += 1;
        BlockGuard {
            region: Arc::clone(self),
            start,
        }
    }

    fn release(&self, start: usize) {
        let mut state = self.state.lock().unwrap();
        let count = state
            .live_blocks
            .get_mut(&start)
            .expect("released block must be live");
        *count -= 1;
        if *count == 0 {
            state.live_blocks.remove(&start);
        }
        self.sweep(&mut state);
    }

    /// Return every whole page nobody needs anymore to the OS
    fn sweep(&self, state: &mut RegionState) {
        let first_live_block = state.live_blocks.keys().next().copied();
        let free_end =
            state.head.min(first_live_block.unwrap_or(usize::MAX)) & !(self.page - 1);
        if free_end > state.unmapped {
            let rc = unsafe {
                libc::munmap(
                    self.base.add(state.unmapped).cast(),
                    free_end - state.unmapped,
                )
            };
            assert_eq!(rc, 0, "munmap failed");
            state.unmapped = free_end;
        }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        let unmapped = self.state.lock().unwrap().unmapped;
        if self.cap > unmapped {
            unsafe { libc::munmap(self.base.add(unmapped).cast(), self.cap - unmapped) };
        }
    }
}

/// Owner of a handed out block, releases its pages when the last buffer is dropped
#[derive(Debug)]
struct BlockGuard {
    region: Arc<Region>,
    start: usize,
}

impl Drop for BlockGuard {
    fn drop(&mut self) {
        self.region.release(self.start);
    }
}

/// A block taken out of a [`CopyItemBlockedVecBuilder`], exclusively owned like a `Vec`
/// so it can be mutated in place, and convertible into an arrow buffer without copying.
/// Its pages are returned to the OS once the block (or the buffer made from it) is dropped
pub struct MmapVec<T: Copy> {
    ptr: NonNull<T>,
    len: usize,
    /// `None` for an empty block that borrows no pages
    guard: Option<BlockGuard>,
}

// The block owns its range of the mapping exclusively, items are plain data
unsafe impl<T: Copy + Send> Send for MmapVec<T> {}
unsafe impl<T: Copy + Sync> Sync for MmapVec<T> {}

impl<T: Copy> MmapVec<T> {
    pub fn as_slice(&self) -> &[T] {
        self
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self
    }

    /// Wrap into an arrow buffer, the pages stay mapped for as long as the buffer lives
    pub fn into_buffer(self) -> Buffer {
        match self.guard {
            None => Buffer::from(&[]),
            Some(guard) => unsafe {
                Buffer::from_custom_allocation(
                    self.ptr.cast::<u8>(),
                    self.len * size_of::<T>(),
                    Arc::new(guard),
                )
            },
        }
    }

    pub fn into_scalar_buffer(self) -> ScalarBuffer<T>
    where
        T: ArrowNativeType,
    {
        let len = self.len;
        ScalarBuffer::new(self.into_buffer(), 0, len)
    }
}

impl<T: Copy> Deref for MmapVec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<T: Copy> DerefMut for MmapVec<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl<T: Copy> AsRef<[T]> for MmapVec<T> {
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<T: ArrowNativeType> From<MmapVec<T>> for ScalarBuffer<T> {
    fn from(block: MmapVec<T>) -> Self {
        block.into_scalar_buffer()
    }
}

impl<T: Copy> From<MmapVec<T>> for Buffer {
    fn from(block: MmapVec<T>) -> Self {
        block.into_buffer()
    }
}

impl<T: Copy + Debug> Debug for MmapVec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.as_slice(), f)
    }
}

impl<T: Copy + PartialEq, U: AsRef<[T]>> PartialEq<U> for MmapVec<T> {
    fn eq(&self, other: &U) -> bool {
        self.as_slice() == other.as_ref()
    }
}

/// Items live contiguously in an mmap'ed region at `[head, tail)`, so any item is
/// reachable by plain offset. Blocks are only a view over that range: fixed sizing
/// computes them from `head`, manual sizing records where each block starts.
/// Taking from the front advances `head` and unmaps the pages it left behind.
#[derive(Debug)]
pub struct CopyItemBlockedVecBuilder<const FIXED_BLOCK_SIZING: bool, T: Copy> {
    region: Arc<Region>,
    /// Absolute item index of the first live item
    head: usize,
    /// Absolute item index one past the last live item
    tail: usize,
    block_size: usize,
    /// Fixed sizing only: absolute item index at which the current block is full
    next_block_end: usize,
    /// Manual sizing only: absolute start of every block, `block_starts[0] == head`
    block_starts: VecDeque<usize>,
    _t: PhantomData<T>,
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy> Drop
    for CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    fn drop(&mut self) {
        self.region.set_head(self.region.cap);
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy>
    CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    const ITEM: usize = size_of::<T>();

    pub fn new(block_size: usize) -> Self {
        Self::with_reservation(block_size, RESERVED_BYTES)
    }

    /// Reserve `reserved_bytes` of address space instead of the default
    pub fn with_reservation(block_size: usize, reserved_bytes: usize) -> Self {
        assert!(Self::ITEM > 0, "zero sized items are not supported");
        if FIXED_BLOCK_SIZING {
            assert_ne!(block_size, 0, "block size must be greater than 0");
        }
        let mut this = Self {
            region: Region::map(reserved_bytes, MIN_RESERVED_BYTES.min(reserved_bytes)),
            head: 0,
            tail: 0,
            block_size,
            next_block_end: 0,
            block_starts: VecDeque::from([0]),
            _t: PhantomData,
        };
        this.relayout_fixed();
        this
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.tail - self.head
    }

    #[inline(always)]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn num_blocks(&self) -> usize {
        if FIXED_BLOCK_SIZING {
            self.len() / self.block_size + 1
        } else {
            self.block_starts.len()
        }
    }

    /// Mapped bytes that may hold data, from the first live page to the page after `tail`
    pub fn allocated_size(&self) -> usize {
        let page = self.region.page;
        let live_start = (self.head * Self::ITEM) & !(page - 1);
        (self.tail * Self::ITEM).next_multiple_of(page) - live_start
            + self.block_starts.capacity() * size_of::<usize>()
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr_at(self.head), self.len()) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr_at(self.head).cast_mut(), self.len())
        }
    }

    pub fn block(&self, block_index: usize) -> &[T] {
        let Range { start, end } = self.block_range(block_index);
        &self.as_slice()[start - self.head..end - self.head]
    }

    pub fn current_block_len(&self) -> usize {
        self.tail - self.current_block_start()
    }

    pub fn start_new_block(&mut self) {
        assert!(
            !FIXED_BLOCK_SIZING,
            "fixed sizing finishes blocks on its own"
        );
        self.block_starts.push_back(self.tail);
    }

    /// Push an item and return whether the current block is now full
    #[inline]
    pub fn push(&mut self, value: T) -> bool {
        self.reserve(1);
        unsafe { self.ptr_at(self.tail).cast_mut().write(value) };
        self.tail += 1;
        if FIXED_BLOCK_SIZING && self.tail == self.next_block_end {
            self.next_block_end += self.block_size;
            true
        } else {
            false
        }
    }

    /// Make `n` more items live without writing them, they read as all zero bytes
    /// since the mapping is zero filled and their pages stay untouched
    ///
    /// # Safety
    /// `T` must be valid when all of its bytes are zero
    pub unsafe fn advance_untouched(&mut self, n: usize) {
        self.reserve(n);
        self.tail += n;
        self.relayout_fixed();
    }

    pub fn extend_from_slice(&mut self, slice: &[T]) {
        self.reserve(slice.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                slice.as_ptr(),
                self.ptr_at(self.tail).cast_mut(),
                slice.len(),
            )
        };
        self.tail += slice.len();
        self.relayout_fixed();
    }

    pub fn push_value_n(&mut self, value: T, n: usize) {
        self.reserve(n);
        let start = self.tail;
        self.tail += n;
        unsafe { std::slice::from_raw_parts_mut(self.ptr_at(start).cast_mut(), n) }
            .fill(value);
        self.relayout_fixed();
    }

    pub fn push_default_n(&mut self, n: usize)
    where
        T: Default,
    {
        self.push_value_n(T::default(), n);
    }

    /// Make room for `extra` more items past `tail`
    #[inline]
    pub fn reserve(&mut self, extra: usize) {
        if (self.tail + extra) * Self::ITEM > self.region.cap {
            self.relocate(extra);
        }
    }

    pub fn reserve_blocks(&mut self, n: usize) {
        self.block_starts.reserve(n);
    }

    /// Take the first block, `None` once there are no more items
    pub fn take_block(&mut self) -> Option<MmapVec<T>> {
        if self.is_empty() {
            return None;
        }
        Some(self.take_first_block())
    }

    /// Take the first block even when it is empty, for callers that know from
    /// elsewhere that the block holds items
    pub fn take_first_block(&mut self) -> MmapVec<T> {
        let range = self.block_range(0);
        let block = self.hand_out(range.clone());
        self.head = range.end;
        if !FIXED_BLOCK_SIZING {
            self.block_starts.pop_front();
            if self.block_starts.is_empty() {
                self.block_starts.push_back(self.tail);
            }
        }
        self.after_head_moved();
        block
    }

    pub fn take_block_finished(&mut self) -> Option<ScalarBuffer<T>>
    where
        T: ArrowNativeType,
    {
        self.take_block().map(Into::into)
    }

    /// Take every non empty block
    pub fn take_all(&mut self) -> Vec<MmapVec<T>> {
        let blocks = (0..self.num_blocks())
            .map(|i| self.block_range(i))
            .filter(|r| !r.is_empty())
            .map(|r| self.hand_out(r))
            .collect();
        self.reset();
        blocks
    }

    /// Take the first `n` items. With manual sizing `adjusted_block_size_iter` gives
    /// the block sizes of the remaining items and must sum to their count
    pub fn take_n(
        &mut self,
        n: usize,
        adjusted_block_size_iter: Option<impl Iterator<Item = usize> + Clone>,
    ) -> MmapVec<T> {
        assert_eq!(FIXED_BLOCK_SIZING, adjusted_block_size_iter.is_none());
        assert!(n <= self.len(), "n ({n}) must be <= len ({})", self.len());

        let taken = self.hand_out(self.head..self.head + n);
        self.head += n;

        if let Some(sizes) = adjusted_block_size_iter {
            self.block_starts.clear();
            let mut start = self.head;
            for size in sizes {
                self.block_starts.push_back(start);
                start += size;
            }
            assert_eq!(
                start, self.tail,
                "adjusted block sizes must equal the length of the remaining items"
            );
            if self.block_starts.is_empty() {
                self.block_starts.push_back(self.head);
            }
        }
        self.after_head_moved();
        taken
    }

    pub fn reset(&mut self) {
        self.head = self.tail;
        self.block_starts.clear();
        self.block_starts.push_back(self.tail);
        self.after_head_moved();
    }

    // ---- internals ----

    #[inline]
    fn ptr_at(&self, item_index: usize) -> *const T {
        unsafe { self.region.base.add(item_index * Self::ITEM).cast::<T>() }
    }

    #[inline]
    fn current_block_start(&self) -> usize {
        if FIXED_BLOCK_SIZING {
            self.next_block_end - self.block_size
        } else {
            *self.block_starts.back().expect("always at least one block")
        }
    }

    /// Absolute item range of a block
    fn block_range(&self, block_index: usize) -> Range<usize> {
        if FIXED_BLOCK_SIZING {
            let start = self.head + block_index * self.block_size;
            start..(start + self.block_size).min(self.tail)
        } else {
            let start = self.block_starts[block_index];
            start
                ..self
                    .block_starts
                    .get(block_index + 1)
                    .copied()
                    .unwrap_or(self.tail)
        }
    }

    /// Fixed sizing: recompute where the current block ends after `head`/`tail` changed
    fn relayout_fixed(&mut self) {
        if FIXED_BLOCK_SIZING {
            self.next_block_end =
                self.head + (self.len() / self.block_size + 1) * self.block_size;
        }
    }

    fn after_head_moved(&mut self) {
        self.relayout_fixed();
        self.region.set_head(self.head * Self::ITEM);
    }

    /// Zero copy hand over of an absolute item range, its pages stay mapped while it lives
    fn hand_out(&self, range: Range<usize>) -> MmapVec<T> {
        let ptr = NonNull::new(self.ptr_at(range.start).cast_mut())
            .expect("mmap is never null");
        let guard =
            (!range.is_empty()).then(|| self.region.claim(range.start * Self::ITEM));
        MmapVec {
            ptr,
            len: range.len(),
            guard,
        }
    }

    /// Move the live window to the start of a fresh region, doubling it while the
    /// window would fill more than half
    fn relocate(&mut self, extra: usize) {
        let live = self.len();
        let need = (live + extra) * Self::ITEM;
        let mut cap = self.region.cap;
        while cap < need.saturating_mul(2) {
            cap *= 2;
        }
        let new = Region::map(cap, need);
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.ptr_at(self.head).cast::<u8>(),
                new.base,
                live * Self::ITEM,
            )
        };
        // Blocks handed out of the old region keep it alive for as long as they need it
        self.region.set_head(self.region.cap);
        self.region = new;
        for start in &mut self.block_starts {
            *start -= self.head;
        }
        self.next_block_end -= self.head;
        self.head = 0;
        self.tail = live;
    }
}

impl<T: Copy> CopyItemBlockedVecBuilder<true, T> {
    pub fn take_n_fixed(&mut self, n: usize) -> MmapVec<T> {
        self.take_n(n, None::<std::iter::Empty<usize>>)
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy> Extend<T>
    for CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        for value in iter {
            self.push(value);
        }
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy>
    CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    /// Offset of an index from `head`
    #[inline]
    fn offset(&self, index: BlocksIndex) -> usize {
        if FIXED_BLOCK_SIZING {
            index.into_index_in_fixed_block_size(self.block_size)
        } else {
            index.into_flat_index_in_dyn_block_size(&self.block_starts, self.head)
        }
    }

    /// Item at `index` without bounds checking
    ///
    /// # Safety
    /// `index` must point at an existing item, i.e. `self.offset(index) < self.len()`
    #[inline]
    pub unsafe fn get_unchecked(&self, index: BlocksIndex) -> &T {
        let offset = self.offset(index);
        debug_assert!(offset < self.len());
        unsafe { &*self.ptr_at(self.head + offset) }
    }

    /// Mutable item at `index` without bounds checking
    ///
    /// # Safety
    /// `index` must point at an existing item, i.e. `self.offset(index) < self.len()`
    #[inline]
    pub unsafe fn get_unchecked_mut(&mut self, index: BlocksIndex) -> &mut T {
        let offset = self.offset(index);
        debug_assert!(offset < self.len());
        unsafe { &mut *self.ptr_at(self.head + offset).cast_mut() }
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy> Index<BlocksIndex>
    for CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    type Output = T;

    #[inline]
    fn index(&self, index: BlocksIndex) -> &T {
        &self.as_slice()[self.offset(index)]
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T: Copy> IndexMut<BlocksIndex>
    for CopyItemBlockedVecBuilder<FIXED_BLOCK_SIZING, T>
{
    #[inline]
    fn index_mut(&mut self, index: BlocksIndex) -> &mut T {
        let offset = self.offset(index);
        &mut self.as_mut_slice()[offset]
    }
}

/// Still used by the builders that keep `Vec` blocks (e.g. the bytes buffer builder)
impl<T: Copy> BlockBuilder for Vec<T> {
    type Output = Vec<T>;

    fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn truncate(&mut self, len: usize) {
        Vec::truncate(self, len)
    }

    fn append_range(&mut self, src: &Self, range: Range<usize>) {
        self.extend_from_slice(&src[range])
    }

    fn shift_down(&mut self, offset: usize, len: usize) {
        if offset > 0 {
            self.copy_within(offset..offset + len, 0);
        }

        Vec::truncate(self, len)
    }

    fn allocated_size(&self) -> usize {
        size_of::<T>() * self.capacity()
    }

    fn finish(self) -> Vec<T> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups_accumulator::BlocksIndex;

    type Fixed = CopyItemBlockedVecBuilder<true, i32>;
    type Manual = CopyItemBlockedVecBuilder<false, i32>;

    fn to_vec(builder: &Fixed) -> Vec<i32> {
        (0..builder.len()).map(|i| builder[i]).collect()
    }

    /// A fixed builder always keeps a trailing block with room for the next push
    fn check_fixed_layout(builder: &Fixed) {
        let block_size = builder.block_size();
        assert_eq!(
            builder.num_blocks(),
            builder.len() / block_size + 1,
            "unexpected number of blocks for len {} and block size {block_size}",
            builder.len()
        );
        assert_eq!(builder.current_block_len(), builder.len() % block_size);
    }

    fn fixed_with(block_size: usize, values: &[i32]) -> Fixed {
        let mut builder = Fixed::new(block_size);
        builder.extend_from_slice(values);
        check_fixed_layout(&builder);
        builder
    }

    fn values(range: Range<usize>) -> Vec<i32> {
        range.map(|v| v as i32).collect()
    }

    #[test]
    fn new_is_empty() {
        let builder = Fixed::new(4);
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.block_size(), 4);
        assert_eq!(builder.current_block_len(), 0);
        check_fixed_layout(&builder);
    }

    #[test]
    #[should_panic(expected = "block size must be greater than 0")]
    fn fixed_zero_block_size_panics() {
        Fixed::new(0);
    }

    #[test]
    fn push_reports_when_block_fills() {
        let mut builder = Fixed::new(3);
        assert!(!builder.push(1));
        assert!(!builder.push(2));
        assert_eq!(builder.current_block_len(), 2);
        assert!(builder.push(3));
        assert_eq!(builder.len(), 3);
        assert_eq!(builder.num_blocks(), 2);
        assert_eq!(builder.current_block_len(), 0);
        assert!(!builder.push(4));
        assert_eq!(to_vec(&builder), [1, 2, 3, 4]);
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_with_block_size_one() {
        let mut builder = Fixed::new(1);
        for v in 0..5 {
            assert!(builder.push(v));
        }
        assert_eq!(builder.num_blocks(), 6);
        assert_eq!(to_vec(&builder), values(0..5));
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_value_n_spans_blocks() {
        let mut builder = Fixed::new(4);
        builder.push_value_n(7, 0);
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        builder.push_value_n(7, 10);
        assert_eq!(builder.len(), 10);
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(to_vec(&builder), vec![7; 10]);
        check_fixed_layout(&builder);

        // fill exactly to a block boundary
        builder.push_value_n(8, 2);
        assert_eq!(builder.len(), 12);
        assert_eq!(builder.num_blocks(), 4);
        assert_eq!(builder.current_block_len(), 0);
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_default_n() {
        let mut builder = Fixed::new(3);
        builder.push(5);
        builder.push_default_n(5);
        assert_eq!(to_vec(&builder), [5, 0, 0, 0, 0, 0]);
        check_fixed_layout(&builder);
    }

    #[test]
    fn extend_spans_blocks() {
        let mut builder = Fixed::new(4);
        builder.extend(values(0..0));
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        builder.extend(values(0..3));
        builder.extend(values(3..11));
        assert_eq!(to_vec(&builder), values(0..11));
        check_fixed_layout(&builder);

        // exactly to the boundary
        builder.extend(values(11..12));
        assert_eq!(builder.num_blocks(), 4);
        assert_eq!(builder.current_block_len(), 0);
        check_fixed_layout(&builder);
    }

    #[test]
    fn extend_from_slice_spans_blocks() {
        let mut builder = Fixed::new(4);
        builder.extend_from_slice(&[]);
        assert!(builder.is_empty());

        builder.extend_from_slice(&values(0..3));
        builder.extend_from_slice(&values(3..13));
        assert_eq!(to_vec(&builder), values(0..13));
        assert_eq!(builder.num_blocks(), 4);
        check_fixed_layout(&builder);
    }

    #[test]
    fn index_by_usize_and_blocks_index() {
        let mut builder = fixed_with(3, &values(0..8));

        for i in 0..8 {
            assert_eq!(builder[i], i as i32);
            let blocks_index = BlocksIndex::from_index_in_fixed_block_size(i, 3);
            assert_eq!(builder[blocks_index], i as i32);
        }
        assert_eq!(builder[BlocksIndex::new(1, 2)], 5);
        assert_eq!(builder[BlocksIndex::new(2, 1)], 7);

        builder[4] = 40;
        builder[BlocksIndex::new(2, 0)] = 60;
        assert_eq!(to_vec(&builder), [0, 1, 2, 3, 40, 5, 60, 7]);
    }

    #[test]
    fn take_block_returns_none_when_empty() {
        let mut builder = Fixed::new(4);
        assert!(builder.take_block().is_none());
        assert!(builder.take_block_finished().is_none());
        check_fixed_layout(&builder);

        // still usable after
        builder.push(1);
        assert_eq!(to_vec(&builder), [1]);
    }

    #[test]
    fn take_block_shifts_remaining() {
        let mut builder = fixed_with(3, &values(0..8));

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(0..3)));
        assert_eq!(builder.len(), 5);
        assert_eq!(to_vec(&builder), values(3..8));
        assert_eq!(builder[0], 3);
        assert_eq!(builder[BlocksIndex::new(1, 1)], 7);
        check_fixed_layout(&builder);

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..6)));
        assert_eq!(to_vec(&builder), values(6..8));
        check_fixed_layout(&builder);

        // partial last block
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(6..8)));
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn take_block_when_len_is_multiple_of_block_size() {
        let mut builder = fixed_with(3, &values(0..6));
        assert_eq!(builder.num_blocks(), 3);

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(0..3)));
        check_fixed_layout(&builder);
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..6)));
        check_fixed_layout(&builder);
        assert_eq!(builder.take_block(), None);
        check_fixed_layout(&builder);
    }

    #[test]
    fn push_after_take_block_continues_layout() {
        let mut builder = fixed_with(3, &values(0..7));
        builder.take_block();

        builder.extend_from_slice(&values(7..12));
        assert_eq!(to_vec(&builder), values(3..12));
        check_fixed_layout(&builder);

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..6)));
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(6..9)));
        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(values(9..12))
        );
        assert_eq!(builder.take_block(), None);
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_block_finished_returns_scalar_buffer() {
        let mut builder = fixed_with(2, &values(0..3));
        let finished = builder.take_block_finished().unwrap();
        assert_eq!(finished.as_ref(), &[0, 1]);
        let finished = builder.take_block_finished().unwrap();
        assert_eq!(finished.as_ref(), &[2]);
        assert!(builder.take_block_finished().is_none());
    }

    #[test]
    fn take_all_returns_only_non_empty_blocks() {
        let mut builder = fixed_with(3, &values(0..7));
        let blocks = builder.take_all();
        assert_eq!(blocks, vec![values(0..3), values(3..6), values(6..7)]);
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        // exact multiple, the trailing empty block is not returned
        let mut builder = fixed_with(3, &values(0..6));
        assert_eq!(builder.take_all(), vec![values(0..3), values(3..6)]);
        assert!(builder.is_empty());
        check_fixed_layout(&builder);

        let mut builder = Fixed::new(3);
        assert!(builder.take_all().is_empty());
        check_fixed_layout(&builder);
    }

    #[test]
    fn usable_after_take_all() {
        let mut builder = fixed_with(3, &values(0..7));
        builder.take_all();

        builder.extend_from_slice(&values(0..4));
        assert_eq!(to_vec(&builder), values(0..4));
        check_fixed_layout(&builder);
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(0..3)));
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..4)));
        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn reset_clears_everything() {
        let mut builder = fixed_with(3, &values(0..7));
        builder.reset();
        assert!(builder.is_empty());
        assert_eq!(builder.num_blocks(), 1);
        check_fixed_layout(&builder);
        assert!(builder.take_block().is_none());

        builder.push(1);
        assert_eq!(to_vec(&builder), [1]);
    }

    #[test]
    fn take_n_zero_is_noop() {
        let mut builder = fixed_with(3, &values(0..7));
        let taken = builder.take_n(0, None::<std::iter::Empty<usize>>);
        assert!(taken.is_empty());
        assert_eq!(to_vec(&builder), values(0..7));
        check_fixed_layout(&builder);

        let mut builder = Fixed::new(3);
        let taken = builder.take_n(0, None::<std::iter::Empty<usize>>);
        assert!(taken.is_empty());
        assert!(builder.is_empty());
        check_fixed_layout(&builder);
    }

    #[test]
    fn take_n_everything_when_less_than_block() {
        let mut builder = fixed_with(5, &values(0..3));
        let taken = builder.take_n(3, None::<std::iter::Empty<usize>>);
        assert_eq!(taken, values(0..3));
        assert!(builder.is_empty());
        check_fixed_layout(&builder);
        assert!(builder.take_block().is_none());
    }

    #[test]
    fn take_n_relayouts_blocks() {
        let mut builder = fixed_with(4, &values(0..10));
        let taken = builder.take_n(3, None::<std::iter::Empty<usize>>);
        assert_eq!(taken, values(0..3));
        assert_eq!(builder.len(), 7);
        assert_eq!(to_vec(&builder), values(3..10));
        check_fixed_layout(&builder);

        // blocks are now [3,4,5,6] [7,8,9]
        assert_eq!(builder[BlocksIndex::new(0, 3)], 6);
        assert_eq!(builder[BlocksIndex::new(1, 0)], 7);
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(3..7)));
        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(values(7..10))
        );
        assert_eq!(builder.take_block(), None);
    }

    #[test]
    fn take_n_full_block_equals_take_block() {
        let mut builder = fixed_with(4, &values(0..10));
        let taken = builder.take_n(4, None::<std::iter::Empty<usize>>);
        assert_eq!(taken, values(0..4));
        assert_eq!(to_vec(&builder), values(4..10));
        check_fixed_layout(&builder);
    }

    #[test]
    #[should_panic(expected = "must be <= len")]
    fn take_n_more_than_len_panics() {
        let mut builder = fixed_with(4, &values(0..2));
        builder.take_n(3, None::<std::iter::Empty<usize>>);
    }

    #[test]
    fn take_n_matches_model_and_stays_usable() {
        for block_size in 1..=6 {
            for total in 0..=(3 * block_size + 1) {
                for n in 0..=total.min(block_size) {
                    let mut builder = fixed_with(block_size, &values(0..total));
                    let mut model = values(0..total);

                    let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
                    let expected_taken: Vec<i32> = model.drain(..n).collect();
                    assert_eq!(
                        taken, expected_taken,
                        "taken mismatch bs={block_size} total={total} n={n}"
                    );
                    assert_eq!(
                        to_vec(&builder),
                        model,
                        "remaining mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder);

                    // keep using it after the re-layout
                    let more = values(1000..(1000 + 2 * block_size + 1));
                    builder.extend_from_slice(&more);
                    model.extend_from_slice(&more);
                    assert_eq!(
                        to_vec(&builder),
                        model,
                        "after push mismatch bs={block_size} total={total} n={n}"
                    );
                    check_fixed_layout(&builder);

                    // and drain block by block
                    let mut drained = vec![];
                    while let Some(block) = builder.take_block() {
                        assert!(block.len() <= block_size);
                        drained.extend_from_slice(&block);
                    }
                    assert_eq!(
                        drained, model,
                        "drain mismatch bs={block_size} total={total} n={n}"
                    );
                    assert!(builder.is_empty());
                    check_fixed_layout(&builder);
                }
            }
        }
    }

    #[test]
    fn repeated_take_n_and_push() {
        let block_size = 5;
        let mut builder = Fixed::new(block_size);
        let mut model: Vec<i32> = vec![];
        let mut next = 0;

        for step in 0..50 {
            let to_push = (step * 7) % 11;
            let pushed = values(next..next + to_push);
            next += to_push;
            builder.extend_from_slice(&pushed);
            model.extend_from_slice(&pushed);

            let n = ((step * 3) % block_size).min(model.len());
            let taken = builder.take_n(n, None::<std::iter::Empty<usize>>);
            let expected: Vec<i32> = model.drain(..n).collect();
            assert_eq!(taken, expected, "step {step}");
            assert_eq!(to_vec(&builder), model, "step {step}");
            check_fixed_layout(&builder);

            if step % 4 == 0 {
                let block = builder.take_block();
                let expected_len = model.len().min(block_size);
                let expected: Vec<i32> = model.drain(..expected_len).collect();
                assert_eq!(
                    block.map(|b| b.to_vec()).unwrap_or_default(),
                    expected,
                    "step {step}"
                );
                assert_eq!(to_vec(&builder), model, "step {step}");
                check_fixed_layout(&builder);
            }
        }
    }

    #[test]
    fn allocated_size_follows_blocks() {
        // memory is returned page by page, so make a block span whole pages
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let block_size = page / size_of::<i32>();
        let mut builder = Fixed::new(block_size);
        let empty = builder.allocated_size();

        builder.extend_from_slice(&values(0..2 * block_size + 1));
        let full = builder.allocated_size();
        assert!(full >= empty + (2 * block_size + 1) * size_of::<i32>());

        builder.take_block();
        let after_take = builder.allocated_size();
        assert_eq!(after_take, full - page);

        builder.take_all();
        assert!(builder.allocated_size() < after_take);
    }

    #[test]
    fn relocates_past_the_initial_mapping() {
        const RESERVED: usize = 1 << 20;
        let mut builder = Fixed::with_reservation(1024, RESERVED);
        let total = RESERVED / size_of::<i32>() + 5000;
        let mut expected = vec![];
        let mut next = 0;
        while builder.len() + expected.len() < total {
            // drain a block now and then so the live window stays small
            if next % 7 == 0 {
                let block = builder.take_block().map(|b| b.to_vec()).unwrap_or_default();
                assert_eq!(block, expected.drain(..block.len()).collect::<Vec<_>>());
            }
            let chunk = values(next..next + 3000);
            builder.extend_from_slice(&chunk);
            expected.extend_from_slice(&chunk);
            next += 3000;
            builder.push(-1);
            expected.push(-1);
            next += 1;
        }
        assert_eq!(to_vec(&builder), expected);
        check_fixed_layout(&builder);

        // grow while everything stays live
        let mut builder = Fixed::with_reservation(1024, RESERVED);
        let big = values(0..RESERVED / size_of::<i32>() + 3);
        builder.extend_from_slice(&big);
        assert_eq!(to_vec(&builder), big);
        assert_eq!(builder[BlocksIndex::new(3, 7)], big[3 * 1024 + 7]);
        check_fixed_layout(&builder);
    }

    // ---- manual block sizing ----

    /// Items of a manual builder that only has a single block
    fn manual_to_vec(builder: &Manual) -> Vec<i32> {
        assert_eq!(builder.num_blocks(), 1);
        (0..builder.len())
            .map(|i| builder[BlocksIndex::new(0, i)])
            .collect()
    }

    #[test]
    fn manual_push_never_finishes_block() {
        let mut builder = Manual::new(0);
        for v in 0..10 {
            assert!(!builder.push(v));
        }
        assert_eq!(builder.len(), 10);
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.current_block_len(), 10);
        assert_eq!(builder[BlocksIndex::new(0, 9)], 9);
    }

    #[test]
    fn manual_extend_goes_into_current_block() {
        let mut builder = Manual::new(0);
        builder.extend(values(0..5));
        builder.extend_from_slice(&values(5..8));
        builder.push_value_n(9, 2);
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.len(), 10);
        assert_eq!(manual_to_vec(&builder), [0, 1, 2, 3, 4, 5, 6, 7, 9, 9]);
    }

    #[test]
    fn manual_start_new_block_and_take_block() {
        let mut builder = Manual::new(0);
        builder.extend(values(0..5));
        builder.start_new_block();
        builder.extend(values(5..7));
        builder.start_new_block();
        builder.extend(values(7..11));
        assert_eq!(builder.num_blocks(), 3);
        assert_eq!(builder.len(), 11);
        assert_eq!(builder.current_block_len(), 4);
        assert_eq!(builder[BlocksIndex::new(1, 1)], 6);
        assert_eq!(builder[BlocksIndex::new(2, 3)], 10);

        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(0..5)));
        assert_eq!(builder.len(), 6);
        assert_eq!(builder[BlocksIndex::new(0, 1)], 6);
        assert_eq!(builder.take_block().map(|b| b.to_vec()), Some(values(5..7)));
        assert_eq!(
            builder.take_block().map(|b| b.to_vec()),
            Some(values(7..11))
        );
        assert!(builder.is_empty());
        assert_eq!(builder.take_block(), None);
        assert_eq!(builder.num_blocks(), 1);

        builder.push(1);
        assert_eq!(builder[BlocksIndex::new(0, 0)], 1);
    }

    #[test]
    fn manual_take_all() {
        let mut builder = Manual::new(0);
        builder.extend(values(0..5));
        builder.start_new_block();
        builder.extend(values(5..7));
        assert_eq!(builder.take_all(), vec![values(0..5), values(5..7)]);
        assert!(builder.is_empty());
        assert_eq!(builder.num_blocks(), 1);

        // trailing empty block from start_new_block is dropped
        let mut builder = Manual::new(0);
        builder.extend(values(0..5));
        builder.start_new_block();
        assert_eq!(builder.take_all(), vec![values(0..5)]);
        assert!(builder.is_empty());
    }

    fn manual_with_blocks(blocks: &[Vec<i32>]) -> Manual {
        let mut builder = Manual::new(0);
        for (i, block) in blocks.iter().enumerate() {
            if i > 0 {
                builder.start_new_block();
            }
            builder.extend_from_slice(block);
        }
        builder
    }

    fn drain_manual(builder: &mut Manual) -> Vec<Vec<i32>> {
        let mut out = vec![];
        while let Some(block) = builder.take_block() {
            out.push(block.to_vec());
        }
        out
    }

    #[test]
    fn manual_take_n_shrinks_first_block() {
        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(2, Some([3usize, 3, 4].into_iter()));
        assert_eq!(taken, values(0..2));
        assert_eq!(builder.len(), 10);
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(2..5), values(5..8), values(8..12)]
        );
    }

    #[test]
    fn manual_take_n_merges_blocks() {
        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(2, Some([10usize].into_iter()));
        assert_eq!(taken, values(0..2));
        assert_eq!(drain_manual(&mut builder), vec![values(2..12)]);

        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(2, Some([4usize, 6].into_iter()));
        assert_eq!(taken, values(0..2));
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(2..6), values(6..12)]
        );
    }

    #[test]
    fn manual_take_n_splits_blocks() {
        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(2, Some(std::iter::repeat_n(1usize, 10)));
        assert_eq!(taken, values(0..2));
        let expected: Vec<Vec<i32>> = (2..12).map(|v| vec![v]).collect();
        assert_eq!(drain_manual(&mut builder), expected);

        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(1, Some([2usize, 3, 1, 5].into_iter()));
        assert_eq!(taken, values(0..1));
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(1..3), values(3..6), values(6..7), values(7..12)]
        );
    }

    #[test]
    fn manual_take_n_whole_first_block() {
        let mut builder =
            manual_with_blocks(&[values(0..5), values(5..8), values(8..12)]);
        let taken = builder.take_n(5, Some([3usize, 4].into_iter()));
        assert_eq!(taken, values(0..5));
        assert_eq!(builder.len(), 7);
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(5..8), values(8..12)]
        );
    }

    #[test]
    fn manual_take_n_zero_keeps_layout() {
        let mut builder = manual_with_blocks(&[values(0..5), values(5..8)]);
        let taken = builder.take_n(0, Some([5usize, 3].into_iter()));
        assert!(taken.is_empty());
        assert_eq!(builder.len(), 8);
        assert_eq!(drain_manual(&mut builder), vec![values(0..5), values(5..8)]);
    }

    #[test]
    fn manual_take_n_everything() {
        let mut builder = manual_with_blocks(&[values(0..5)]);
        let taken = builder.take_n(5, Some(std::iter::empty::<usize>()));
        assert_eq!(taken, values(0..5));
        assert!(builder.is_empty());
        assert_eq!(builder.num_blocks(), 1);
        assert_eq!(builder.take_block(), None);

        builder.push(1);
        assert_eq!(builder[BlocksIndex::new(0, 0)], 1);
    }

    #[test]
    fn manual_take_n_then_push_continues_in_last_block() {
        let mut builder = manual_with_blocks(&[values(0..5), values(5..8)]);
        builder.take_n(2, Some([3usize, 3].into_iter()));
        builder.push(100);
        assert_eq!(builder.len(), 7);
        assert_eq!(builder.current_block_len(), 4);
        assert_eq!(
            drain_manual(&mut builder),
            vec![values(2..5), vec![5, 6, 7, 100]]
        );
    }

    #[test]
    #[should_panic(expected = "must equal the length")]
    fn manual_take_n_wrong_adjusted_sizes_panics() {
        let mut builder = manual_with_blocks(&[values(0..5), values(5..8)]);
        builder.take_n(2, Some([3usize, 2].into_iter()));
    }
}
