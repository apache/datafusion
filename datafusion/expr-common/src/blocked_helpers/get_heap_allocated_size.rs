use std::marker::PhantomData;
use datafusion_common::utils::proxy::VecAllocExt;

/// Get size of value T
pub trait GetHeapAllocatedSize<T> {
  /// Whether the value is on the stack or heap, if on the stack the allocated size function will not be called
  const HAS_HEAP_ALLOCATION: bool;

  /// Get the size of the value, this should not return size for the stack allocated values as it is already
  /// accounted for in [`CollectSetGroupAccumulatorValues::size`] function
  fn get_heap_allocated_size(value: &T) -> usize;
}

#[derive(Debug)]
pub struct OnlyOnStackSize;

impl<T> GetHeapAllocatedSize<T> for OnlyOnStackSize {
  const HAS_HEAP_ALLOCATION: bool = false;

  fn get_heap_allocated_size(_value: &T) -> usize {
    unreachable!("This should not be called if the value is on the stack")
  }
}

#[derive(Debug)]
pub struct CommonHeapAllocatorSize;
impl<T: Copy> GetHeapAllocatedSize<Option<Vec<T>>> for CommonHeapAllocatorSize {
  const HAS_HEAP_ALLOCATION: bool = true;

  fn get_heap_allocated_size(value: &Option<Vec<T>>) -> usize {
    value.as_ref().map_or(0, |v| v.allocated_size())
  }
}

impl<T: Copy> GetHeapAllocatedSize<Vec<T>> for CommonHeapAllocatorSize {
  const HAS_HEAP_ALLOCATION: bool = true;

  fn get_heap_allocated_size(value: &Vec<T>) -> usize {
    value.allocated_size()
  }
}
