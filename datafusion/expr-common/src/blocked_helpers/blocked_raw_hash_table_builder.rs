use super::blocked_custom_input_builder::{
    Block, BlockProvider, BlockProviderFinish, BlockWithSlice, BlockedCustomInputBuilder,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeType;
use datafusion_common::utils::proxy::VecAllocExt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct BlockedRawHashTableBuilder<const FIXED_BLOCK_SIZING: bool, T>(
    BlockedCustomInputBuilder<FIXED_BLOCK_SIZING, RawHashTableBlockProvider<T>>,
);

impl<const FIXED_BLOCK_SIZING: bool, T>
    BlockedRawHashTableBuilder<FIXED_BLOCK_SIZING, T>
{
    pub fn new(block_size: usize) -> Self {
        BlockedRawHashTableBuilder(BlockedCustomInputBuilder::new(
            block_size,
            RawHashTableBlockProvider::<T>::default(),
        ))
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T> Deref
    for BlockedRawHashTableBuilder<FIXED_BLOCK_SIZING, T>
{
    type Target =
        BlockedCustomInputBuilder<FIXED_BLOCK_SIZING, RawHashTableBlockProvider<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const FIXED_BLOCK_SIZING: bool, T> DerefMut
    for BlockedRawHashTableBuilder<FIXED_BLOCK_SIZING, T>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct RawHashTableBlockProvider<T>(PhantomData<T>);

impl<T> Default for RawHashTableBlockProvider<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> BlockProvider for RawHashTableBlockProvider<T> {
    type Block = hashbrown::HashTable<T>;

    fn new_block(&self) -> Self::Block {
        hashbrown::HashTable::new()
    }
}

impl<T> Block for hashbrown::HashTable<T> {
    type Item = T;

    fn allocated_size(&self) -> usize {
        self.allocation_size()
    }
    //
    // fn push(&mut self, item: Self::Item) {
    //     Self::push(self, item)
    // }
    //
    // fn extend(&mut self, iter: impl Iterator<Item = Self::Item>) {
    //     Extend::extend(self, iter)
    // }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}
//
// impl<T: Clone> BlockWithSlice for Vec<T> {
//     fn extend_from_slice(&mut self, slice: &[Self::Item]) {
//         Vec::extend_from_slice(self, slice)
//     }
//
//     fn append_n(&mut self, item: Self::Item, n: usize) {
//         self.resize(self.len() + n, item)
//     }
// }
