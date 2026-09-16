/// Test helper: `take_all` returns exactly `num_blocks` blocks, and `take_block` yields
/// `num_blocks` times before it yields `None`; returns the blocks of `take_all`
#[cfg(test)]
macro_rules! assert_blocks_consistent {
    ($make:expr) => {{
        let make = $make;
        let mut builder = make();
        let num_blocks = builder.num_blocks();
        let blocks = builder.take_all();
        assert_eq!(
            blocks.len(),
            num_blocks,
            "take_all returned {} blocks but num_blocks was {num_blocks}",
            blocks.len()
        );
        assert_eq!(builder.num_blocks(), 0, "nothing counts after take_all");
        assert!(builder.take_all().is_empty());
        assert!(builder.take_block().is_none());

        let mut builder = make();
        for taken in 0..num_blocks {
            assert!(
                builder.take_block().is_some(),
                "take_block {taken} of {num_blocks} must yield a block"
            );
            assert_eq!(builder.num_blocks(), num_blocks - taken - 1);
        }
        assert!(
            builder.take_block().is_none(),
            "take_block must yield None after {num_blocks} blocks"
        );
        assert_eq!(builder.num_blocks(), 0);
        blocks
    }};
}

mod blocked_offset_buffer_builder;
pub use blocked_offset_buffer_builder::BlockedOffsetBufferBuilder;
mod blocked_bytes_buffer_builder;
pub use blocked_bytes_buffer_builder::BlockedBytesBufferBuilder;
mod blocked_nulls_builder;
pub use blocked_nulls_builder::BlockedNullsBuilder;
mod blocked_byte_array_builder;
pub use blocked_byte_array_builder::BlockedByteArrayBuilder;
mod blocked_boolean_builder;
pub use blocked_boolean_builder::BlockedBooleanBuilder;
mod blocked_vec_builder;
pub use blocked_vec_builder::{CopyItemBlockedVecBuilder, MmapVec};
mod blocked_custom_input_builder;
pub use blocked_custom_input_builder::{
    Block, BlockProvider, BlockProviderFinish, BlockWithSlice, BlockedCustomInputBuilder,
};
mod blocked_rows_builder;
pub use blocked_rows_builder::BlockedRowsBuilder;
mod blocked_custom_input_builder_with_lifetime;
pub use blocked_custom_input_builder_with_lifetime::{
    BlockProviderWithLifetimeFinish, BlockWithLifetime, BlockWithLifetimeProvider,
    BlockWithLifetimeWithSlice, BlockedCustomInputBuilderWithLifetime,
};
// Still a stub, needs a `hashbrown` dependency and the `Block` push/extend methods
// mod blocked_raw_hash_table_builder;
mod blocked_custom_heap_allocated_input_builder;
mod blocked_heap_items_vec_builder;
pub mod get_heap_allocated_size;
pub mod take_n_helpers;
pub mod take_n_helpers_heap_allocated;

pub use blocked_heap_items_vec_builder::BlockedVecBuilder;
pub use get_heap_allocated_size::{GetHeapAllocatedSize, OnlyOnStackSize};
