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

//! Adapter for integrating DataFusion's [`MemoryPool`] with Arrow's memory tracking APIs.

use crate::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use std::fmt::Debug;
use std::sync::Arc;

/// An adapter that implements Arrow's [`arrow_buffer::MemoryPool`] trait
/// by wrapping a DataFusion [`MemoryPool`].
///
/// This allows DataFusion's memory management system to be used with Arrow's
/// memory allocation APIs. Each reservation made through this pool will be
/// tracked using the provided [`MemoryConsumer`], enabling DataFusion to
/// monitor and limit memory usage across Arrow operations.
///
/// This is useful when you want Arrow operations (such as array builders
/// or compute kernels) to participate in DataFusion's memory management
/// and respect the same memory limits as DataFusion operators.
#[derive(Debug)]
pub struct ArrowMemoryPool {
    inner: Arc<dyn MemoryPool>,
    consumer: MemoryConsumer,
}

impl ArrowMemoryPool {
    /// Creates a new [`ArrowMemoryPool`] that wraps the given DataFusion [`MemoryPool`]
    /// and tracks allocations under the specified [`MemoryConsumer`].
    pub fn new(inner: Arc<dyn MemoryPool>, consumer: MemoryConsumer) -> Self {
        Self { inner, consumer }
    }
}

impl arrow_buffer::MemoryReservation for MemoryReservation {
    fn size(&self) -> usize {
        MemoryReservation::size(self)
    }

    fn resize(&mut self, new_size: usize) {
        MemoryReservation::resize(self, new_size)
    }
}

impl arrow_buffer::MemoryPool for ArrowMemoryPool {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        let consumer = self.consumer.clone_with_new_id();
        let mut reservation = consumer.register(&self.inner);
        reservation.grow(size);

        Box::new(reservation)
    }

    fn available(&self) -> isize {
        // The pool may be overfilled, so this method might return a negative value.
        (self.capacity() as i128 - self.used() as i128)
            .try_into()
            .unwrap_or(isize::MIN)
    }

    fn used(&self) -> usize {
        self.inner.reserved()
    }

    fn capacity(&self) -> usize {
        match self.inner.memory_limit() {
            MemoryLimit::Infinite | MemoryLimit::Unknown => usize::MAX,
            MemoryLimit::Finite(capacity) => capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
    use arrow::array::{Array, Int32Array};
    use arrow_buffer::MemoryPool;

    // Until https://github.com/apache/arrow-rs/pull/8918 lands, we need to iterate all
    // buffers in the array. Change once the PR is released.
    fn claim_array(array: &dyn Array, pool: &dyn MemoryPool) {
        for buffer in array.to_data().buffers() {
            buffer.claim(pool);
        }
    }

    #[test]
    pub fn can_claim_array() {
        let pool = Arc::new(UnboundedMemoryPool::default());

        let consumer = MemoryConsumer::new("arrow");
        let arrow_pool = ArrowMemoryPool::new(pool, consumer);

        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        claim_array(&array, &arrow_pool);

        assert_eq!(arrow_pool.used(), array.get_buffer_memory_size());

        let slice = array.slice(0, 2);

        // This should be a no-op
        claim_array(&slice, &arrow_pool);

        assert_eq!(arrow_pool.used(), array.get_buffer_memory_size());
    }

    #[test]
    pub fn can_claim_array_with_finite_limit() {
        let pool_capacity = 1024;
        let pool = Arc::new(GreedyMemoryPool::new(pool_capacity));

        let consumer = MemoryConsumer::new("arrow");
        let arrow_pool = ArrowMemoryPool::new(pool, consumer);

        assert_eq!(arrow_pool.capacity(), pool_capacity);
        assert_eq!(arrow_pool.available(), pool_capacity as isize);

        let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        claim_array(&array, &arrow_pool);

        assert_eq!(arrow_pool.used(), array.get_buffer_memory_size());
        assert_eq!(
            arrow_pool.available(),
            (pool_capacity - array.get_buffer_memory_size()) as isize
        );
    }
}
