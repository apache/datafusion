// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// The local red zone used by SQL recursive entry points.
///
/// Some SQL planner and unparser recursion paths need more than `recursive`'s
/// default 128 KiB red zone in debug builds. Keep this value local to each
/// stack-growth checkpoint rather than mutating `recursive`'s process-global
/// minimum stack size.
#[cfg(feature = "recursive_protection")]
pub(crate) const SQL_RECURSION_RED_ZONE: usize = 256 * 1024;

/// Runs `callback` on a stack with enough space for SQL recursive entry points.
#[cfg(feature = "recursive_protection")]
#[inline]
pub(crate) fn maybe_grow<R>(callback: impl FnOnce() -> R) -> R {
    stacker::maybe_grow(
        SQL_RECURSION_RED_ZONE,
        recursive::get_stack_allocation_size(),
        callback,
    )
}

/// Runs `callback` without stack growth when recursive protection is disabled.
#[cfg(not(feature = "recursive_protection"))]
#[inline]
pub(crate) fn maybe_grow<R>(callback: impl FnOnce() -> R) -> R {
    callback()
}

#[cfg(all(test, feature = "recursive_protection"))]
mod tests {
    use super::*;

    #[test]
    fn maybe_grow_does_not_mutate_recursive_minimum_stack_size() {
        let before = recursive::get_minimum_stack_size();
        let observed = maybe_grow(recursive::get_minimum_stack_size);

        assert_eq!(observed, before);
        assert_eq!(recursive::get_minimum_stack_size(), before);
    }
}
