// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Completion protocol shared by the probe partitions of a join.
//!
//! The probe partitions of one `HashJoinExec` or `NestedLoopJoinExec` run
//! concurrently over a shared build side. Some facts have to cross partition
//! boundaries:
//!
//! - which partition emits the build-side rows in the final stage, namely the
//!   last one to finish,
//! - whether any partition stopped before it finished, in which case its
//!   matches are missing and nobody may emit, and
//! - what the partitions collectively saw, which the null-aware `NOT IN` rules
//!   of the hash join need: did any partition see a row, and did any see a
//!   NULL join key.
//!
//! [`ProbeCompletion`] owns all of them, because reading the facts without
//! ordering them against the choice of the last partition produced wrong
//! results. A partition that read the NULL
//! flag *before* decrementing the counter could observe `false`, have a
//! sibling record a NULL and finish, then become the last partition itself and
//! emit build rows that `NOT IN` must suppress.
//!
//! The invariant is therefore: **whichever caller observes itself to be last
//! sees every fact recorded by every other partition.** It rests on two
//! properties, and the type is shaped to keep both.
//!
//! 1. *Order.* The facts are reachable only through the value returned by
//!    [`ProbeCompletion::report_completed`], so no caller can read them before
//!    its own decrement.
//! 2. *Visibility.* That decrement is `AcqRel`, so the last partition
//!    synchronizes with the release of every partition that finished before
//!    it. Were the counter `Relaxed`, nothing would order a sibling's store
//!    before the final load even when the decrement happened first.
//!
//! Neither property can be pinned down by an ordinary concurrent test: the
//! window is a few instructions wide, and on x86 a `Relaxed` decrement lowers
//! to the same instruction as an `AcqRel` one. Both are model-checked in
//! `loom_tests` instead.

use std::sync::atomic::Ordering;

/// What every probe partition together saw, as observed by the last one to
/// finish.
///
/// Obtainable only from [`ProbeCompletion::report_completed`], which is what
/// stops the final stage from reading the shared state too early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProbeSideSummary {
    /// Some probe partition saw a row. An entirely empty probe side is not the
    /// same as one that matched nothing: `NULL NOT IN (empty)` is TRUE.
    pub(super) non_empty: bool,
    /// Some probe partition saw a NULL join key.
    pub(super) has_null: bool,
}

/// Describes the state of a probe partition after it has finished its work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Completion {
    /// Not the last partition to finish.
    NotLast,
    /// Last partition to finish and emit a summary.
    Emit(ProbeSideSummary),
    /// The last partition with some previous partition abandoned.
    Abandoned,
}

/// Defines [`ProbeCompletion`] over the given atomic types.
///
/// [loom] can only explore its own atomics, so the protocol is written once
/// here and instantiated twice: over [`std::sync::atomic`] for the join, and
/// over `loom::sync::atomic` for the model in `loom_tests`. Both share this
/// single copy of the orderings and the call sequence, which is what lets the
/// model speak for the real thing. Keep it that way: a second copy of these
/// orderings would not be covered.
///
/// [loom]: https://docs.rs/loom
macro_rules! define_probe_completion {
    (counter: $counter:ty, flag: $flag:ty) => {
        /// Counts the probe partitions that are still running.
        type Counter = $counter;
        /// One fact, set by any partition and read by the last one.
        type Flag = $flag;

        /// Tracks how many probe partitions are still running, together with
        /// the facts they contribute.
        ///
        /// See the [module docs](self) for the invariant this upholds.
        #[derive(Debug)]
        pub(super) struct ProbeCompletion {
            /// Probe partitions that have not finished yet.
            running: Counter,
            /// Set once any probe partition has seen a row.
            saw_row: Flag,
            /// Set once any probe partition has seen a NULL join key.
            saw_null_key: Flag,
            /// Set once any probe partition has stopped before it finished.
            abandoned: Flag,
        }

        impl ProbeCompletion {
            /// Creates the protocol state for `probe_threads` partitions.
            pub(super) fn new(probe_threads: usize) -> Self {
                Self {
                    running: Counter::new(probe_threads),
                    saw_row: Flag::new(false),
                    saw_null_key: Flag::new(false),
                    abandoned: Flag::new(false),
                }
            }

            /// Records what one probe partition saw in one batch.
            ///
            /// `Relaxed` suffices: the `AcqRel` decrement in
            /// [`Self::report_completed`] publishes these stores to the last
            /// partition.
            pub(super) fn record_batch(&self, non_empty: bool, has_null: bool) {
                if non_empty {
                    self.saw_row.store(true, Ordering::Relaxed);
                }
                if has_null {
                    self.saw_null_key.store(true, Ordering::Relaxed);
                }
            }

            /// Whether some partition has already recorded a NULL join key.
            ///
            /// A hint for skipping work during the probe phase, which may lag
            /// behind sibling partitions. It must never decide what the final
            /// stage emits; use the [`ProbeSideSummary`] from
            /// [`Self::report_completed`] for that.
            pub(super) fn saw_null_key_hint(&self) -> bool {
                self.saw_null_key.load(Ordering::Relaxed)
            }

            /// Marks the calling partition finished. Only the last one gets
            /// more than [`Completion::NotLast`]: [`Completion::Emit`] with
            /// what all partitions saw, or [`Completion::Abandoned`] if some
            /// partition stopped early (see [`Self::abandon`]).
            ///
            /// The `AcqRel` decrement publishes this partition's own stores
            /// and acquires those of the partitions that finished earlier, so
            /// the summary handed to the last caller is complete.
            pub(super) fn report_completed(&self) -> Completion {
                // `fetch_sub` returns the value from *before* the decrement,
                // so `1` means this call is the one that took it to zero.
                let was_last = self.running.fetch_sub(1, Ordering::AcqRel) == 1;
                match (was_last, self.abandoned.load(Ordering::Relaxed)) {
                    (false, _) => Completion::NotLast,
                    (true, false) => {
                        let summary = ProbeSideSummary {
                            non_empty: self.saw_row.load(Ordering::Relaxed),
                            has_null: self.saw_null_key.load(Ordering::Relaxed),
                        };
                        Completion::Emit(summary)
                    }
                    (true, true) => Completion::Abandoned,
                }
            }

            /// Marks `count` probe partitions as having stopped before they finished.
            ///
            /// They still count down, so the partitions that remain are not left
            /// waiting. But their matches are missing, so the build-side rows must not
            /// be emitted: the last partition is not handed a summary.
            ///
            /// `Relaxed` suffices for the flag, as in [`Self::record_batch`]: the
            /// `AcqRel` decrement publishes it.
            pub(super) fn abandon(&self, count: usize) -> bool {
                if count == 0 {
                    return false;
                }
                self.abandoned.store(true, Ordering::Relaxed);
                self.running.fetch_sub(count, Ordering::AcqRel) == count
            }
        }
    };
}

define_probe_completion!(
    counter: std::sync::atomic::AtomicUsize,
    flag: std::sync::atomic::AtomicBool
);

#[cfg(test)]
mod tests {
    use super::*;

    /// The sequential contract: only the last caller is handed the summary,
    /// and it carries what the earlier partitions recorded.
    ///
    /// The concurrent invariant is covered by `loom_tests` instead.
    #[test]
    fn only_the_last_partition_receives_the_summary() {
        let completion = ProbeCompletion::new(3);

        completion.record_batch(true, false);
        assert_eq!(completion.report_completed(), Completion::NotLast);

        completion.record_batch(true, true);
        assert_eq!(completion.report_completed(), Completion::NotLast);

        assert_eq!(
            completion.report_completed(),
            Completion::Emit(ProbeSideSummary {
                non_empty: true,
                has_null: true,
            })
        );
    }

    /// A probe side that never saw a row leaves both facts clear, which is how
    /// `NULL NOT IN (empty)` stays TRUE.
    #[test]
    fn an_untouched_probe_side_reports_nothing_seen() {
        let completion = ProbeCompletion::new(1);

        assert_eq!(
            completion.report_completed(),
            Completion::Emit(ProbeSideSummary {
                non_empty: false,
                has_null: false,
            })
        );
    }

    /// The hint may lag, but it must never report a NULL that was not
    /// recorded.
    #[test]
    fn the_hint_reflects_recorded_nulls() {
        let completion = ProbeCompletion::new(1);
        assert!(!completion.saw_null_key_hint());

        completion.record_batch(true, false);
        assert!(!completion.saw_null_key_hint());

        completion.record_batch(true, true);
        assert!(completion.saw_null_key_hint());
    }

    /// Abandoning no partitions changes nothing. A caller may report a count of
    /// zero, for example when no partition went away before the left side
    /// spilled, and that must not suppress the build-side rows.
    #[test]
    fn abandoning_no_partitions_still_emits() {
        let completion = ProbeCompletion::new(1);
        assert!(!completion.abandon(0));
        assert_eq!(
            completion.report_completed(),
            Completion::Emit(ProbeSideSummary {
                non_empty: false,
                has_null: false,
            })
        );
    }
}

/// Model-checked concurrency tests for the protocol.
///
/// These instantiate the protocol over loom's atomics (see the
/// [module docs](self)) and let loom enumerate the interleavings and store
/// visibility the memory model allows. They fail if the decrement in
/// `report_completed` or `abandon` is weakened to `Relaxed`.
///
/// Compiled only under `--cfg datafusion_loom`, which is also what pulls the `loom`
/// dev-dependency into the build (see `datafusion-physical-plan`'s
/// `Cargo.toml` for why it is kept out of the ordinary one). Run them with:
///
/// ```text
/// RUSTFLAGS="--cfg datafusion_loom" cargo test -p datafusion-physical-plan --lib loom_tests
/// ```
#[cfg(all(test, datafusion_loom))]
mod loom_tests {
    use super::Completion;
    use super::ProbeSideSummary;
    use loom::sync::Arc;
    use std::sync::atomic::Ordering;

    define_probe_completion!(
        counter: loom::sync::atomic::AtomicUsize,
        flag: loom::sync::atomic::AtomicBool
    );

    /// The invariant behind the fix: whichever partition observes itself to be
    /// last sees the NULL its sibling recorded.
    ///
    /// This is the regression test for the wrong-results bug. One partition
    /// records a NULL and finishes while the other finishes having recorded
    /// nothing, and the last one must not conclude that no NULL was seen.
    #[test]
    fn the_last_partition_observes_a_sibling_null() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let recorder = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || {
                    completion.record_batch(true, true);
                    completion.report_completed()
                })
            };

            let observer = completion.report_completed();
            let recorded = recorder.join().unwrap();

            let summary = match (recorded, observer) {
                (Completion::Emit(summary), Completion::NotLast)
                | (Completion::NotLast, Completion::Emit(summary)) => summary,
                (Completion::Emit(_), Completion::Emit(_)) => {
                    panic!("two partitions both finished last")
                }
                (Completion::NotLast, Completion::NotLast) => {
                    panic!("no partition finished last")
                }
                other => panic!("unexpected outcome: {other:?}"),
            };

            assert!(
                summary.has_null,
                "the last partition missed the NULL its sibling recorded"
            );
            assert!(
                summary.non_empty,
                "the last partition missed the row its sibling recorded"
            );
        });
    }

    /// With both partitions recording, the summary must be the union of what
    /// they saw, whichever finishes last.
    #[test]
    fn the_summary_unions_what_every_partition_recorded() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let other = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || {
                    // Rows, but no NULL key.
                    completion.record_batch(true, false);
                    completion.report_completed()
                })
            };

            // A NULL key in an otherwise empty batch.
            completion.record_batch(false, true);
            let here = completion.report_completed();
            let there = other.join().unwrap();

            let summary = match (here, there) {
                (Completion::Emit(summary), Completion::NotLast)
                | (Completion::NotLast, Completion::Emit(summary)) => summary,
                other => panic!("some partition must finish last: {other:?}"),
            };
            assert_eq!(
                summary,
                ProbeSideSummary {
                    non_empty: true,
                    has_null: true,
                },
                "the summary lost a fact one of the partitions recorded"
            );
        });
    }

    /// Exactly one partition may take the final stage. Two would emit the
    /// build side twice; none would drop it.
    #[test]
    fn exactly_one_partition_finishes_last() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let other = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || {
                    completion.report_completed() != Completion::NotLast
                })
            };

            let here = completion.report_completed() != Completion::NotLast;
            let there = other.join().unwrap();

            assert!(here ^ there, "exactly one partition must finish last");
        });
    }

    /// The probe-phase hint never invents a NULL: if no partition recorded
    /// one, no observation of the hint may report one.
    #[test]
    fn the_hint_never_reports_an_unrecorded_null() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let other = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || {
                    completion.record_batch(true, false);
                    completion.saw_null_key_hint()
                })
            };

            let here = completion.saw_null_key_hint();
            let there = other.join().unwrap();

            assert!(!here && !there, "the hint reported an unrecorded NULL");
        });
    }

    /// A partition that stops early still counts down, so exactly one
    /// partition finishes last. But its matches are missing, so no partition
    /// may emit, whichever of the two finishes last.
    #[test]
    fn an_abandoned_partition_counts_down_but_suppresses_the_emit() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let quitter = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || completion.abandon(1))
            };

            let here = completion.report_completed();
            let quitter_was_last = quitter.join().unwrap();

            match (here, quitter_was_last) {
                // This partition finished last and saw the abandon
                (Completion::Abandoned, false) => {}
                // The quitter finished last and releases the shared state
                (Completion::NotLast, true) => {}
                other => panic!("unexpected outcome: {other:?}"),
            }
        });
    }
}
