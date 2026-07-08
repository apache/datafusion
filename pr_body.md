## Which issue does this PR close?

- N/A (design discussed offline; can file a tracking issue if preferred)

## Rationale for this change

Operators bound their output batches by row count (`datafusion.execution.batch_size`) only. With wide rows (large string values, wide schemas) a batch within the row limit can still be arbitrarily large in bytes — multi-GB batches break memory accounting assumptions and cause OOMs. Conversely, streams of many tiny batches waste per-batch overhead.

The existing `BatchSplitStream` on `DataSourceExec` splits by rows only, and splitting by zero-copy `slice` cannot release memory anyway: a slice of a 4GB batch still pins all 4GB of buffers (arrow's `concat` also short-circuits single inputs to a zero-copy slice).

This PR adds a **batch normalizer**: a stream wrapper that re-chunks data source output towards a target row count *and* byte size, opt-in via a new config option.

## What changes are included in this PR?

1. `BatchNormalizer` / `BatchNormalizerStream` (`datafusion/physical-plan/src/batch_normalizer.rs`). Per input batch, one of four actions:
   - **Pass through** (zero copy): rows ≥ `batch_size / 2` or logical bytes ≥ `target / 2`, up to `2x` the byte target. The wide acceptance band means near-target batches are never copied.
   - **Coalesce**: small batches are buffered (via arrow `BatchCoalescer`) and flushed when the buffer reaches the row target **or** the byte target, whichever first.
   - **Split**: oversized batches (by rows or bytes) are re-emitted as compact ~target-sized copies (`take_record_batch` + view-array GC), produced incrementally (one chunk per poll) so peak memory is the parent plus ~one chunk. Copying is load-bearing: zero-copy slices would keep the oversized parent alive.
   - **Compact**: batches that retain far more memory than they logically use (> 2x and > 1MiB waste, e.g. a small slice pinning a huge parent, or sparse StringView data buffers) are copied so the backing buffers can be freed.
2. New config `datafusion.execution.target_batch_size_bytes` (default `None` = unchanged behavior). When set, `DataSourceExec` wraps its output in `BatchNormalizerStream` instead of the row-only `BatchSplitStream`.
3. Operator metrics: `batches_passed_through`, `batches_coalesced`, `batches_split`, `batches_compacted`.

Planned follow-ups (intentionally not in this PR): memory-pool reservation for the normalizer's buffer with a spill fallback (following the `RepartitionExec` `OutputChannel`/`SpillPool` pattern), and byte-aware bounding for operator emit paths (join/aggregate output amplification is not addressable at the scan).

## Local benchmark observations (laptop, noisy — GKE runs requested)

Interleaved off/on A/B (2 rounds, per-query minimums, same binary, 16MiB target via `DATAFUSION_EXECUTION_TARGET_BATCH_SIZE_BYTES`):

- TPC-H SF1: total off 623ms vs on 678ms. Operator metrics confirm the normalizer is pure zero-copy pass-through on these scans (e.g. Q1/Q6: 742/742 `batches_passed_through`, 0 copies), so the residual delta is per-batch size measurement overhead and/or laptop noise (off-vs-off noise floor spans 0.66-1.22x per query).
- ClickBench partitioned: total off 21.9s vs on 22.4s (+2.6%; noise floor p90 1.13x). String-heavy group-by queries (Q13/Q17/Q21) show 1.19-1.25x, slightly above the noise band — likely the StringView waste-compaction path; being investigated with operator metrics. Improvements to 0.81x on Q32/Q33.

Laptop numbers are close to the noise floor; treating the dedicated benchmark runner as the source of truth.

## Are these changes tested?

Yes — written test-first (TDD):

- 18 unit tests covering pass-through zero-copy guarantees (pointer equality), coalesce flush on rows/bytes, incremental split with compaction guarantees (chunks must not retain the parent's buffers, including the StringView GC case), waste compaction and its thresholds, ordering across buffered/pass-through boundaries, single-giant-row and zero-column edge cases, metrics, and the stream adapter.
- 3 end-to-end tests exercising the config through `SessionContext` scans.
- Extended workspace test suite passes; config docs and `information_schema.slt` regenerated.

## Are there any user-facing changes?

New opt-in config option `datafusion.execution.target_batch_size_bytes` (documented in `configs.md`). Default behavior is unchanged. No breaking API changes.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01UcPTREZVLXsSZDRCae33gm
