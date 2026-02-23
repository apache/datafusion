# CI Investigation: `Run sqllogictests with the sqlite test suite`

## Question
Why did the CI task
`Datafusion extended tests / Run sqllogictests with the sqlite test suite (pull_request)`
increase from about 1 hour to about 2 hours after merge commit `76be0b64c`?

## Scope
Compared:
- pre-merge parent: `8f959bba6`
- merge result: `76be0b64c`

## Findings

### 1) CI workflow/job definition did not change in the merge
I diffed `.github/workflows/extended.yml` between `8f959bba6` and `76be0b64c` and found no changes.

Implication: the slowdown is not explained by a direct change to the job steps, image, or command in this merge.

### 2) The sqllogictest workload increased materially
`datafusion/sqllogictest/test_files` changes in this merge range:
- `34 files changed`
- `+2359 / -296` lines (net `+2063`)
- new files include:
  - `join_limit_pushdown.slt`
  - `spark/bitmap/bitmap_bit_position.slt`
  - `spark/bitmap/bitmap_bucket_number.slt`
  - `spark/json/json_tuple.slt`

Largest growth files:
- `sort_pushdown.slt`: `+748`
- `projection_pushdown.slt`: `+348/-191`
- `dynamic_filter_pushdown_config.slt`: `+301`
- `join_limit_pushdown.slt`: `+269`

Aggregate test corpus size in `datafusion/sqllogictest/test_files`:
- files: `459 -> 463`
- lines: `134122 -> 136189`
- runnable records (query/statement/skipif/onlyif markers): `14845 -> 15110` (`+265`)

Implication: the same CI command now executes more sqllogictest content than before.

### 2.1) PRs in this merge range that expanded sqllogictest corpus
The following PRs (from `8f959bba6..76be0b64c`) had positive net line growth under
`datafusion/sqllogictest/test_files`:

- #20329 `fix: validate inter-file ordering in eq_properties()` (`+538`)
- #20192 `Support parent dynamic filters for more join types` (`+282`)
- #20228 `feat: Push limit into hash join` (`+265`)
- #20247 `Fix incorrect SortExec removal before AggregateExec` (`+210`)
- 
- #20117 `feat: add ExtractLeafExpressions optimizer rule for get_field pushdown` (`+166`)
- #20412 `feat: support Spark-compatible json_tuple function` (`+154`)
- #20288 `feat: Implement Spark bitmap_bucket_number function` (`+122`)
- #20275 `feat: Implement Spark bitmap_bit_position function` (`+112`)
- #20420 `test: Extend Spark Array functions: array_repeat, shuffle and slice test coverage` (`+55`)
- #20189 `Adds support for ANSI mode in negative function` (`+52`)
- #20224 `fix: Fix scalar broadcast for to_timestamp()` (`+26`)
- #20279 `fix: disable dynamic filter pushdown for non min/max aggregates` (`+19`)
- #20361 `fix: Handle Utf8View and LargeUtf8 separators in concat_ws` (`+19`)
- #20191 `Support pushing down empty projections into joins` (`+19`)
- #20328 `perf: Optimize trim UDFs for single-character trims` (`+9`)
- #20241 `fix: Add integer check for bitwise coercion` (`+8`)
- #20305 `perf: Optimize translate() UDF for scalar inputs` (`+5`)
- #20341 `Reduce ExtractLeafExpressions optimizer overhead with fast pre-scan` (`+2`)

Notes:
- Net growth values above are line-based deltas in `datafusion/sqllogictest/test_files`.
- Some PRs touched sqllogictests with net `0` (balanced add/remove) and are excluded here.

### 3) Sqllogictest crate/dependency changes also landed from `main`
In `datafusion/sqllogictest/Cargo.toml`:
- `sqllogictest 0.29.0 -> 0.29.1`
- `clap 4.5.57 -> 4.5.60`

`Cargo.lock` in the merge range changed significantly (`+350/-185`), including new packages.

Implication: compile/setup time for the job can increase even if workflow YAML is unchanged.

### 4) Datafusion engine/query-planning code changed heavily in the merge range
This merge pulled many optimizer/execution changes from `main` (plus extensive sqllogictest updates). Even with "perf" commits, net runtime of this specific test corpus can still shift.

Implication: execution time of thousands of sqllogictest queries can change due to planner/executor behavior changes, not only due to test-count growth.

## Most likely explanation
The duration increase is most likely from **workload growth + dependency/build churn introduced from `main`**, not from a workflow definition change in commit `76be0b64c` itself.

In other words, `76be0b64c` is the integration point where many upstream changes became active on this branch.

## Confidence
- High confidence: no job YAML change in this merge, and sqllogictest corpus/deps grew.
- Medium confidence on exact split between "build-time increase" vs "test-runtime increase" because I could not fetch GitHub step timing logs in this environment.

## Limitation encountered
`gh auth status` shows the local GitHub token is invalid, so I could not inspect historical GitHub Actions step durations for direct Build-vs-Run timing attribution.

## Recommended next check (to confirm exact driver)
Compare step durations for two runs (before/after `76be0b64c`) for:
1. `Build sqllogictest binary`
2. `Run sqllogictest`

If Build step grew most: dependency/compile churn is primary.
If Run step grew most: test corpus / query execution behavior is primary.
