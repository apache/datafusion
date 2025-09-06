# MATCH_RECOGNIZE Physical Implementation

This document provides a comprehensive overview of the physical implementation of SQL MATCH_RECOGNIZE pattern matching in DataFusion. The implementation turns a logical pattern (compiled from SQL) into a compact NFA and streams Arrow RecordBatches to produce matches with the right semantics: greedy, left‑most precedence, anchors (^/$), quantifiers, alternation, grouping, PERMUTE, and AFTER MATCH SKIP with multiple ROWS PER MATCH modes.

## Architecture at a Glance

Flow of data and control:

1. Parser/logical layer (outside this crate) produces a pattern AST (`datafusion_expr::match_recognize::Pattern`) and the list of DEFINE symbols.
2. Compiler builds an immutable NFA and the symbol mapping.
3. The execution plan (`MatchRecognizePatternExec`) slices input batches into partitions, instantiates a streaming matcher, and accumulates results into output batches.
4. The matcher simulates the NFA row‑by‑row with ε‑closures, greedy scoring and left‑most tie‑breaks, honoring anchors and AFTER MATCH SKIP.

```
SQL Pattern → Compiler → NFA → Matcher → Output Rows
    ↓           ↓        ↓       ↓          ↓
  "A B+"    NfaBuilder  States  Simulate   Metadata
```

Key modules:

- **compile/** - Pattern compilation to NFA
  - `builder.rs` — façade that walks the `Pattern` AST and creates NFA states
  - `concat_alt.rs` — helpers for concatenation and alternation
  - `quantifier.rs` — repetition quantifiers (\*, +, {m,n}, …)
  - `permute.rs` — PERMUTE(N1, …, Nk) without factorial blow‑up
- **nfa.rs** — immutable NFA/data types shared by compiler and matcher
- **matcher/** - Runtime pattern matching engine
  - `row_loop.rs` — outer scan loop and per‑row driving logic
  - `epsilon.rs` — ε‑closure with optional anchor checking
  - `state_set.rs` (+ `dedup.rs`) — per‑row state deduplication arena
  - `symbols.rs` — hot‑path symbol evaluation from Boolean columns
  - `candidate_mod.rs` — greedy/left‑most/branch precedence ordering
- **pattern_exec.rs** — physical operator integrating with DF execution

## Core Data Model

The implementation uses the following data model:

- **Sym** — Dense symbol id used at runtime: 0 = Empty sentinel, 1+ = user symbols
- **PathStep** `{ row, sym }` — One logical step in a match path
- **PathNode** — Persistent linked list for paths with shared prefixes
- **PathScore** `{ total_steps, classified_steps }` — Greedy score: longer path first, then more classified (non‑special) symbols
- **NFAState** — Core automaton state with:
  - `id`, `is_accepting`
  - `transitions`: HashMap<String, HashSet<usize>> (string keys while compiling)
  - `numeric_transitions`: Vec<Vec<usize>> (dense by runtime symbol id)
  - `epsilon_transitions_pred`: Vec<(dst, Option<AnchorPredicate>)>
  - `epsilon_closure`, `unconditional_closure` (precomputed)
  - `alt_branch_idx`: Option<u32> (alternation branch precedence tag)
- **AnchorPredicate** — StartOfInput (^) and EndOfInput ($) attached to ε‑edges

The compiler produces `CompiledPattern`:

- `pattern`: original AST (for diagnostics)
- `id_to_symbol`: Vec<Option<String>> with index 0 reserved for Empty
- `symbol_to_id`: HashMap<String, usize>
- `nfa`: immutable slice of `NFAState` (plus one accepting sink)
- `after_match_skip`, `empty_matches_mode` flags used by runtime
- `has_anchor_preds`: quick flag for anchor‑aware ε‑closure

## Compilation Pipeline

**Entry point**: `compile::CompiledPattern::compile` or its builder API.

### 1. Build NFA from AST

`compile::NfaBuilder::build` recursively processes pattern types:

- **Symbol(Named)** → transition start -name-> end
- **Symbol(Start)** → ε[^] to end (with predicate)
- **Symbol(End)** → ε[$] to end
- **Concat(A, B, …)** — sequentially link end(A) →ε→ start(B)
- **Alternation(A | B | …)** — new start, ε to each branch start; ε from each branch end to a shared end; tag each branch terminal with `alt_branch_idx`
- **Repetition(inner, quant)** — see quantifiers below
- **Group(inner)** — compile inner directly
- **Exclude(sym)** — currently compiled like a symbol (physical exclusion handled elsewhere)
- **PERMUTE(symbols)** — attach a dedicated sub‑NFA

### 2. Add Single Accepting Sink

After building the pattern, a fresh accepting state is added and the pattern's local end connects via ε to that sink so that the machine exposes exactly one accepting state reachable from the pattern.

### 3. Precompute Closures

For every state, compute:

- **epsilon_closure**: DFS over all ε‑edges (including anchors), sorted
- **unconditional_closure**: DFS over only `None`‑predicate ε‑edges (fast path when anchors don't apply on a row)

### 4. Finalize Symbol Mapping

After the list of DECLARE/DEFINE symbols is known:

- `id_to_symbol[0] = None` (Empty), then user symbols in declaration order
- Replace `transitions` with a dense `numeric_transitions: Vec<Vec<usize>>` indexed by runtime symbol id to avoid string lookups on the hot path
- Clear the string map to save memory

### 5. Detect Anchors

Set `has_anchor_preds` if any ε‑edge carries a predicate.

### Quantifiers (`compile/quantifier.rs`)

- Extract `min`, `max`, and whether the pattern loops (`*`, `+`, `{m,}`)
- Emit `min` mandatory copies; for bounded `max`, append additional optional copies
- For unbounded, add a loop from last end back to the last copy's start; if `min == 0` also add ε from the first start to the last end (optional branch)

### Alternation Precedence

`compile_alternation` tags each branch's terminal state with `alt_branch_idx` (0‑based by branch order). The runtime uses this to enforce SQL's left‑most rule.

### PERMUTE Without Factorial Blow‑up (`compile/permute.rs`)

Build a sub‑NFA whose states are keyed by a multiset counter of how many of each unique symbol has been matched so far. BFS over keys emits transitions only when the corresponding count can still increase. All accepting keys (fully consumed multiset) connect via ε to one shared sink.

**Example** (from tests):

```
PERMUTE(A, B)
0: A->1 | B->2
1: B->3
2: A->3
3: ε->4
4: ε->5
5*:
```

### Example: NFA for pattern "A B+"

```
0: A->1
1: ε->2
2: B->3
3: ε->2 | ε->4  (loop back to B or continue)
4: ε->5
5*:
```

## Matching Algorithm (Streaming)

The matcher operates per partition and per physical row, simulating the NFA with state management and tie-breaking.

### Outer Loop (`matcher/row_loop.rs`)

1. **Start Scanning**: Begin at each potential start row, including a virtual End‑Of‑Input row (total_rows = physical_len + 1)
2. **Maintain Frontier**: Keep a set of active NFA states; begin with state_id 0, empty path and zero score
3. **For Each Row**:
   - Evaluate Boolean DEFINE predicates for all symbols into a dense `[bool]` (no allocation)
   - Compute ε‑closure for each active state:
     - `AnchorMode::Ignore` → use precomputed `unconditional_closure`
     - `AnchorMode::Check` → DFS honoring `AnchorPredicate` on the virtual row index (0 allows ^, last virtual row allows $)
   - For each reachable state and for each symbol id that matched this row, traverse `numeric_transitions[state][sym_id]` and create successor states
   - Extend the persistent `PathNode` and `PathScore` for each successor
   - Compute ε‑closure again from each successor; if closure reaches an accepting state, consider it a candidate best match
   - Insert successors into a dense per‑row arena (`DedupArena`) keyed by `state_id`. If the same state appears again on the current row, keep only the path with the better greedy/left‑most score
4. **Check Empty Matches**: Also check ε‑only accepting states for the current row and push candidates (creates an Empty step when needed)
5. **Advance Frontier**: Replace the frontier with the arena's populated entries and move to the next row
6. **Final Check**: After the last physical row, run a final ε‑only accepting check

### Candidate Ordering (`matcher/candidate_mod.rs`)

Order by (ascending):

1. **Alternation branch precedence** (`alt_branch_idx`) — earlier branch wins (left‑most)
2. **Greedy score** — higher `PathScore` wins (longer path, then more classified steps)
3. **State ID** — lower wins (left‑most tie‑break)
4. **End row** — earlier wins (final tie‑break)

### Deduplication Arena (`matcher/state_set.rs` + `matcher/dedup.rs`)

- Dense `Vec<Option<ActiveNFAState>>` indexed by `state_id`
- `next_gen_flags: Vec<u32>` with a per‑row generation counter avoids clearing the dense vector on every row; wrap‑around resets the flags vector to zeros
- When inserting the same `state_id` again on the row, compare as candidates and keep the better one

### Anchors (`matcher/epsilon.rs`)

- If the compiled pattern has no anchor predicates, ε‑closures use the unconditional precomputed closure
- Otherwise anchors are checked only on virtual rows that can satisfy ^ or $: row 0 and the last virtual row (`virt_row + 1 == total_virt_rows`)

### Symbol Evaluation (`matcher/symbols.rs`)

- Pre‑sliced Boolean arrays per partition; at each physical row, populate a reused `[bool]` vector and collect matching symbol ids with a simple scan

### AFTER MATCH SKIP (`row_loop.rs::next_scan_row`)

- **PastLastRow** — resume after the last row of the chosen best match
- **ToNextRow** — resume at next row regardless of match extent (allows overlap)
- **ToFirst/ToLast(symbol)** — resume at the row of the first/last occurrence of `symbol` within the chosen match; if absent, behave like PastLastRow

### ROWS PER MATCH — Empty Match Handling

- **OneRow** — default (no unmatched rows)
- **AllRows(Omit)** — Empty steps are part of the match internally but are not emitted as rows
- **AllRows(WithUnmatched)** — the accumulator appends unmatched physical rows with null metadata and all classifier bitsets false

**Complexity** (per row, per partition): O(#NFA states + #transitions taken), with linear memory due to the dense arena and no per‑row allocations in the hot path. ε‑closures reuse a bitmap with a generation counter.

## Physical Operator (`pattern_exec.rs`)

### Schema and Output

- **Schema** — built from an `OutputSpec` that selects passthrough input columns, which metadata columns to materialize (CLASSIFIER, MATCH_NUMBER, MATCH_SEQUENCE_NUMBER), and which classifier bitset columns to append (one Boolean column per selected symbol)
- **Order/partition semantics** — partitions are determined by PARTITION BY; optional ORDER BY is admissible when the operator preserves input order
- **Preserving input order** — guaranteed only when `AFTER MATCH SKIP PAST LAST ROW` and rows‑per‑match mode does not emit unmatched rows. In other cases, emitted rows may be reordered relative to the input

### Properties and Requirements

- **Properties (ordering/equivalence)** — projects child properties to passthrough columns; advertises additional orderings (partition prefix, then optional ORDER BY tail, and/or MATCH_NUMBER/MATCH_SEQUENCE_NUMBER when admissible)
- **Required input distribution and ordering** — mirrors WindowAgg: hash partitioning by keys and the corresponding ordering requirements
- **Streaming** — buffers input batches until either a partition closes or a configurable threshold (`batch_size`) is reached, then runs the matcher and flushes output. The last open partition may be carried over to the next poll to honor partition boundaries

### MatchAccumulator

Handles output generation:

- Collects output row indices; projects input columns, then `take_record_batch` by those indices
- Emits selected metadata columns and classifier bitset columns
- Tracks and emits unmatched rows when requested
- Counts logical matches to feed metrics

## Metrics (Per Partition)

Counters and timers exposed via `PatternMetrics` and visible in the execution metrics:

- **input_batches**, **input_rows**, **matches_emitted**
- **define_pred_evals** — total DEFINE predicate evaluations
- **nfa_state_transitions** — number of NFA transitions traversed
- **nfa_active_states_max** — peak frontier size
- **match_compute_time** (overall), and subsets:
  - **symbol_eval_time**, **epsilon_eval_time**, **nfa_eval_time**, **row_loop_time**,
    **transition_time**, **candidate_time**, **alloc_time**, **slice_time**

## Debugging and Testing

### Snapshot Tests (`nfa.rs`)

Render the compiled NFA graphically (topology + ε‑edges):

```
0: ε[^]->1
1: ε->2
2: A->3
3: ε->4
4: ε[$]->5
5: ε->6
6*:
```

### Runtime Tests (`matcher_tests.rs`)

Exercise:

- Basic symbol matches and metadata columns
- AFTER MATCH SKIP variants (overlap vs non‑overlap)
- WITH UNMATCHED ROWS behavior
- Anchors (^ / $) over full partitions
- PERMUTE order‑insensitive acceptance
- Left‑most precedence over alternation and when it conflicts with greedy length

### Tips

- Print/inspect `CompiledPattern` symbol tables and NFA size when adding new features
- Validate ε‑closure correctness with anchors by crafting minimal patterns `^`, `$`, `^ A+ $`
- Instrument specific timers to isolate hot paths (symbol eval vs ε‑closure vs transitions)

## Extensibility Guidelines

### New Pattern Constructs

- Add compilation in `compile/*` producing the standard NFA fragments
- Ensure the compiled NFA ends at a single accepting sink
- Tag alternation branch terminal states with `alt_branch_idx` when left‑most precedence must apply
- If you add new zero‑width predicates, extend `AnchorPredicate` and `epsilon.rs` and precompute an appropriate unconditional closure

### Symbol Model

- Keep index 0 reserved for `Sym::Empty`
- Always populate `numeric_transitions` during `CompiledPattern::compile`
- Avoid string lookups on the hot path

### Performance

- Use generation counters to avoid clearing large buffers
- Avoid allocations on the hot path; use `Arc<PathNode>` only when extending a match path
- Maintain deterministic ordering (sort closures; use stable tie‑breaks)

### Testing

- Add snapshot tests for new compilation patterns
- Add matcher tests covering greedy/left‑most conflicts and anchor behavior

## File Map

### compile/

- **builder.rs** — orchestrates AST → NFA, ε‑closures
- **concat_alt.rs** — concat/alternation fragments
- **quantifier.rs** — quantified repetition
- **permute.rs** — multiset‑keyed PERMUTE NFA

### matcher/

- **matcher.rs** — `PatternMatcher` entry points and tests
- **row_loop.rs** — row scanning, AFTER MATCH SKIP, accumulation hookups
- **epsilon.rs** — ε‑closure (Ignore vs Check anchors)
- **state_set.rs**, **dedup.rs** — per‑row successor dedup arena
- **symbols.rs** — evaluate DEFINE predicate columns fast
- **candidate_mod.rs** — candidate ordering (branch → greedy → left‑most)

### Other

- **pattern_exec.rs** — physical operator, streaming, metrics, schema/properties
- **nfa.rs** — NFA/NFAState/Sym/Path\* types, display, tests

## Performance Characteristics

- **Time Complexity**: O(#NFA states + #transitions taken) per row, with linear memory
- **Memory Usage**: Dense arenas, generation counters, shared path nodes
- **Hot Path**: No allocations with reusable buffers and precomputed closures
- **Scalability**: Linear per partition for most patterns

## Implementation Characteristics

1. **Modular Design**: Separation between compilation, matching, and execution
2. **Performance Features**: Generation-based state tracking, dense symbol indexing, shared path nodes
3. **SQL Compliance**: Greedy/left‑most semantics, anchor handling, precedence rules
4. **Streaming Integration**: Batched processing with bounded memory usage
5. **Testing Coverage**: Snapshot tests for compilation, runtime tests for matching behavior

The implementation provides SQL MATCH_RECOGNIZE pattern matching functionality within DataFusion's streaming execution model.

## License

This module is licensed under the Apache License, Version 2.0, consistent with the rest of the project.
