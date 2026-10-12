# Sort pushdown SQL benchmarks

Run the existing sort pushdown queries through `benchmark_runner`. This suite
uses the same Parquet files and references the original SQL files under
`benchmarks/queries/`. The `dfbench` implementation and its commands remain
available for comparison.

From `benchmarks/`, prepare data with the existing generators:

```bash
./bench.sh data sort_pushdown
./bench.sh data sort_pushdown_inexact
```

The second command also generates the overlap dataset and requires PyArrow.
The SQL suite does not generate or rewrite data.

| SQL subgroup       | Existing benchmark               | Dataset directory               | Queries |
|--------------------|----------------------------------|---------------------------------|---------|
| `baseline`         | `sort_pushdown`                  | `sort_pushdown`                 | 1–8     |
| `sorted`           | `sort_pushdown_sorted`           | `sort_pushdown`                 | 1–8     |
| `inexact`          | `sort_pushdown_inexact`          | `sort_pushdown_inexact`         | 1–4     |
| `inexact_unsorted` | `sort_pushdown_inexact_unsorted` | `sort_pushdown_inexact`         | 1–6     |
| `inexact_overlap`  | `sort_pushdown_inexact_overlap`  | `sort_pushdown_inexact_overlap` | 1–4     |

Run the standalone SQL suite directly:

```bash
cargo run --release --bin benchmark_runner -- sort_pushdown --iterations 5
```

Use `--subgroup` and `--query` to select a variant and original query number:

```bash
cargo run --release --bin benchmark_runner -- sort_pushdown \
  --subgroup sorted --query 2 --iterations 5
```

Omit `--subgroup` to run all 30 cases. `--path` points to the parent directory
containing the dataset directories in the table above; it defaults to
`benchmarks/data`. Each dataset must contain a `lineitem/` directory.

The suite forces statistics collection, as the legacy runner does. The three
inexact subgroups also enable Parquet filter pushdown, matching their `bench.sh`
commands. The ordered subgroups declare `l_orderkey ASC NULLS LAST`, including
for queries that request descending output. Other settings use the common
runner options and environment variables.
