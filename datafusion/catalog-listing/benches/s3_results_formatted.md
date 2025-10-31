# S3 Benchmark Results

Benchmark results comparing `original` (main) vs `list_all` implementations for S3 partition listing.

- **Total**: Time to collect all results from the file listing stream (`.collect::<Vec<_>>()`). Represents complete listing overhead.
- **TTFR**: Time To First Result - when the stream yields its first file. DataFusion can begin downstream processing (e.g. metadata fetching) after this duration.
- **Speedup**: Ratio of `original`/`list_all` performance (>1.0x means `list_all` is faster)

---

## 1. Depth Variation Tests (Constant Files = 100,000)

Testing performance across different directory depth levels with a constant total file count.

| Depth | Partitions | Files | Original Total | Original TTFR | List_All Total | List_All TTFR | Total Speedup | TTFR Speedup |
|-------|-----------|-------|----------------|---------------|----------------|---------------|---------------|--------------|
| 1 | 10 | 100,000 | 1.90s | 1.87s | 13.15s | 136ms | 0.14x | 13.75x |
| 2 | 100 | 100,000 | 1.36s | 1.16s | 13.19s | 134ms | 0.10x | 8.66x |
| 3 | 1,000 | 100,000 | 1.81s | 1.74s | 13.05s | 134ms | 0.14x | 12.99x |
| 4 | 10,000 | 100,000 | 13.76s | 13.77s | 13.07s | 137ms | 1.05x | 100.51x |
| 5 | 100,000 | 100,000 | 134.89s | 134.67s | 13.33s | 148ms | 10.12x | 909.93x |

---

## 2. Breadth Variation Tests (Files ≈100K)

Testing performance with varying partition breadth (partitions per directory level).

| Breadth | Partitions | Files | Original Total | Original TTFR | List_All Total | List_All TTFR | Total Speedup | TTFR Speedup |
|---------|-----------|-------|----------------|---------------|----------------|---------------|---------------|--------------|
| 5 | 125 | 100,000 | 1.48s | 1.24s | 13.17s | 135ms | 0.11x | 9.19x |
| 10 | 1,000 | 100,000 | 1.81s | 1.74s | 13.05s | 135ms | 0.14x | 12.89x |
| 20 | 8,000 | 96,000 | 10.48s | 10.45s | 12.66s | 135ms | 0.83x | 77.41x |
| 30 | 27,000 | 81,000 | 34.08s | 34.06s | 10.72s | 141ms | 3.18x | 241.56x |
| 32 | 32,768 | 98,304 | 41.35s | 41.31s | 12.99s | 138ms | 3.18x | 299.35x |

---

## 3. Scale Tests (Constant Partitions = 32,768)

Testing performance with varying file counts while keeping partition count constant.

| Files/Partition | Total Partitions | Total Files | Original Total | Original TTFR | List_All Total | List_All TTFR | Total Speedup | TTFR Speedup |
|-----------------|-----------------|------------|----------------|---------------|----------------|---------------|---------------|--------------|
| 1 | 32,768 | 32,768 | 45.77s | 45.75s | 4.42s | 149ms | 10.36x | 307.05x |
| 5 | 32,768 | 163,840 | 45.83s | 45.74s | 21.63s | 137ms | 2.12x | 333.87x |
| 15 | 32,768 | 491,520 | 46.01s | 45.85s | 64.70s | 136ms | 0.71x | 337.13x |

---

## 4. Combined Scenario Tests

Realistic scenarios with various depth, breadth, and file combinations.

| Scenario | Depth | Breadth | Files/Part | Partitions | Total Files | Original Total | Original TTFR | List_All Total | List_All TTFR | Total Speedup | TTFR Speedup |
|----------|-------|---------|-----------|-----------|------------|----------------|---------------|----------------|---------------|---------------|--------------|
| Tiny | 5 | 1 | 1 | 1 | 1 | 728ms | 729ms | 122ms | 121ms | 5.96x | 6.02x |
| Small | 1 | 100 | 1 | 100 | 100 | 247ms | 246ms | 125ms | 125ms | 1.98x | 1.97x |
| Medium | 2 | 50 | 10 | 2,500 | 25,000 | 3.30s | 3.29s | 3.38s | 136ms | 0.98x | 24.19x |
| Large | 3 | 20 | 50 | 8,000 | 400,000 | 10.68s | 10.60s | 52.44s | 134ms | 0.20x | 79.10x |
| Deep | 5 | 5 | 10 | 3,125 | 31,250 | 5.15s | 5.17s | 4.22s | 136ms | 1.22x | 38.01x |

---

## 5. Partition Filtering Tests

Testing filter effectiveness with different pruning percentages.

### 5.1 Small Scale (100 partitions, 10,000 files)

| Filter Type | Partitions Pruned | Original Total | Original TTFR | List_All Total | List_All TTFR | Total Speedup | TTFR Speedup |
|-------------|------------------|----------------|---------------|----------------|---------------|---------------|--------------|
| No Filter | 0% | 439ms | 438ms | 1.45s | 134ms | 0.30x | 3.27x |
| Restrictive | 99% | 122ms | 123ms | 1.46s | 271ms | 0.08x | 0.45x |
| Broad | 50% | 438ms | 437ms | 1.47s | 135ms | 0.30x | 3.24x |

**Filter Clauses:**
- **Restrictive**: `WHERE a = 10 AND b = 100`
- **Broad**: `WHERE a <= 50`

### 5.2 Large Scale (32,768 partitions, 32,768 files)

| Filter Type | Partitions Pruned | Original Total | Original TTFR | List_All Total | List_All TTFR | Total Speedup | TTFR Speedup |
|-------------|------------------|----------------|---------------|----------------|---------------|---------------|--------------|
| No Filter | 0% | 45.78s | 45.81s | 4.42s | 163ms | 10.36x | 281.04x |
| Restrictive | 99.99% | 121ms | 121ms | 4.52s | 4.52s | 0.03x | 0.03x |
| Broad | 50% | 45.78s | 45.73s | 4.48s | 150ms | 10.22x | 304.87x |

**Filter Clauses:**
- **Restrictive**: `WHERE a = 1 AND b = 8 AND c = 64 AND d = 512 AND e = 4096`
- **Broad**: `WHERE a <= 4`

---
