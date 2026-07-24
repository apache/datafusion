# Parquet row-group prefetch benchmark

This report compares Parquet scans with prefetching disabled against a 20 MiB
(`20971520` byte) I/O-only prefetch budget. A speedup greater than `1.00x` is
faster. Per-query times are the arithmetic mean of three iterations, and
speedup is `prefetch-off / prefetch-on`.

## Setup

- Build profile: `release-nonlto`.
- TPC-H: SF1, ZSTD(1), one Parquet file per table, approximately 7 MiB target
  row groups, all 22 queries, default hash joins.
- ClickBench: the existing 43-query suite against a local 167 MiB single-file
  ClickBench dataset. This is smaller than the full 14 GiB ClickBench dataset,
  so the absolute times should not be compared with published ClickBench runs.
- Simulated I/O: the existing `--simulate-latency` benchmark option (25-200 ms
  per GET and 40-400 ms per LIST).
- Prefetching was enabled with
  `datafusion.execution.parquet.prefetch_size=20971520`.

The very short no-latency ClickBench measurements are sensitive to ordinary
sub-millisecond noise. The aggregate and simulated-I/O results are the more
useful signals. The latency wrapper cycles through a fixed latency
distribution, so changing the request count also changes which latency samples
land on an individual query; aggregate results are more reliable than isolated
simulated-I/O outliers.

## Aggregate results

| Benchmark | Simulated I/O | Sum, off | Sum, 20 MiB | Workload speedup | Geomean query speedup |
|---|---:|---:|---:|---:|---:|
| TPC-H SF1 | no | 998.12 ms | 1011.01 ms | 0.99x | 0.99x |
| TPC-H SF1 | yes | 25103.83 ms | 12923.06 ms | 1.94x | 1.95x |
| ClickBench | no | 1462.91 ms | 1421.74 ms | 1.03x | 1.04x |
| ClickBench | yes | 11932.80 ms | 9823.90 ms | 1.21x | 1.22x |

With filter pushdown and filter reordering enabled:

| Benchmark | Simulated I/O | Sum, off | Sum, 20 MiB | Workload speedup | Geomean query speedup |
|---|---:|---:|---:|---:|---:|
| TPC-H SF1 | no | 1282.95 ms | 1255.47 ms | 1.02x | 1.03x |
| TPC-H SF1 | yes | 53082.86 ms | 30651.08 ms | 1.73x | 1.67x |
| ClickBench | no | 1293.52 ms | 1285.13 ms | 1.01x | 1.04x |
| ClickBench | yes | 18852.91 ms | 15916.91 ms | 1.18x | 1.22x |

## TPC-H per-query results

| Query | Latency off (off → 20 MiB) | Speedup | Simulated I/O (off → 20 MiB) | Speedup |
|---:|---:|---:|---:|---:|
| Q1 | 65.38 → 62.28 ms | 1.05x | 709.56 → 233.16 ms | 3.04x |
| Q2 | 18.83 → 18.59 ms | 1.01x | 1035.59 → 846.49 ms | 1.22x |
| Q3 | 46.21 → 41.11 ms | 1.12x | 1048.95 → 593.33 ms | 1.77x |
| Q4 | 20.30 → 21.01 ms | 0.97x | 982.91 → 412.13 ms | 2.38x |
| Q5 | 49.29 → 50.99 ms | 0.97x | 1471.05 → 804.32 ms | 1.83x |
| Q6 | 24.33 → 27.05 ms | 0.90x | 698.34 → 207.41 ms | 3.37x |
| Q7 | 59.30 → 54.87 ms | 1.08x | 1373.52 → 814.54 ms | 1.69x |
| Q8 | 51.34 → 55.64 ms | 0.92x | 1513.23 → 899.25 ms | 1.68x |
| Q9 | 65.91 → 80.39 ms | 0.82x | 1446.88 → 794.52 ms | 1.82x |
| Q10 | 52.00 → 55.71 ms | 0.93x | 1144.27 → 597.02 ms | 1.92x |
| Q11 | 17.94 → 13.47 ms | 1.33x | 1018.01 → 734.22 ms | 1.39x |
| Q12 | 34.69 → 38.85 ms | 0.89x | 993.30 → 421.07 ms | 2.36x |
| Q13 | 39.78 → 35.38 ms | 1.12x | 433.36 → 396.48 ms | 1.09x |
| Q14 | 32.14 → 31.84 ms | 1.01x | 861.29 → 314.42 ms | 2.74x |
| Q15 | 38.60 → 43.45 ms | 0.89x | 1409.70 → 546.98 ms | 2.58x |
| Q16 | 14.21 → 15.12 ms | 0.94x | 484.33 → 331.93 ms | 1.46x |
| Q17 | 100.43 → 103.89 ms | 0.97x | 1504.40 → 583.54 ms | 2.58x |
| Q18 | 91.62 → 86.41 ms | 1.06x | 1794.52 → 803.55 ms | 2.23x |
| Q19 | 48.18 → 45.21 ms | 1.07x | 913.29 → 378.06 ms | 2.42x |
| Q20 | 39.42 → 41.53 ms | 0.95x | 1199.47 → 603.65 ms | 1.99x |
| Q21 | 74.40 → 73.09 ms | 1.02x | 2515.58 → 1051.49 ms | 2.39x |
| Q22 | 13.82 → 15.13 ms | 0.91x | 552.29 → 555.50 ms | 0.99x |

## ClickBench per-query results

| Query | Latency off (off → 20 MiB) | Speedup | Simulated I/O (off → 20 MiB) | Speedup |
|---:|---:|---:|---:|---:|
| Q0 | 0.85 → 0.86 ms | 0.99x | 55.60 → 57.51 ms | 0.97x |
| Q1 | 3.24 → 1.97 ms | 1.64x | 298.84 → 248.13 ms | 1.20x |
| Q2 | 3.38 → 3.16 ms | 1.07x | 241.95 → 174.08 ms | 1.39x |
| Q3 | 2.96 → 2.82 ms | 1.05x | 246.76 → 236.58 ms | 1.04x |
| Q4 | 10.59 → 10.00 ms | 1.06x | 270.48 → 183.75 ms | 1.47x |
| Q5 | 23.37 → 22.35 ms | 1.05x | 242.77 → 239.81 ms | 1.01x |
| Q6 | 1.01 → 0.83 ms | 1.22x | 127.73 → 79.46 ms | 1.61x |
| Q7 | 2.52 → 2.22 ms | 1.14x | 263.30 → 201.38 ms | 1.31x |
| Q8 | 15.96 → 16.00 ms | 1.00x | 300.40 → 220.09 ms | 1.36x |
| Q9 | 19.75 → 20.02 ms | 0.99x | 265.57 → 241.42 ms | 1.10x |
| Q10 | 5.88 → 5.85 ms | 1.01x | 344.91 → 220.57 ms | 1.56x |
| Q11 | 6.57 → 6.49 ms | 1.01x | 276.63 → 211.88 ms | 1.31x |
| Q12 | 22.88 → 22.93 ms | 1.00x | 239.92 → 220.93 ms | 1.09x |
| Q13 | 25.75 → 26.20 ms | 0.98x | 278.98 → 286.83 ms | 0.97x |
| Q14 | 23.55 → 23.17 ms | 1.02x | 230.76 → 161.37 ms | 1.43x |
| Q15 | 11.31 → 11.78 ms | 0.96x | 343.50 → 274.02 ms | 1.25x |
| Q16 | 33.06 → 32.57 ms | 1.02x | 282.15 → 196.85 ms | 1.43x |
| Q17 | 32.94 → 31.96 ms | 1.03x | 277.19 → 282.06 ms | 0.98x |
| Q18 | 36.88 → 35.34 ms | 1.04x | 328.66 → 222.46 ms | 1.48x |
| Q19 | 2.94 → 2.71 ms | 1.09x | 176.61 → 232.76 ms | 0.76x |
| Q20 | 42.95 → 42.28 ms | 1.02x | 297.22 → 255.14 ms | 1.16x |
| Q21 | 43.87 → 45.19 ms | 0.97x | 274.66 → 251.58 ms | 1.09x |
| Q22 | 88.52 → 89.49 ms | 0.99x | 375.51 → 288.93 ms | 1.30x |
| Q23 | 218.85 → 218.39 ms | 1.00x | 450.76 → 465.70 ms | 0.97x |
| Q24 | 16.65 → 16.43 ms | 1.01x | 240.95 → 194.64 ms | 1.24x |
| Q25 | 14.10 → 13.27 ms | 1.06x | 238.21 → 230.81 ms | 1.03x |
| Q26 | 16.69 → 16.48 ms | 1.01x | 220.52 → 228.45 ms | 0.97x |
| Q27 | 53.24 → 49.59 ms | 1.07x | 326.81 → 255.37 ms | 1.28x |
| Q28 | 244.63 → 226.17 ms | 1.08x | 466.57 → 398.93 ms | 1.17x |
| Q29 | 13.16 → 14.16 ms | 0.93x | 288.82 → 215.63 ms | 1.34x |
| Q30 | 21.81 → 22.27 ms | 0.98x | 247.12 → 214.86 ms | 1.15x |
| Q31 | 24.88 → 20.94 ms | 1.19x | 266.06 → 225.74 ms | 1.18x |
| Q32 | 19.73 → 18.41 ms | 1.07x | 285.02 → 243.70 ms | 1.17x |
| Q33 | 70.19 → 68.18 ms | 1.03x | 331.59 → 255.69 ms | 1.30x |
| Q34 | 69.13 → 68.15 ms | 1.01x | 340.37 → 266.57 ms | 1.28x |
| Q35 | 10.92 → 10.67 ms | 1.02x | 235.95 → 220.74 ms | 1.07x |
| Q36 | 47.33 → 46.54 ms | 1.02x | 301.37 → 224.93 ms | 1.34x |
| Q37 | 11.98 → 11.30 ms | 1.06x | 264.87 → 179.13 ms | 1.48x |
| Q38 | 39.00 → 36.89 ms | 1.06x | 283.74 → 192.01 ms | 1.48x |
| Q39 | 88.38 → 86.75 ms | 1.02x | 326.41 → 332.52 ms | 0.98x |
| Q40 | 7.85 → 7.50 ms | 1.05x | 288.58 → 176.32 ms | 1.64x |
| Q41 | 6.70 → 6.67 ms | 1.00x | 191.50 → 186.57 ms | 1.03x |
| Q42 | 6.94 → 6.78 ms | 1.02x | 297.48 → 128.00 ms | 2.32x |

## Local I/O with filter pushdown

Both sides enable Parquet filter pushdown and filter reordering, with simulated
latency disabled. Only the prefetch budget changes. The ClickBench Q0 and Q1
ratios are dominated by sub-millisecond and cold-start noise; the summed
workload is the more representative result.

### TPC-H

| Query | Prefetch off | 20 MiB | Speedup |
|---:|---:|---:|---:|
| Q1 | 62.45 ms | 63.14 ms | 0.99x |
| Q2 | 23.69 ms | 20.15 ms | 1.18x |
| Q3 | 52.11 ms | 53.44 ms | 0.98x |
| Q4 | 30.20 ms | 26.15 ms | 1.15x |
| Q5 | 71.81 ms | 72.70 ms | 0.99x |
| Q6 | 44.96 ms | 43.68 ms | 1.03x |
| Q7 | 60.94 ms | 67.64 ms | 0.90x |
| Q8 | 78.46 ms | 79.73 ms | 0.98x |
| Q9 | 108.11 ms | 107.55 ms | 1.01x |
| Q10 | 59.53 ms | 59.02 ms | 1.01x |
| Q11 | 14.61 ms | 14.38 ms | 1.02x |
| Q12 | 56.54 ms | 54.40 ms | 1.04x |
| Q13 | 45.31 ms | 43.73 ms | 1.04x |
| Q14 | 34.66 ms | 36.22 ms | 0.96x |
| Q15 | 52.58 ms | 48.81 ms | 1.08x |
| Q16 | 17.45 ms | 16.00 ms | 1.09x |
| Q17 | 186.72 ms | 171.16 ms | 1.09x |
| Q18 | 85.94 ms | 83.64 ms | 1.03x |
| Q19 | 44.47 ms | 43.07 ms | 1.03x |
| Q20 | 40.49 ms | 40.79 ms | 0.99x |
| Q21 | 96.33 ms | 94.82 ms | 1.02x |
| Q22 | 15.59 ms | 15.28 ms | 1.02x |

### ClickBench

| Query | Prefetch off | 20 MiB | Speedup |
|---:|---:|---:|---:|
| Q0 | 2.89 ms | 0.86 ms | 3.37x |
| Q1 | 3.32 ms | 1.83 ms | 1.81x |
| Q2 | 3.54 ms | 3.17 ms | 1.12x |
| Q3 | 2.86 ms | 3.07 ms | 0.93x |
| Q4 | 10.12 ms | 10.30 ms | 0.98x |
| Q5 | 22.49 ms | 23.19 ms | 0.97x |
| Q6 | 0.86 ms | 1.02 ms | 0.84x |
| Q7 | 2.62 ms | 2.95 ms | 0.89x |
| Q8 | 15.40 ms | 16.23 ms | 0.95x |
| Q9 | 19.36 ms | 20.24 ms | 0.96x |
| Q10 | 7.22 ms | 6.88 ms | 1.05x |
| Q11 | 7.56 ms | 7.63 ms | 0.99x |
| Q12 | 23.07 ms | 24.01 ms | 0.96x |
| Q13 | 26.82 ms | 27.67 ms | 0.97x |
| Q14 | 24.31 ms | 24.48 ms | 0.99x |
| Q15 | 11.43 ms | 11.91 ms | 0.96x |
| Q16 | 32.47 ms | 32.58 ms | 1.00x |
| Q17 | 30.72 ms | 30.81 ms | 1.00x |
| Q18 | 36.20 ms | 35.86 ms | 1.01x |
| Q19 | 2.92 ms | 2.85 ms | 1.02x |
| Q20 | 43.29 ms | 41.39 ms | 1.05x |
| Q21 | 45.29 ms | 46.08 ms | 0.98x |
| Q22 | 85.92 ms | 86.22 ms | 1.00x |
| Q23 | 142.91 ms | 137.17 ms | 1.04x |
| Q24 | 17.92 ms | 17.69 ms | 1.01x |
| Q25 | 14.36 ms | 14.28 ms | 1.01x |
| Q26 | 18.75 ms | 18.84 ms | 1.00x |
| Q27 | 52.33 ms | 53.18 ms | 0.98x |
| Q28 | 222.07 ms | 224.70 ms | 0.99x |
| Q29 | 13.09 ms | 12.96 ms | 1.01x |
| Q30 | 21.83 ms | 21.79 ms | 1.00x |
| Q31 | 21.96 ms | 21.87 ms | 1.00x |
| Q32 | 22.10 ms | 19.48 ms | 1.13x |
| Q33 | 67.55 ms | 66.73 ms | 1.01x |
| Q34 | 66.63 ms | 65.89 ms | 1.01x |
| Q35 | 10.68 ms | 10.32 ms | 1.03x |
| Q36 | 30.12 ms | 29.66 ms | 1.02x |
| Q37 | 13.79 ms | 13.66 ms | 1.01x |
| Q38 | 18.02 ms | 17.89 ms | 1.01x |
| Q39 | 52.27 ms | 51.81 ms | 1.01x |
| Q40 | 9.69 ms | 9.56 ms | 1.01x |
| Q41 | 8.88 ms | 8.84 ms | 1.00x |
| Q42 | 7.90 ms | 7.58 ms | 1.04x |

## Simulated I/O with filter pushdown

Both sides of this comparison enable Parquet filter pushdown and filter
reordering. Only the prefetch budget changes.

### TPC-H

| Query | Prefetch off | 20 MiB | Speedup |
|---:|---:|---:|---:|
| Q1 | 1289.75 ms | 791.69 ms | 1.63x |
| Q2 | 2238.25 ms | 1598.67 ms | 1.40x |
| Q3 | 2619.05 ms | 1546.62 ms | 1.69x |
| Q4 | 1668.98 ms | 1103.72 ms | 1.51x |
| Q5 | 2558.34 ms | 1514.43 ms | 1.69x |
| Q6 | 2121.67 ms | 1315.05 ms | 1.61x |
| Q7 | 3333.48 ms | 1534.35 ms | 2.17x |
| Q8 | 3816.23 ms | 2006.30 ms | 1.90x |
| Q9 | 2981.66 ms | 1279.27 ms | 2.33x |
| Q10 | 2620.31 ms | 1706.30 ms | 1.54x |
| Q11 | 1568.13 ms | 1293.11 ms | 1.21x |
| Q12 | 2637.63 ms | 1763.15 ms | 1.50x |
| Q13 | 900.48 ms | 639.89 ms | 1.41x |
| Q14 | 1471.44 ms | 1124.83 ms | 1.31x |
| Q15 | 3111.08 ms | 1730.41 ms | 1.80x |
| Q16 | 982.33 ms | 775.29 ms | 1.27x |
| Q17 | 2593.53 ms | 933.30 ms | 2.78x |
| Q18 | 2543.13 ms | 1130.98 ms | 2.25x |
| Q19 | 2880.48 ms | 1748.10 ms | 1.65x |
| Q20 | 2851.58 ms | 1599.39 ms | 1.78x |
| Q21 | 5323.19 ms | 2724.60 ms | 1.95x |
| Q22 | 972.12 ms | 791.61 ms | 1.23x |

### ClickBench

| Query | Prefetch off | 20 MiB | Speedup |
|---:|---:|---:|---:|
| Q0 | 56.55 ms | 58.40 ms | 0.97x |
| Q1 | 327.30 ms | 333.22 ms | 0.98x |
| Q2 | 246.76 ms | 198.63 ms | 1.24x |
| Q3 | 245.16 ms | 195.28 ms | 1.26x |
| Q4 | 287.64 ms | 199.63 ms | 1.44x |
| Q5 | 249.47 ms | 239.43 ms | 1.04x |
| Q6 | 126.97 ms | 113.46 ms | 1.12x |
| Q7 | 256.87 ms | 149.37 ms | 1.72x |
| Q8 | 319.22 ms | 269.40 ms | 1.18x |
| Q9 | 259.60 ms | 186.71 ms | 1.39x |
| Q10 | 392.25 ms | 347.71 ms | 1.13x |
| Q11 | 461.70 ms | 325.25 ms | 1.42x |
| Q12 | 246.07 ms | 219.90 ms | 1.12x |
| Q13 | 458.07 ms | 335.17 ms | 1.37x |
| Q14 | 420.26 ms | 330.00 ms | 1.27x |
| Q15 | 297.14 ms | 184.97 ms | 1.61x |
| Q16 | 268.01 ms | 250.93 ms | 1.07x |
| Q17 | 355.99 ms | 243.09 ms | 1.46x |
| Q18 | 305.64 ms | 242.55 ms | 1.26x |
| Q19 | 213.95 ms | 212.82 ms | 1.01x |
| Q20 | 302.75 ms | 280.37 ms | 1.08x |
| Q21 | 451.41 ms | 357.93 ms | 1.26x |
| Q22 | 519.06 ms | 496.29 ms | 1.05x |
| Q23 | 589.59 ms | 570.53 ms | 1.03x |
| Q24 | 308.38 ms | 249.42 ms | 1.24x |
| Q25 | 293.79 ms | 210.64 ms | 1.39x |
| Q26 | 471.43 ms | 325.42 ms | 1.45x |
| Q27 | 506.67 ms | 377.73 ms | 1.34x |
| Q28 | 502.78 ms | 382.46 ms | 1.31x |
| Q29 | 301.80 ms | 224.83 ms | 1.34x |
| Q30 | 393.39 ms | 316.58 ms | 1.24x |
| Q31 | 458.32 ms | 395.64 ms | 1.16x |
| Q32 | 294.24 ms | 218.91 ms | 1.34x |
| Q33 | 326.62 ms | 265.16 ms | 1.23x |
| Q34 | 378.21 ms | 275.89 ms | 1.37x |
| Q35 | 262.65 ms | 220.87 ms | 1.19x |
| Q36 | 909.99 ms | 856.96 ms | 1.06x |
| Q37 | 916.85 ms | 818.67 ms | 1.12x |
| Q38 | 1083.95 ms | 955.38 ms | 1.13x |
| Q39 | 756.59 ms | 842.90 ms | 0.90x |
| Q40 | 1069.77 ms | 837.90 ms | 1.28x |
| Q41 | 1070.80 ms | 990.05 ms | 1.08x |
| Q42 | 889.26 ms | 810.45 ms | 1.10x |

## 100 MiB budget with simulated I/O and filter pushdown

Both filter pushdown and filter reordering remain enabled. The 100 MiB result
uses the same three-iteration simulated-latency setup as the off and 20 MiB
results. A `20 MiB / 100 MiB` ratio greater than `1.00x` favors 100 MiB.

| Benchmark | Off | 20 MiB | 100 MiB | 100 MiB vs off | 20 MiB vs 100 MiB |
|---|---:|---:|---:|---:|---:|
| TPC-H SF1 | 53082.86 ms | 30651.08 ms | 30688.45 ms | 1.73x | 1.00x |
| ClickBench | 18852.91 ms | 15916.91 ms | 16100.72 ms | 1.17x | 0.99x |

The larger budget is effectively neutral on TPC-H and 1.2% slower than 20 MiB
on ClickBench. For these datasets, 20 MiB already captures the useful I/O
batching and increasing the budget does not improve aggregate performance.

### TPC-H

| Query | Off | 20 MiB | 100 MiB | 100 MiB vs off | 20 MiB vs 100 MiB |
|---:|---:|---:|---:|---:|---:|
| Q1 | 1289.75 ms | 791.69 ms | 741.42 ms | 1.74x | 1.07x |
| Q2 | 2238.25 ms | 1598.67 ms | 1628.24 ms | 1.37x | 0.98x |
| Q3 | 2619.05 ms | 1546.62 ms | 1544.35 ms | 1.70x | 1.00x |
| Q4 | 1668.98 ms | 1103.72 ms | 1154.20 ms | 1.45x | 0.96x |
| Q5 | 2558.34 ms | 1514.43 ms | 1516.25 ms | 1.69x | 1.00x |
| Q6 | 2121.67 ms | 1315.05 ms | 1451.57 ms | 1.46x | 0.91x |
| Q7 | 3333.48 ms | 1534.35 ms | 1522.14 ms | 2.19x | 1.01x |
| Q8 | 3816.23 ms | 2006.30 ms | 1978.64 ms | 1.93x | 1.01x |
| Q9 | 2981.66 ms | 1279.27 ms | 1245.59 ms | 2.39x | 1.03x |
| Q10 | 2620.31 ms | 1706.30 ms | 1712.38 ms | 1.53x | 1.00x |
| Q11 | 1568.13 ms | 1293.11 ms | 1271.29 ms | 1.23x | 1.02x |
| Q12 | 2637.63 ms | 1763.15 ms | 1690.82 ms | 1.56x | 1.04x |
| Q13 | 900.48 ms | 639.89 ms | 709.32 ms | 1.27x | 0.90x |
| Q14 | 1471.44 ms | 1124.83 ms | 1113.10 ms | 1.32x | 1.01x |
| Q15 | 3111.08 ms | 1730.41 ms | 1706.49 ms | 1.82x | 1.01x |
| Q16 | 982.33 ms | 775.29 ms | 792.50 ms | 1.24x | 0.98x |
| Q17 | 2593.53 ms | 933.30 ms | 931.06 ms | 2.79x | 1.00x |
| Q18 | 2543.13 ms | 1130.98 ms | 1138.17 ms | 2.23x | 0.99x |
| Q19 | 2880.48 ms | 1748.10 ms | 1726.46 ms | 1.67x | 1.01x |
| Q20 | 2851.58 ms | 1599.39 ms | 1614.30 ms | 1.77x | 0.99x |
| Q21 | 5323.19 ms | 2724.60 ms | 2685.98 ms | 1.98x | 1.01x |
| Q22 | 972.12 ms | 791.61 ms | 814.17 ms | 1.19x | 0.97x |

### ClickBench

| Query | Off | 20 MiB | 100 MiB | 100 MiB vs off | 20 MiB vs 100 MiB |
|---:|---:|---:|---:|---:|---:|
| Q0 | 56.55 ms | 58.40 ms | 58.06 ms | 0.97x | 1.01x |
| Q1 | 327.30 ms | 333.22 ms | 298.80 ms | 1.10x | 1.12x |
| Q2 | 246.76 ms | 198.63 ms | 199.30 ms | 1.24x | 1.00x |
| Q3 | 245.16 ms | 195.28 ms | 192.92 ms | 1.27x | 1.01x |
| Q4 | 287.64 ms | 199.63 ms | 202.73 ms | 1.42x | 0.98x |
| Q5 | 249.47 ms | 239.43 ms | 229.65 ms | 1.09x | 1.04x |
| Q6 | 126.97 ms | 113.46 ms | 115.39 ms | 1.10x | 0.98x |
| Q7 | 256.87 ms | 149.37 ms | 145.64 ms | 1.76x | 1.03x |
| Q8 | 319.22 ms | 269.40 ms | 261.48 ms | 1.22x | 1.03x |
| Q9 | 259.60 ms | 186.71 ms | 194.32 ms | 1.34x | 0.96x |
| Q10 | 392.25 ms | 347.71 ms | 356.45 ms | 1.10x | 0.98x |
| Q11 | 461.70 ms | 325.25 ms | 329.97 ms | 1.40x | 0.99x |
| Q12 | 246.07 ms | 219.90 ms | 233.57 ms | 1.05x | 0.94x |
| Q13 | 458.07 ms | 335.17 ms | 350.69 ms | 1.31x | 0.96x |
| Q14 | 420.26 ms | 330.00 ms | 346.03 ms | 1.21x | 0.95x |
| Q15 | 297.14 ms | 184.97 ms | 200.31 ms | 1.48x | 0.92x |
| Q16 | 268.01 ms | 250.93 ms | 265.21 ms | 1.01x | 0.95x |
| Q17 | 355.99 ms | 243.09 ms | 245.71 ms | 1.45x | 0.99x |
| Q18 | 305.64 ms | 242.55 ms | 241.95 ms | 1.26x | 1.00x |
| Q19 | 213.95 ms | 212.82 ms | 218.51 ms | 0.98x | 0.97x |
| Q20 | 302.75 ms | 280.37 ms | 308.19 ms | 0.98x | 0.91x |
| Q21 | 451.41 ms | 357.93 ms | 374.16 ms | 1.21x | 0.96x |
| Q22 | 519.06 ms | 496.29 ms | 471.86 ms | 1.10x | 1.05x |
| Q23 | 589.59 ms | 570.53 ms | 647.52 ms | 0.91x | 0.88x |
| Q24 | 308.38 ms | 249.42 ms | 300.97 ms | 1.02x | 0.83x |
| Q25 | 293.79 ms | 210.64 ms | 230.01 ms | 1.28x | 0.92x |
| Q26 | 471.43 ms | 325.42 ms | 263.11 ms | 1.79x | 1.24x |
| Q27 | 506.67 ms | 377.73 ms | 389.95 ms | 1.30x | 0.97x |
| Q28 | 502.78 ms | 382.46 ms | 389.03 ms | 1.29x | 0.98x |
| Q29 | 301.80 ms | 224.83 ms | 233.71 ms | 1.29x | 0.96x |
| Q30 | 393.39 ms | 316.58 ms | 396.90 ms | 0.99x | 0.80x |
| Q31 | 458.32 ms | 395.64 ms | 344.35 ms | 1.33x | 1.15x |
| Q32 | 294.24 ms | 218.91 ms | 190.84 ms | 1.54x | 1.15x |
| Q33 | 326.62 ms | 265.16 ms | 304.40 ms | 1.07x | 0.87x |
| Q34 | 378.21 ms | 275.89 ms | 245.02 ms | 1.54x | 1.13x |
| Q35 | 262.65 ms | 220.87 ms | 238.78 ms | 1.10x | 0.92x |
| Q36 | 909.99 ms | 856.96 ms | 849.44 ms | 1.07x | 1.01x |
| Q37 | 916.85 ms | 818.67 ms | 829.68 ms | 1.11x | 0.99x |
| Q38 | 1083.95 ms | 955.38 ms | 1006.91 ms | 1.08x | 0.95x |
| Q39 | 756.59 ms | 842.90 ms | 709.13 ms | 1.07x | 1.19x |
| Q40 | 1069.77 ms | 837.90 ms | 869.94 ms | 1.23x | 0.96x |
| Q41 | 1070.80 ms | 990.05 ms | 1015.94 ms | 1.05x | 0.97x |
| Q42 | 889.26 ms | 810.45 ms | 804.20 ms | 1.11x | 1.01x |
