
## clickbench_1 results:

┏━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Query        ┃      base ┃ base-rerun ┃ Change ┃  HASHAGG0 ┃ Change ┃ HASHAGG32 ┃ Change ┃ HASHAGG256 ┃ Change ┃ HASHAGG1024 ┃ Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━┩
│ QQuery 0     │    0.27ms │     0.28ms │    +6% │    0.28ms │    +5% │    0.28ms │    +4% │     0.29ms │    +7% │      0.27ms │      ~ │
│ QQuery 1     │   31.19ms │    32.61ms │    +4% │   33.92ms │    +8% │   32.68ms │    +4% │    31.75ms │      ~ │     32.34ms │      ~ │
│ QQuery 2     │   51.36ms │    49.41ms │      ~ │   51.66ms │      ~ │   51.10ms │      ~ │    51.77ms │      ~ │     51.03ms │      ~ │
│ QQuery 3     │   64.74ms │    67.49ms │    +4% │   67.43ms │    +4% │   64.27ms │      ~ │    64.47ms │      ~ │     63.16ms │      ~ │
│ QQuery 4     │  409.63ms │   407.33ms │      ~ │  421.82ms │      ~ │  400.91ms │      ~ │   416.56ms │      ~ │    438.73ms │    +7% │
│ QQuery 5     │  461.31ms │   456.98ms │      ~ │  463.26ms │      ~ │  472.63ms │      ~ │   472.77ms │      ~ │    470.03ms │      ~ │
│ QQuery 6     │   29.89ms │    32.07ms │    +7% │   33.08ms │   +10% │   31.21ms │    +4% │    31.09ms │    +4% │     29.52ms │      ~ │
│ QQuery 7     │   34.04ms │    35.41ms │    +4% │   35.62ms │    +4% │   37.16ms │    +9% │    35.18ms │      ~ │     34.66ms │      ~ │
│ QQuery 8     │  494.60ms │   493.23ms │      ~ │  490.83ms │      ~ │  479.49ms │      ~ │   502.85ms │      ~ │    510.02ms │      ~ │
│ QQuery 9     │  508.37ms │   520.18ms │      ~ │  500.33ms │      ~ │  530.49ms │    +4% │   523.04ms │      ~ │    517.80ms │      ~ │
│ QQuery 10    │  128.61ms │   131.21ms │      ~ │  127.79ms │      ~ │  139.73ms │    +8% │   129.58ms │      ~ │    129.23ms │      ~ │
│ QQuery 11    │  140.51ms │   148.69ms │    +5% │  145.57ms │      ~ │  143.28ms │      ~ │   139.91ms │      ~ │    145.31ms │      ~ │
│ QQuery 12    │  495.69ms │   482.68ms │      ~ │  497.86ms │      ~ │  500.66ms │      ~ │   501.95ms │      ~ │    497.07ms │      ~ │
│ QQuery 13    │  767.73ms │   766.02ms │      ~ │  774.43ms │      ~ │  769.23ms │      ~ │   791.70ms │      ~ │    783.01ms │      ~ │
│ QQuery 14    │  470.49ms │   458.54ms │      ~ │  477.70ms │      ~ │  479.52ms │      ~ │   486.09ms │      ~ │    492.74ms │    +4% │
│ QQuery 15    │  482.71ms │   477.44ms │      ~ │  487.63ms │      ~ │  487.11ms │      ~ │   505.11ms │    +4% │    497.94ms │      ~ │
│ QQuery 16    │  943.89ms │   936.83ms │      ~ │  939.66ms │      ~ │  947.74ms │      ~ │   944.87ms │      ~ │    966.60ms │      ~ │
│ QQuery 17    │  907.91ms │   930.29ms │      ~ │  913.00ms │      ~ │  916.88ms │      ~ │   915.44ms │      ~ │    930.51ms │      ~ │
│ QQuery 18    │ 1836.69ms │  1870.40ms │      ~ │ 1771.92ms │      ~ │ 1777.57ms │      ~ │  1789.17ms │      ~ │   1793.51ms │      ~ │
│ QQuery 19    │   64.63ms │    58.05ms │   -10% │   57.72ms │   -10% │   59.63ms │    -7% │    56.80ms │   -12% │     57.51ms │   -11% │
│ QQuery 20    │  549.49ms │   525.23ms │    -4% │  517.85ms │    -5% │  526.70ms │    -4% │   523.41ms │    -4% │    533.24ms │      ~ │
│ QQuery 21    │  660.70ms │   669.35ms │      ~ │  674.51ms │      ~ │  692.57ms │    +4% │   673.67ms │      ~ │    664.04ms │      ~ │
│ QQuery 22    │ 1715.50ms │  1730.73ms │      ~ │ 1750.51ms │      ~ │ 1748.13ms │      ~ │  1794.39ms │    +4% │   1735.55ms │      ~ │
│ QQuery 23    │ 6471.15ms │  6468.85ms │      ~ │ 6472.26ms │      ~ │ 6448.36ms │      ~ │  6526.21ms │      ~ │   6436.59ms │      ~ │
│ QQuery 24    │  280.70ms │   290.26ms │      ~ │  283.30ms │      ~ │  281.20ms │      ~ │   284.59ms │      ~ │    278.74ms │      ~ │
│ QQuery 25    │  257.45ms │   248.51ms │      ~ │  235.78ms │    -8% │  252.03ms │      ~ │   249.72ms │      ~ │    255.09ms │      ~ │
│ QQuery 26    │  306.45ms │   314.66ms │      ~ │  311.52ms │      ~ │  317.62ms │      ~ │   306.57ms │      ~ │    309.00ms │      ~ │
│ QQuery 27    │  692.88ms │   748.21ms │    +7% │  714.70ms │      ~ │  695.32ms │      ~ │   706.12ms │      ~ │    694.18ms │      ~ │
│ QQuery 28    │ 4826.40ms │  4866.34ms │      ~ │ 4816.79ms │      ~ │ 4921.92ms │      ~ │  4986.14ms │      ~ │   4849.35ms │      ~ │
│ QQuery 29    │  215.60ms │   212.96ms │      ~ │  217.68ms │      ~ │  207.21ms │      ~ │   229.35ms │    +6% │    217.72ms │      ~ │
│ QQuery 30    │  461.25ms │   457.22ms │      ~ │  469.40ms │      ~ │  462.56ms │      ~ │   462.76ms │      ~ │    477.93ms │      ~ │
│ QQuery 31    │  553.02ms │   556.01ms │      ~ │  557.06ms │      ~ │  552.24ms │      ~ │   562.06ms │      ~ │    551.76ms │      ~ │
│ QQuery 32    │ 1751.35ms │  1765.71ms │      ~ │ 1779.03ms │      ~ │ 1765.37ms │      ~ │  1779.60ms │      ~ │   1756.35ms │      ~ │
│ QQuery 33    │ 1860.68ms │  1868.21ms │      ~ │ 1830.39ms │      ~ │ 1815.34ms │      ~ │  1843.46ms │      ~ │   1823.71ms │      ~ │
│ QQuery 34    │ 1852.96ms │  1866.21ms │      ~ │ 1856.65ms │      ~ │ 1848.38ms │      ~ │  1890.71ms │      ~ │   1854.66ms │      ~ │
│ QQuery 35    │  626.89ms │   599.60ms │    -4% │  608.11ms │      ~ │  620.78ms │      ~ │   614.45ms │      ~ │    628.31ms │      ~ │
│ QQuery 36    │  129.51ms │   140.12ms │    +8% │  130.78ms │      ~ │  137.67ms │    +6% │   131.43ms │      ~ │    131.73ms │      ~ │
│ QQuery 37    │   88.59ms │    87.72ms │      ~ │   88.63ms │      ~ │   89.35ms │      ~ │    88.96ms │      ~ │     91.12ms │      ~ │
│ QQuery 38    │   89.21ms │    90.56ms │      ~ │   87.94ms │      ~ │   92.07ms │      ~ │    93.41ms │    +4% │     92.07ms │      ~ │
│ QQuery 39    │  222.97ms │   236.90ms │    +6% │  234.69ms │    +5% │  229.34ms │      ~ │   237.21ms │    +6% │    230.33ms │      ~ │
│ QQuery 40    │   38.28ms │    38.54ms │      ~ │   38.24ms │      ~ │   38.59ms │      ~ │    39.02ms │      ~ │     39.12ms │      ~ │
│ QQuery 41    │   37.19ms │    36.20ms │      ~ │   35.07ms │    -5% │   35.71ms │      ~ │    35.58ms │    -4% │     37.04ms │      ~ │
│ QQuery 42    │   42.33ms │    40.34ms │    -4% │   43.91ms │      ~ │   44.35ms │    +4% │    46.88ms │   +10% │     41.25ms │      ~ │
└──────────────┴───────────┴────────────┴────────┴───────────┴────────┴───────────┴────────┴────────────┴────────┴─────────────┴────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Benchmark Summary          ┃            ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ Total Time (base)          │ 32054.83ms │
│ Total Time (base-rerun)    │ 32213.60ms │
│ Total Time (HASHAGG0)      │ 32046.35ms │
│ Total Time (HASHAGG32)     │ 32144.36ms │
│ Total Time (HASHAGG256)    │ 32496.05ms │
│ Total Time (HASHAGG1024)   │ 32169.91ms │
│ Average Time (base)        │   745.46ms │
│ Average Time (base-rerun)  │   749.15ms │
│ Queries Faster             │          4 │
│ Queries Slower             │          9 │
│ Queries with No Change     │         30 │
│ Average Time (HASHAGG0)    │   745.26ms │
│ Queries Faster             │          4 │
│ Queries Slower             │          6 │
│ Queries with No Change     │         33 │
│ Average Time (HASHAGG32)   │   747.54ms │
│ Queries Faster             │          2 │
│ Queries Slower             │          9 │
│ Queries with No Change     │         32 │
│ Average Time (HASHAGG256)  │   755.72ms │
│ Queries Faster             │          3 │
│ Queries Slower             │          8 │
│ Queries with No Change     │         32 │
│ Average Time (HASHAGG1024) │   748.14ms │
│ Queries Faster             │          1 │
│ Queries Slower             │          2 │
│ Queries with No Change     │         40 │
└────────────────────────────┴────────────┘

## clickbench_partitioned results:

┏━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Query        ┃      base ┃ base-rerun ┃ Change ┃  HASHAGG0 ┃ Change ┃ HASHAGG32 ┃ Change ┃ HASHAGG256 ┃ Change ┃ HASHAGG1024 ┃ Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━┩
│ QQuery 0     │    1.41ms │     1.39ms │      ~ │    1.42ms │      ~ │    1.42ms │      ~ │     1.39ms │      ~ │      1.43ms │      ~ │
│ QQuery 1     │   16.66ms │    16.27ms │      ~ │   16.15ms │      ~ │   16.30ms │      ~ │    15.93ms │    -4% │     15.70ms │    -5% │
│ QQuery 2     │   39.83ms │    38.24ms │      ~ │   37.55ms │    -5% │   38.47ms │      ~ │    40.08ms │      ~ │     40.71ms │      ~ │
│ QQuery 3     │   48.23ms │    48.86ms │      ~ │   48.26ms │      ~ │   46.23ms │    -4% │    50.55ms │    +4% │     48.63ms │      ~ │
│ QQuery 4     │  389.42ms │   382.19ms │      ~ │  373.68ms │    -4% │  401.72ms │      ~ │   388.55ms │      ~ │    390.97ms │      ~ │
│ QQuery 5     │  409.93ms │   413.22ms │      ~ │  413.39ms │      ~ │  426.99ms │    +4% │   420.50ms │      ~ │    409.10ms │      ~ │
│ QQuery 6     │   15.53ms │    14.83ms │    -4% │   15.96ms │      ~ │   15.84ms │      ~ │    16.58ms │    +6% │     14.83ms │    -4% │
│ QQuery 7     │   18.23ms │    18.25ms │      ~ │   18.70ms │      ~ │   18.31ms │      ~ │    18.44ms │      ~ │     19.35ms │    +6% │
│ QQuery 8     │  464.55ms │   458.17ms │      ~ │  460.19ms │      ~ │  464.57ms │      ~ │   461.06ms │      ~ │    468.11ms │      ~ │
│ QQuery 9     │  493.73ms │   484.49ms │      ~ │  494.93ms │      ~ │  491.58ms │      ~ │   484.72ms │      ~ │    490.85ms │      ~ │
│ QQuery 10    │  123.75ms │   115.46ms │    -6% │  115.96ms │    -6% │  119.16ms │      ~ │   119.78ms │      ~ │    115.22ms │    -6% │
│ QQuery 11    │  127.30ms │   132.39ms │      ~ │  131.23ms │      ~ │  133.28ms │    +4% │   131.45ms │      ~ │    132.21ms │      ~ │
│ QQuery 12    │  447.91ms │   447.96ms │      ~ │  464.37ms │      ~ │  444.70ms │      ~ │   454.14ms │      ~ │    448.65ms │      ~ │
│ QQuery 13    │  674.57ms │   662.34ms │      ~ │  673.01ms │      ~ │  672.60ms │      ~ │   691.69ms │      ~ │    684.62ms │      ~ │
│ QQuery 14    │  430.16ms │   430.82ms │      ~ │  434.79ms │      ~ │  421.05ms │      ~ │   427.57ms │      ~ │    438.23ms │      ~ │
│ QQuery 15    │  460.95ms │   475.36ms │      ~ │  462.06ms │      ~ │  461.32ms │      ~ │   462.85ms │      ~ │    453.28ms │      ~ │
│ QQuery 16    │  919.83ms │   908.13ms │      ~ │  902.23ms │      ~ │  924.49ms │      ~ │   899.22ms │      ~ │    909.20ms │      ~ │
│ QQuery 17    │  877.38ms │   886.52ms │      ~ │  880.74ms │      ~ │  886.99ms │      ~ │   880.20ms │      ~ │    902.43ms │      ~ │
│ QQuery 18    │ 1778.87ms │  1768.27ms │      ~ │ 1784.00ms │      ~ │ 1831.23ms │      ~ │  1828.95ms │      ~ │   1794.52ms │      ~ │
│ QQuery 19    │   41.17ms │    41.06ms │      ~ │   43.22ms │    +4% │   43.20ms │    +4% │    44.96ms │    +9% │     43.17ms │    +4% │
│ QQuery 20    │  457.37ms │   464.49ms │      ~ │  466.98ms │      ~ │  481.34ms │    +5% │   478.85ms │    +4% │    462.04ms │      ~ │
│ QQuery 21    │  553.79ms │   544.90ms │      ~ │  536.98ms │      ~ │  558.54ms │      ~ │   540.78ms │      ~ │    537.61ms │      ~ │
│ QQuery 22    │ 1139.66ms │  1101.74ms │      ~ │ 1114.11ms │      ~ │ 1102.37ms │      ~ │  1074.46ms │    -5% │   1086.69ms │    -4% │
│ QQuery 23    │ 6209.71ms │  6124.43ms │      ~ │ 6158.19ms │      ~ │ 6191.10ms │      ~ │  6130.16ms │      ~ │   6130.70ms │      ~ │
│ QQuery 24    │  212.72ms │   215.80ms │      ~ │  208.48ms │      ~ │  217.56ms │      ~ │   224.21ms │    +5% │    212.47ms │      ~ │
│ QQuery 25    │  174.14ms │   178.51ms │      ~ │  154.33ms │   -11% │  162.65ms │    -6% │   164.15ms │    -5% │    160.52ms │    -7% │
│ QQuery 26    │  239.74ms │   244.47ms │      ~ │  236.90ms │      ~ │  239.09ms │      ~ │   243.29ms │      ~ │    238.87ms │      ~ │
│ QQuery 27    │  621.53ms │   648.46ms │    +4% │  618.49ms │      ~ │  631.58ms │      ~ │   627.13ms │      ~ │    622.19ms │      ~ │
│ QQuery 28    │ 4517.84ms │  4569.81ms │      ~ │ 4610.92ms │      ~ │ 4577.19ms │      ~ │  4582.45ms │      ~ │   4671.47ms │      ~ │
│ QQuery 29    │  200.48ms │   201.42ms │      ~ │  196.43ms │      ~ │  194.77ms │      ~ │   187.06ms │    -6% │    192.61ms │      ~ │
│ QQuery 30    │  404.06ms │   397.24ms │      ~ │  406.89ms │      ~ │  402.90ms │      ~ │   408.52ms │      ~ │    406.43ms │      ~ │
│ QQuery 31    │  508.28ms │   515.77ms │      ~ │  513.07ms │      ~ │  500.25ms │      ~ │   511.35ms │      ~ │    506.23ms │      ~ │
│ QQuery 32    │ 1795.68ms │  1796.02ms │      ~ │ 1782.62ms │      ~ │ 1765.08ms │      ~ │  1748.92ms │      ~ │   1789.79ms │      ~ │
│ QQuery 33    │ 1812.84ms │  1797.98ms │      ~ │ 1800.54ms │      ~ │ 1788.04ms │      ~ │  1791.60ms │      ~ │   1764.79ms │      ~ │
│ QQuery 34    │ 1896.58ms │  1820.08ms │    -4% │ 1814.14ms │    -4% │ 1781.56ms │    -6% │  1821.72ms │      ~ │   1781.69ms │    -6% │
│ QQuery 35    │  614.27ms │   601.42ms │      ~ │  603.48ms │      ~ │  600.88ms │      ~ │   613.94ms │      ~ │    612.81ms │      ~ │
│ QQuery 36    │   97.03ms │   101.78ms │    +4% │  107.14ms │   +10% │  106.33ms │    +9% │   107.17ms │   +10% │    106.66ms │    +9% │
│ QQuery 37    │   44.10ms │    42.71ms │      ~ │   44.06ms │      ~ │   43.48ms │      ~ │    43.89ms │      ~ │     43.28ms │      ~ │
│ QQuery 38    │   60.98ms │    61.00ms │      ~ │   61.84ms │      ~ │   61.69ms │      ~ │    60.23ms │      ~ │     60.98ms │      ~ │
│ QQuery 39    │  206.42ms │   199.44ms │      ~ │  187.75ms │    -9% │  199.52ms │      ~ │   196.73ms │    -4% │    191.91ms │    -7% │
│ QQuery 40    │   22.86ms │    24.80ms │    +8% │   24.59ms │    +7% │   24.66ms │    +7% │    24.20ms │    +5% │     25.14ms │    +9% │
│ QQuery 41    │   22.30ms │    22.08ms │      ~ │   21.70ms │      ~ │   21.42ms │      ~ │    24.24ms │    +8% │     21.50ms │      ~ │
│ QQuery 42    │   26.82ms │    29.02ms │    +8% │   29.44ms │    +9% │   28.76ms │    +7% │    28.43ms │    +5% │     28.53ms │    +6% │
└──────────────┴───────────┴────────────┴────────┴───────────┴────────┴───────────┴────────┴────────────┴────────┴─────────────┴────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Benchmark Summary          ┃            ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ Total Time (base)          │ 29618.65ms │
│ Total Time (base-rerun)    │ 29447.61ms │
│ Total Time (HASHAGG0)      │ 29470.93ms │
│ Total Time (HASHAGG32)     │ 29540.20ms │
│ Total Time (HASHAGG256)    │ 29472.10ms │
│ Total Time (HASHAGG1024)   │ 29476.11ms │
│ Average Time (base)        │   688.81ms │
│ Average Time (base-rerun)  │   684.83ms │
│ Queries Faster             │          3 │
│ Queries Slower             │          4 │
│ Queries with No Change     │         36 │
│ Average Time (HASHAGG0)    │   685.37ms │
│ Queries Faster             │          6 │
│ Queries Slower             │          4 │
│ Queries with No Change     │         33 │
│ Average Time (HASHAGG32)   │   686.98ms │
│ Queries Faster             │          3 │
│ Queries Slower             │          7 │
│ Queries with No Change     │         33 │
│ Average Time (HASHAGG256)  │   685.40ms │
│ Queries Faster             │          5 │
│ Queries Slower             │          9 │
│ Queries with No Change     │         29 │
│ Average Time (HASHAGG1024) │   685.49ms │
│ Queries Faster             │          7 │
│ Queries Slower             │          5 │
│ Queries with No Change     │         31 │
└────────────────────────────┴────────────┘

## clickbench_extended results:

┏━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Query        ┃      base ┃ base-rerun ┃ Change ┃  HASHAGG0 ┃ Change ┃ HASHAGG32 ┃ Change ┃ HASHAGG256 ┃ Change ┃ HASHAGG1024 ┃ Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━┩
│ QQuery 0     │  919.34ms │   858.85ms │    -6% │  855.00ms │    -6% │  868.53ms │    -5% │   863.30ms │    -6% │    874.25ms │    -4% │
│ QQuery 1     │  240.76ms │   245.19ms │      ~ │  256.27ms │    +6% │  235.20ms │      ~ │   248.94ms │      ~ │    234.98ms │      ~ │
│ QQuery 2     │  445.67ms │   459.20ms │      ~ │  471.59ms │    +5% │  461.19ms │      ~ │   465.82ms │    +4% │    491.51ms │   +10% │
│ QQuery 3     │  253.06ms │   252.94ms │      ~ │  257.25ms │      ~ │  270.69ms │    +6% │   246.76ms │      ~ │    257.07ms │      ~ │
│ QQuery 4     │  887.57ms │   904.68ms │      ~ │  892.31ms │      ~ │  897.75ms │      ~ │   918.72ms │      ~ │    898.94ms │      ~ │
│ QQuery 5     │ 8249.61ms │  8189.48ms │      ~ │ 8321.68ms │      ~ │ 8233.07ms │      ~ │  8226.32ms │      ~ │   8207.45ms │      ~ │
└──────────────┴───────────┴────────────┴────────┴───────────┴────────┴───────────┴────────┴────────────┴────────┴─────────────┴────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Benchmark Summary          ┃            ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ Total Time (base)          │ 10996.01ms │
│ Total Time (base-rerun)    │ 10910.33ms │
│ Total Time (HASHAGG0)      │ 11054.10ms │
│ Total Time (HASHAGG32)     │ 10966.43ms │
│ Total Time (HASHAGG256)    │ 10969.86ms │
│ Total Time (HASHAGG1024)   │ 10964.19ms │
│ Average Time (base)        │  1832.67ms │
│ Average Time (base-rerun)  │  1818.39ms │
│ Queries Faster             │          1 │
│ Queries Slower             │          0 │
│ Queries with No Change     │          5 │
│ Average Time (HASHAGG0)    │  1842.35ms │
│ Queries Faster             │          1 │
│ Queries Slower             │          2 │
│ Queries with No Change     │          3 │
│ Average Time (HASHAGG32)   │  1827.74ms │
│ Queries Faster             │          1 │
│ Queries Slower             │          1 │
│ Queries with No Change     │          4 │
│ Average Time (HASHAGG256)  │  1828.31ms │
│ Queries Faster             │          1 │
│ Queries Slower             │          1 │
│ Queries with No Change     │          4 │
│ Average Time (HASHAGG1024) │  1827.36ms │
│ Queries Faster             │          1 │
│ Queries Slower             │          1 │
│ Queries with No Change     │          4 │
└────────────────────────────┴────────────┘

## tpch_sf1 results:

┏━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Query        ┃     base ┃ base-rerun ┃ Change ┃ HASHAGG0 ┃ Change ┃ HASHAGG32 ┃ Change ┃ HASHAGG256 ┃ Change ┃ HASHAGG1024 ┃ Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━┩
│ QQuery 1     │  73.03ms │    78.72ms │    +7% │  78.06ms │    +6% │   75.42ms │      ~ │    73.56ms │      ~ │     76.09ms │    +4% │
│ QQuery 2     │  22.70ms │    23.29ms │      ~ │  23.04ms │      ~ │   22.78ms │      ~ │    23.00ms │      ~ │     23.48ms │      ~ │
│ QQuery 3     │  30.18ms │    29.33ms │      ~ │  30.64ms │      ~ │   29.63ms │      ~ │    29.27ms │      ~ │     29.40ms │      ~ │
│ QQuery 4     │  28.50ms │    28.48ms │      ~ │  28.07ms │      ~ │   27.46ms │      ~ │    29.19ms │      ~ │     28.49ms │      ~ │
│ QQuery 5     │  51.25ms │    51.86ms │      ~ │  50.18ms │      ~ │   50.13ms │      ~ │    51.56ms │      ~ │     50.22ms │      ~ │
│ QQuery 6     │  14.50ms │    15.16ms │    +4% │  14.89ms │      ~ │   15.21ms │    +4% │    14.77ms │      ~ │     14.78ms │      ~ │
│ QQuery 7     │  79.08ms │    79.31ms │      ~ │  80.12ms │      ~ │   77.01ms │      ~ │    76.59ms │      ~ │     76.28ms │      ~ │
│ QQuery 8     │  43.12ms │    44.35ms │      ~ │  44.11ms │      ~ │   44.83ms │      ~ │    41.88ms │      ~ │     43.84ms │      ~ │
│ QQuery 9     │  62.03ms │    64.05ms │      ~ │  64.66ms │    +4% │   63.74ms │      ~ │    62.73ms │      ~ │     63.30ms │      ~ │
│ QQuery 10    │  49.65ms │    50.23ms │      ~ │  52.10ms │    +4% │   49.41ms │      ~ │    52.28ms │    +5% │     52.16ms │    +5% │
│ QQuery 11    │  16.97ms │    16.49ms │      ~ │  17.38ms │      ~ │   17.63ms │      ~ │    17.59ms │      ~ │     17.10ms │      ~ │
│ QQuery 12    │  27.86ms │    28.28ms │      ~ │  27.79ms │      ~ │   28.68ms │      ~ │    28.51ms │      ~ │     27.99ms │      ~ │
│ QQuery 13    │  30.88ms │    31.93ms │      ~ │  31.83ms │      ~ │   31.66ms │      ~ │    33.03ms │    +6% │     32.10ms │      ~ │
│ QQuery 14    │  23.35ms │    24.01ms │      ~ │  23.50ms │      ~ │   23.17ms │      ~ │    24.11ms │      ~ │     24.61ms │    +5% │
│ QQuery 15    │  33.71ms │    34.11ms │      ~ │  37.79ms │   +12% │   35.20ms │    +4% │    33.22ms │      ~ │     35.72ms │    +5% │
│ QQuery 16    │  14.99ms │    14.53ms │      ~ │  14.98ms │      ~ │   15.57ms │      ~ │    14.59ms │      ~ │     15.39ms │      ~ │
│ QQuery 17    │  64.29ms │    62.49ms │      ~ │  64.54ms │      ~ │   68.21ms │    +6% │    65.00ms │      ~ │     65.85ms │      ~ │
│ QQuery 18    │ 109.76ms │   110.60ms │      ~ │ 111.15ms │      ~ │  109.35ms │      ~ │   108.47ms │      ~ │    107.64ms │      ~ │
│ QQuery 19    │  43.47ms │    43.95ms │      ~ │  43.21ms │      ~ │   43.25ms │      ~ │    43.93ms │      ~ │     44.22ms │      ~ │
│ QQuery 20    │  34.84ms │    33.56ms │      ~ │  33.53ms │      ~ │   34.73ms │      ~ │    35.72ms │      ~ │     34.74ms │      ~ │
│ QQuery 21    │  92.47ms │    90.91ms │      ~ │  90.50ms │      ~ │  101.81ms │   +10% │    89.67ms │      ~ │     91.09ms │      ~ │
│ QQuery 22    │  19.00ms │    18.73ms │      ~ │  18.73ms │      ~ │   19.35ms │      ~ │    19.77ms │    +4% │     19.41ms │      ~ │
└──────────────┴──────────┴────────────┴────────┴──────────┴────────┴───────────┴────────┴────────────┴────────┴─────────────┴────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ Benchmark Summary          ┃          ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ Total Time (base)          │ 965.62ms │
│ Total Time (base-rerun)    │ 974.38ms │
│ Total Time (HASHAGG0)      │ 980.79ms │
│ Total Time (HASHAGG32)     │ 984.24ms │
│ Total Time (HASHAGG256)    │ 968.43ms │
│ Total Time (HASHAGG1024)   │ 973.88ms │
│ Average Time (base)        │  43.89ms │
│ Average Time (base-rerun)  │  44.29ms │
│ Queries Faster             │        0 │
│ Queries Slower             │        2 │
│ Queries with No Change     │       20 │
│ Average Time (HASHAGG0)    │  44.58ms │
│ Queries Faster             │        0 │
│ Queries Slower             │        4 │
│ Queries with No Change     │       18 │
│ Average Time (HASHAGG32)   │  44.74ms │
│ Queries Faster             │        0 │
│ Queries Slower             │        4 │
│ Queries with No Change     │       18 │
│ Average Time (HASHAGG256)  │  44.02ms │
│ Queries Faster             │        0 │
│ Queries Slower             │        3 │
│ Queries with No Change     │       19 │
│ Average Time (HASHAGG1024) │  44.27ms │
│ Queries Faster             │        0 │
│ Queries Slower             │        4 │
│ Queries with No Change     │       18 │
└────────────────────────────┴──────────┘

## tpch_sf10 results:

┏━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Query        ┃      base ┃ base-rerun ┃ Change ┃  HASHAGG0 ┃ Change ┃ HASHAGG32 ┃ Change ┃ HASHAGG256 ┃ Change ┃ HASHAGG1024 ┃ Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━┩
│ QQuery 1     │  533.93ms │   543.70ms │      ~ │  564.81ms │    +5% │  546.28ms │      ~ │   548.13ms │      ~ │    557.77ms │    +4% │
│ QQuery 2     │  153.11ms │   152.01ms │      ~ │  153.14ms │      ~ │  151.09ms │      ~ │   151.86ms │      ~ │    153.20ms │      ~ │
│ QQuery 3     │  309.11ms │   311.16ms │      ~ │  310.83ms │      ~ │  314.12ms │      ~ │   310.73ms │      ~ │    312.05ms │      ~ │
│ QQuery 4     │  236.14ms │   236.26ms │      ~ │  239.79ms │      ~ │  234.58ms │      ~ │   233.07ms │      ~ │    237.05ms │      ~ │
│ QQuery 5     │  570.31ms │   566.66ms │      ~ │  569.79ms │      ~ │  572.04ms │      ~ │   572.23ms │      ~ │    566.51ms │      ~ │
│ QQuery 6     │   92.47ms │    90.03ms │      ~ │   84.84ms │    -8% │   87.61ms │    -5% │    84.99ms │    -8% │     85.86ms │    -7% │
│ QQuery 7     │  900.36ms │   914.47ms │      ~ │  912.96ms │      ~ │  863.46ms │    -4% │   852.08ms │    -5% │    857.44ms │    -4% │
│ QQuery 8     │  528.16ms │   531.54ms │      ~ │  534.54ms │      ~ │  532.48ms │      ~ │   535.51ms │      ~ │    528.00ms │      ~ │
│ QQuery 9     │ 1011.72ms │  1013.89ms │      ~ │ 1015.26ms │      ~ │  996.81ms │      ~ │   998.65ms │      ~ │    990.55ms │      ~ │
│ QQuery 10    │  455.50ms │   440.61ms │      ~ │  452.30ms │      ~ │  445.14ms │      ~ │   453.53ms │      ~ │    441.35ms │      ~ │
│ QQuery 11    │  135.66ms │   134.01ms │      ~ │  134.86ms │      ~ │  136.02ms │      ~ │   134.58ms │      ~ │    136.07ms │      ~ │
│ QQuery 12    │  182.91ms │   183.39ms │      ~ │  179.87ms │      ~ │  186.28ms │      ~ │   186.18ms │      ~ │    184.96ms │      ~ │
│ QQuery 13    │  388.11ms │   359.03ms │    -7% │  397.30ms │      ~ │  369.27ms │    -4% │   384.89ms │      ~ │    360.53ms │    -7% │
│ QQuery 14    │  146.85ms │   150.79ms │      ~ │  151.84ms │      ~ │  148.21ms │      ~ │   150.82ms │      ~ │    147.30ms │      ~ │
│ QQuery 15    │  224.77ms │   224.99ms │      ~ │  232.75ms │      ~ │  225.28ms │      ~ │   229.89ms │      ~ │    231.57ms │      ~ │
│ QQuery 16    │   97.17ms │    98.08ms │      ~ │   97.59ms │      ~ │   98.66ms │      ~ │    97.26ms │      ~ │     97.86ms │      ~ │
│ QQuery 17    │  808.27ms │   806.36ms │      ~ │  802.42ms │      ~ │  801.90ms │      ~ │   812.49ms │      ~ │    804.36ms │      ~ │
│ QQuery 18    │ 1360.86ms │  1351.04ms │      ~ │ 1382.98ms │      ~ │ 1392.76ms │      ~ │  1362.57ms │      ~ │   1329.74ms │      ~ │
│ QQuery 19    │  234.19ms │   246.01ms │    +5% │  236.43ms │      ~ │  240.65ms │      ~ │   237.62ms │      ~ │    238.65ms │      ~ │
│ QQuery 20    │  277.69ms │   270.64ms │      ~ │  280.74ms │      ~ │  284.06ms │      ~ │   278.69ms │      ~ │    277.92ms │      ~ │
│ QQuery 21    │ 1113.94ms │  1115.46ms │      ~ │ 1118.36ms │      ~ │ 1178.63ms │    +5% │  1117.09ms │      ~ │   1117.31ms │      ~ │
│ QQuery 22    │   87.45ms │    85.63ms │      ~ │   88.04ms │      ~ │   88.26ms │      ~ │    89.61ms │      ~ │     87.46ms │      ~ │
└──────────────┴───────────┴────────────┴────────┴───────────┴────────┴───────────┴────────┴────────────┴────────┴─────────────┴────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ Benchmark Summary          ┃           ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ Total Time (base)          │ 9848.66ms │
│ Total Time (base-rerun)    │ 9825.76ms │
│ Total Time (HASHAGG0)      │ 9941.45ms │
│ Total Time (HASHAGG32)     │ 9893.59ms │
│ Total Time (HASHAGG256)    │ 9822.47ms │
│ Total Time (HASHAGG1024)   │ 9743.51ms │
│ Average Time (base)        │  447.67ms │
│ Average Time (base-rerun)  │  446.63ms │
│ Queries Faster             │         1 │
│ Queries Slower             │         1 │
│ Queries with No Change     │        20 │
│ Average Time (HASHAGG0)    │  451.88ms │
│ Queries Faster             │         1 │
│ Queries Slower             │         1 │
│ Queries with No Change     │        20 │
│ Average Time (HASHAGG32)   │  449.71ms │
│ Queries Faster             │         3 │
│ Queries Slower             │         1 │
│ Queries with No Change     │        18 │
│ Average Time (HASHAGG256)  │  446.48ms │
│ Queries Faster             │         2 │
│ Queries Slower             │         0 │
│ Queries with No Change     │        20 │
│ Average Time (HASHAGG1024) │  442.89ms │
│ Queries Faster             │         3 │
│ Queries Slower             │         1 │
│ Queries with No Change     │        18 │
└────────────────────────────┴───────────┘

## tpch_mem_sf1 results:

┏━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Query        ┃     base ┃ base-rerun ┃ Change ┃ HASHAGG0 ┃ Change ┃ HASHAGG32 ┃ Change ┃ HASHAGG256 ┃ Change ┃ HASHAGG1024 ┃ Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━┩
│ QQuery 1     │  56.34ms │    59.29ms │    +5% │  59.07ms │    +4% │   58.89ms │    +4% │    61.83ms │    +9% │     61.61ms │    +9% │
│ QQuery 2     │  12.40ms │    12.50ms │      ~ │  12.35ms │      ~ │   12.27ms │      ~ │    13.03ms │    +5% │     12.48ms │      ~ │
│ QQuery 3     │  27.76ms │    27.50ms │      ~ │  27.88ms │      ~ │   27.77ms │      ~ │    27.49ms │      ~ │     27.34ms │      ~ │
│ QQuery 4     │   9.67ms │     9.48ms │      ~ │   9.72ms │      ~ │    9.83ms │      ~ │     9.77ms │      ~ │      9.69ms │      ~ │
│ QQuery 5     │  35.95ms │    35.67ms │      ~ │  35.19ms │      ~ │   35.73ms │      ~ │    35.61ms │      ~ │     35.15ms │      ~ │
│ QQuery 6     │   5.97ms │     5.68ms │    -4% │   6.23ms │    +4% │    5.87ms │      ~ │     5.63ms │    -5% │      6.05ms │      ~ │
│ QQuery 7     │  79.05ms │    78.53ms │      ~ │  77.12ms │      ~ │   70.81ms │   -10% │    73.10ms │    -7% │     69.52ms │   -12% │
│ QQuery 8     │  12.91ms │    12.66ms │      ~ │  13.84ms │    +7% │   13.56ms │    +5% │    12.09ms │    -6% │     11.69ms │    -9% │
│ QQuery 9     │  34.59ms │    35.70ms │      ~ │  36.99ms │    +6% │   35.78ms │      ~ │    33.72ms │      ~ │     33.97ms │      ~ │
│ QQuery 10    │  35.29ms │    35.90ms │      ~ │  36.13ms │      ~ │   35.12ms │      ~ │    36.22ms │      ~ │     36.79ms │    +4% │
│ QQuery 11    │   6.10ms │     5.87ms │      ~ │   5.73ms │    -6% │    5.83ms │    -4% │     5.64ms │    -7% │      5.85ms │    -4% │
│ QQuery 12    │  17.25ms │    22.07ms │   +27% │  17.44ms │      ~ │   15.52ms │   -10% │    16.19ms │    -6% │     17.72ms │      ~ │
│ QQuery 13    │  12.83ms │    12.38ms │      ~ │  12.25ms │    -4% │   11.75ms │    -8% │    11.74ms │    -8% │     12.36ms │      ~ │
│ QQuery 14    │   7.82ms │     7.61ms │      ~ │   6.72ms │   -14% │    5.39ms │   -31% │     8.41ms │    +7% │      6.67ms │   -14% │
│ QQuery 15    │  10.85ms │    12.04ms │   +10% │  11.04ms │      ~ │   13.03ms │   +20% │    11.73ms │    +8% │     11.73ms │    +8% │
│ QQuery 16    │  15.19ms │    12.35ms │   -18% │  13.31ms │   -12% │   11.73ms │   -22% │    13.29ms │   -12% │     11.59ms │   -23% │
│ QQuery 17    │  45.52ms │    46.29ms │      ~ │  46.76ms │      ~ │   45.58ms │      ~ │    46.96ms │      ~ │     46.50ms │      ~ │
│ QQuery 18    │ 148.40ms │   148.77ms │      ~ │ 150.06ms │      ~ │  130.43ms │   -12% │   136.55ms │    -7% │    139.03ms │    -6% │
│ QQuery 19    │  19.62ms │    20.89ms │    +6% │  18.66ms │    -4% │   19.51ms │      ~ │    20.88ms │    +6% │     21.96ms │   +11% │
│ QQuery 20    │  21.97ms │    23.23ms │    +5% │  22.22ms │      ~ │   22.09ms │      ~ │    21.86ms │      ~ │     25.31ms │   +15% │
│ QQuery 21    │ 102.54ms │   101.28ms │      ~ │ 103.90ms │      ~ │   89.69ms │   -12% │    95.87ms │    -6% │     97.12ms │    -5% │
│ QQuery 22    │  17.19ms │    18.08ms │    +5% │  18.48ms │    +7% │   17.68ms │      ~ │    17.33ms │      ~ │     17.87ms │      ~ │
└──────────────┴──────────┴────────────┴────────┴──────────┴────────┴───────────┴────────┴────────────┴────────┴─────────────┴────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
┃ Benchmark Summary          ┃          ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
│ Total Time (base)          │ 735.22ms │
│ Total Time (base-rerun)    │ 743.76ms │
│ Total Time (HASHAGG0)      │ 741.10ms │
│ Total Time (HASHAGG32)     │ 693.85ms │
│ Total Time (HASHAGG256)    │ 714.96ms │
│ Total Time (HASHAGG1024)   │ 718.00ms │
│ Average Time (base)        │  33.42ms │
│ Average Time (base-rerun)  │  33.81ms │
│ Queries Faster             │        2 │
│ Queries Slower             │        6 │
│ Queries with No Change     │       14 │
│ Average Time (HASHAGG0)    │  33.69ms │
│ Queries Faster             │        5 │
│ Queries Slower             │        5 │
│ Queries with No Change     │       12 │
│ Average Time (HASHAGG32)   │  31.54ms │
│ Queries Faster             │        8 │
│ Queries Slower             │        3 │
│ Queries with No Change     │       11 │
│ Average Time (HASHAGG256)  │  32.50ms │
│ Queries Faster             │        9 │
│ Queries Slower             │        5 │
│ Queries with No Change     │        8 │
│ Average Time (HASHAGG1024) │  32.64ms │
│ Queries Faster             │        7 │
│ Queries Slower             │        5 │
│ Queries with No Change     │       10 │
└────────────────────────────┴──────────┘

## tpch_mem_sf10 results:

┏━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┓
┃ Query        ┃      base ┃ base-rerun ┃ Change ┃  HASHAGG0 ┃ Change ┃ HASHAGG32 ┃ Change ┃ HASHAGG256 ┃ Change ┃ HASHAGG1024 ┃ Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━┩
│ QQuery 1     │  628.05ms │   625.82ms │      ~ │  623.65ms │      ~ │  636.04ms │      ~ │   626.30ms │      ~ │    625.25ms │      ~ │
│ QQuery 2     │  111.67ms │   110.78ms │      ~ │  111.42ms │      ~ │  111.31ms │      ~ │   111.62ms │      ~ │    110.61ms │      ~ │
│ QQuery 3     │  265.51ms │   266.26ms │      ~ │  264.97ms │      ~ │  268.04ms │      ~ │   268.55ms │      ~ │    268.44ms │      ~ │
│ QQuery 4     │   78.36ms │    79.05ms │      ~ │   78.39ms │      ~ │   79.13ms │      ~ │    77.93ms │      ~ │     78.90ms │      ~ │
│ QQuery 5     │  438.22ms │   440.66ms │      ~ │  442.97ms │      ~ │  408.16ms │    -6% │   408.81ms │    -6% │    412.53ms │    -5% │
│ QQuery 6     │   49.25ms │    49.29ms │      ~ │   49.03ms │      ~ │   48.51ms │      ~ │    48.56ms │      ~ │     49.24ms │      ~ │
│ QQuery 7     │  910.22ms │   909.01ms │      ~ │  917.13ms │      ~ │  812.51ms │   -10% │   829.49ms │    -8% │    823.53ms │    -9% │
│ QQuery 8     │  344.64ms │   344.93ms │      ~ │  345.15ms │      ~ │  341.58ms │      ~ │   343.42ms │      ~ │    342.91ms │      ~ │
│ QQuery 9     │  781.32ms │   772.21ms │      ~ │  775.47ms │      ~ │  738.33ms │    -5% │   740.97ms │    -5% │    737.93ms │    -5% │
│ QQuery 10    │  356.04ms │   357.06ms │      ~ │  359.89ms │      ~ │  350.10ms │      ~ │   354.79ms │      ~ │    354.40ms │      ~ │
│ QQuery 11    │  103.69ms │   103.00ms │      ~ │  104.45ms │      ~ │  103.44ms │      ~ │   103.51ms │      ~ │    102.43ms │      ~ │
│ QQuery 12    │  161.29ms │   162.11ms │      ~ │  161.07ms │      ~ │  144.46ms │   -10% │   157.01ms │      ~ │    157.05ms │      ~ │
│ QQuery 13    │  187.74ms │   196.69ms │    +4% │  188.83ms │      ~ │  195.11ms │      ~ │   197.47ms │    +5% │    188.98ms │      ~ │
│ QQuery 14    │   42.26ms │    43.01ms │      ~ │   43.30ms │      ~ │   42.93ms │      ~ │    42.82ms │      ~ │     42.58ms │      ~ │
│ QQuery 15    │   89.82ms │    89.18ms │      ~ │   91.50ms │      ~ │   89.71ms │      ~ │    88.88ms │      ~ │     89.85ms │      ~ │
│ QQuery 16    │   84.03ms │    85.20ms │      ~ │   85.45ms │      ~ │   82.76ms │      ~ │    85.08ms │      ~ │     83.52ms │      ~ │
│ QQuery 17    │  708.59ms │   733.30ms │      ~ │  719.05ms │      ~ │  723.57ms │      ~ │   728.85ms │      ~ │    726.40ms │      ~ │
│ QQuery 18    │ 1821.97ms │  1806.55ms │      ~ │ 1842.20ms │      ~ │ 2435.01ms │   +33% │  1697.15ms │    -6% │   1738.31ms │    -4% │
│ QQuery 19    │  107.22ms │   107.08ms │      ~ │  106.73ms │      ~ │  107.86ms │      ~ │   109.88ms │      ~ │    108.67ms │      ~ │
│ QQuery 20    │  195.15ms │   193.83ms │      ~ │  195.46ms │      ~ │  198.39ms │      ~ │   193.45ms │      ~ │    199.52ms │      ~ │
│ QQuery 21    │ 1080.85ms │  1069.29ms │      ~ │ 1069.51ms │      ~ │ 1033.87ms │    -4% │  1003.16ms │    -7% │   1018.03ms │    -5% │
│ QQuery 22    │   69.45ms │    68.21ms │      ~ │   68.47ms │      ~ │   68.71ms │      ~ │    68.47ms │      ~ │     68.64ms │      ~ │
└──────────────┴───────────┴────────────┴────────┴───────────┴────────┴───────────┴────────┴────────────┴────────┴─────────────┴────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ Benchmark Summary          ┃           ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ Total Time (base)          │ 8615.35ms │
│ Total Time (base-rerun)    │ 8612.52ms │
│ Total Time (HASHAGG0)      │ 8644.11ms │
│ Total Time (HASHAGG32)     │ 9019.52ms │
│ Total Time (HASHAGG256)    │ 8286.15ms │
│ Total Time (HASHAGG1024)   │ 8327.71ms │
│ Average Time (base)        │  391.61ms │
│ Average Time (base-rerun)  │  391.48ms │
│ Queries Faster             │         0 │
│ Queries Slower             │         1 │
│ Queries with No Change     │        21 │
│ Average Time (HASHAGG0)    │  392.91ms │
│ Queries Faster             │         0 │
│ Queries Slower             │         0 │
│ Queries with No Change     │        22 │
│ Average Time (HASHAGG32)   │  409.98ms │
│ Queries Faster             │         5 │
│ Queries Slower             │         1 │
│ Queries with No Change     │        16 │
│ Average Time (HASHAGG256)  │  376.64ms │
│ Queries Faster             │         5 │
│ Queries Slower             │         1 │
│ Queries with No Change     │        16 │
│ Average Time (HASHAGG1024) │  378.53ms │
│ Queries Faster             │         5 │
│ Queries Slower             │         0 │
│ Queries with No Change     │        17 │
└────────────────────────────┴───────────┘
