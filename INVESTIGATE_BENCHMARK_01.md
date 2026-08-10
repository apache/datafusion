v1 - https://github.com/apache/datafusion/pull/21566#issuecomment-5018685664 
v2 - https://github.com/apache/datafusion/pull/21566#issuecomment-5214735328

comparing v1 → v2, the big story is **substantially better memory usage**, while the branch’s previous runtime advantage mostly disappears.

| Metric      |  v1 Base | v1 Branch | v2 Base | v2 Branch | v2 Branch vs Base |
| ----------- | -------: | --------: | ------: | --------: | ----------------: |
| Wall time   |    90.0s |     85.0s |   85.0s |     85.0s |            **0%** |
| Peak memory | 11.5 GiB |  11.9 GiB | 9.6 GiB |  10.4 GiB |      **+8.3% 🔴** |
| Avg memory  |  4.8 GiB |   4.6 GiB | 4.3 GiB |   4.6 GiB |      **+7.0% 🔴** |
| CPU user    |   902.2s |    853.8s |  848.2s |    846.0s |         **-0.3%** |
| CPU sys     |    63.3s |     60.2s |   58.8s |     58.9s |               ~0% |
| Peak spill  |      0 B |       0 B |     0 B |       0 B |                 — |

The interesting **v1 → v2 branch changes** are: wall time stays **85s**, peak memory drops **11.9 → 10.4 GiB (-12.6%)**, CPU user drops **853.8 → 846.0s (-0.9%)**, and CPU sys drops **60.2 → 58.9s (-2.2%)**. Average memory is unchanged at **4.6 GiB**.

So overall, **v2 is more resource-efficient than v1, especially on peak memory**. However, relative to its own v2 base, the branch now has essentially **no runtime/CPU advantage and consumes more memory** (+0.8 GiB peak, +0.3 GiB average).

One caveat: the **v2 base itself improved quite a lot versus v1 base**, so the loss of the branch's relative advantage isn't because the branch got slower—the baseline caught up. 👍
