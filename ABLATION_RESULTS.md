# Ablation results

## Semantic preservation (acceptance gate)

Every optimization flag must produce byte-identical lineage to the optimized path.
Verified by running the full Scala suite — which asserts exact witness sets and exact
trace outputs — under each flag forced on via `-Dspark.titian.ablation=…`
(`scripts/verify_ablation.sh`).

| configuration | flags | result |
|---|---|---|
| all optimizations (default) | *(none)* | ✅ 44/44 |
| P1a off | `legacyHash` | ✅ 44/44 |
| P1b off | `boxedCombiner` | ✅ 44/44 |
| P2 off | `boxedJoinBuffer` | ✅ 44/44 |
| P3 off | `boxedBlocks` | ✅ 44/44 |
| P4 off | `driverTrace` | ✅ 44/44 |
| fully legacy | all five | ✅ 44/44 |

**7/7 configurations green** — every legacy/optimized pair is semantically
indistinguishable. The optimizations are performance-only.

## Performance (to be run on dedicated hardware)

The wall-time deltas of individual optimizations (5–15%) are below laptop noise, so
the performance table is left for a cluster run with at-scale data. Reproduce with:

```bash
tpcds/gen_data.py 10          # at-scale data
bin/sbt 'examples/runMain edu.vt.bigdebug.examples.AblationBenchmark 2000000 7'
```

Trustworthy even at small scale (deterministic): **lineage footprint** — the
`boxedBlocks` ablation roughly triples the SQL join+aggregate block footprint
(≈2 MB compact → ≈7 MB boxed at 2M rows), matching the 4.3× cut measured on `main`.
The capture-side GC time and single-row trace latency columns isolate P1/P2
(allocation pressure) and P4 (executor-side filtering) respectively.

Fill this section in from `tpcds/ablation.csv` after a clean cluster run.
