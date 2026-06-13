# Developer guide

How Titian attaches to stock Spark, where the code lives, and how to extend it.

## Two mechanisms make it a library (no forked Spark)

1. **Same-package access.** All code lives in `org.apache.spark.*` packages, so it can
   call Spark's `private[spark]` internals (block manager, task context, RDD
   dependency rewriting) without modifying Spark. Subclassing and DAG splicing do the
   work; a few genuinely-private fields are read reflectively.
2. **Codegen injection.** SQL capture is a `SparkSessionExtensions` columnar rule
   (`TitianSQLExtension`) that inserts tap operators into the physical plan **before**
   `CollapseCodegenStages`, so each tap's per-row capture is fused into Spark's
   generated Java.

## Code layout

| package / dir | contents |
|---|---|
| `org.apache.spark.lineage` | RDD capture engine: `LineageContext` (DAG splicing, trace navigation), `LAggregator`, partitioners, `LineageHashing`, the `TitianAblation` flags |
| `org.apache.spark.lineage.rdd` | the lineage RDDs (`TapLRDD` family) and the `LineageRDD` trace API |
| `org.apache.spark.sql.lineage` | SQL capture: the tap-insertion rule, the tap operators (`TapExec`), per-task runtimes and block formats (`taps.scala`), and the driver-side trace model (`TitianSQL`) |
| `src/test/scala` | the acceptance suites (exact-witness assertions) |
| `python/`, `tpcds/`, `examples/`, `notebooks/`, `docker/` | bindings, coverage harness, demos, reproducibility |

## How SQL capture works (one query)

1. `InsertTitianTaps` runs per query stage (AQE) or once (non-AQE). It guards (skip
   commands / display-only / already-tapped), **validates the plan against a strict
   allowlist** (anything else throws `TitianUnsupportedOperatorException`), then
   inserts taps: scan taps at leaves, pre-exchange taps below shuffles / broadcasts,
   post-keyed taps above aggregates / joins / windows / `ORDER BY ... LIMIT`,
   probe-mark taps under broadcast joins, a result tap at the root.
2. At run time each tap appends to primitive arrays that become compact `TapBlock`s in
   the executors' `BlockManager`s.
3. `TitianSQL` parses the executed plan into a capture graph and walks it:
   backward/forward steps flatMap filtering closures over the blocks (executor-side);
   source rows are recovered by deterministically re-scanning the inputs.

## Adding support for a new operator

1. Add it to the allowlist in `InsertTitianTaps.validateSupported` (or it fails
   loudly — the correct default until lineage is proven).
2. Decide its lineage shape: a **pass-through** (no tap), a **keyed boundary** (pre tap
   below + post tap above, like aggregates/joins), or a **1:1 buffered node**
   (seq/reseq taps, like Python UDF eval). Insert the taps in the rule.
3. Parse it in `TitianSQL.parseSource` so the trace graph knows how to traverse it.
4. Respect the **soundness guard** (`idSafeFromShuffle`): a tap recording
   `currentInputId` must not consume rows that crossed a shuffle without a keyed
   operator re-basing ids in between — that records stale ids (silent wrong lineage).
   When in doubt, fail loudly.
5. Add a unit test asserting the exact witness set, then run the TPC-DS oracle.

## Testing layers (the acceptance bar)

- **Unit suites** (`bin/sbt test`) assert *exact* witness sets and traces on small
  fixtures; the original `LineageSuite` programs are the acceptance bar.
- **TPC-DS coverage** (`TPCDSCoverage`) runs all 103 queries: capture-off vs on answers
  must match, then one row is traced to a scan. `--oracle` re-executes on only the
  traced witnesses to verify recall (ground truth, not just consistency). See
  [TPC-DS coverage](tpcds.md).
- **Ablation verification** (`scripts/verify_ablation.sh`) re-runs the suite under each
  optimization flag to prove the optimizations preserve semantics. See
  [Ablation](ablation.md).

## References

Titian (PVLDB '16) and BigDebug (ICSE '16). Trace granularity across shuffles is
key-hash level; broadcast-join probe sides are exact. The distributed selective join
(partition-id-routed cursor + `zipPartitions`) is preserved from the paper.

For class- and method-level detail, see the
[Scaladoc API reference](api/index.html).
