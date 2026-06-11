# BigDebug 4.0 — Migration Plan: BigDebug + Titian → Spark 4.x

**Goal:** Re-implement Titian (record-level data provenance, VLDB '16 / PVLDB
extension) — and, in a later phase, BigDebug (interactive debugging primitives,
ICSE '16) — for modern Spark, including PySpark and Spark SQL, as an
**attach-as-a-library** that works with stock Spark 4.1+, not as a fork of the Spark
source tree.

**Current scope: Titian only.** All BigDebug items below are deferred until the Titian
port is complete and validated against the original test programs.

- Reference fork (Spark 2.1, Titian): `reference/titian-2.1/` — clean clone of
  https://github.com/maligulzar/bigdebug branch `titian-2.1`, read-only.
- BigDebug primitives (watchpoints/breakpoints/crash culprits) are on the older
  `bdd`/1.2.1-era branches of the same repo — clone into `reference/` when needed.
- Papers: https://people.cs.vt.edu/~gulzar/publications/
- **Everything lives under this `bigdebug-4.0/` directory** — reference clones, new
  code, docs, benchmarks.

**Fidelity constraint:** the new implementation must preserve the semantics of Titian
and BigDebug exactly as defined in the papers and the original implementation (see
"Fidelity requirements" in `TOUCHPOINT_INVENTORY.md`). When a Spark 4 extension point
forces a different *mechanism*, the observable behavior — capture granularity, tracing
results, debugging semantics — must remain the same; validate against the original
`LineageSuite` test programs.

---

## 1. Strategic decision: library, not fork

The 2016 implementation modified Spark core (ShuffleWriters, combiners, RDD iterators)
because Spark 2.1 had no extension points. Spark 4 does. Rebasing the 2.1 diff onto 4.1
is a rewrite anyway, and a fork puts the project back on the release treadmill that aged
out the original artifact.

**Keep the algorithms, replace the integration mechanism:**

| 2016 mechanism (fork) | Spark 4 extension point (library) |
|---|---|
| Custom shuffle instrumentation in core | Pluggable `ShuffleManager` (`spark.shuffle.manager`) wrapping the sort-based shuffle — tags records crossing stage boundaries (heart of Titian capture) |
| Driver/executor lifecycle hacks | Plugin framework (`spark.plugins`, Spark 3.0+) — code in every executor; hosts watchpoint/breakpoint coordination and profiling |
| Modified Catalyst / physical plans | `SparkSessionExtensions` — inject analyzer/optimizer rules and planner strategies from an external jar (the RAPIDS / Spline pattern) |
| Custom runtime probes | `Dataset.observe()` / `CollectMetrics`, `QueryExecutionListener`, task-failure listeners |

Algorithms to preserve from the papers:
- Compact lineage capture **at stage boundaries only**, stitched lazily at trace time (Titian's key overhead insight)
- Recursive join-based backward/forward tracing
- Crash culprit isolation (delta-debugging style)
- Guarded watchpoints / simulated breakpoints / latency alerts

## 1.5 Design notes (2026-06-10): PySpark and SQL codegen

**SQL / whole-stage codegen — yes, generated classes can carry tracing code, without
patching Spark.** Whole-stage codegen fuses the physical plan into one generated Java
class by asking each operator that implements `CodegenSupport` to emit its fragment
(`doProduce`/`doConsume`). A library can inject *its own* physical operators via
`SparkSessionExtensions` (planner strategy or columnar/physical rule); if those
operators implement `CodegenSupport`, their per-record tracing code is **fused into the
generated loop** like any built-in operator — near-zero abstraction overhead. So the
plan is a hybrid:
  - provenance ids propagate as plan-level columns (Catalyst rule — survives the
    optimizer and AQE),
  - capture happens in codegen-participating **tap operators placed at exchange
    boundaries only** (the Titian insight, transplanted), writing the same
    `(inputId, outputId)` buffers `TapLRDD` writes today, reusing `LineageTaskState`/
    `LineageManager`/trace machinery.
  Constraints to respect: taps must keep `supportCodegen` true or the whole stage falls
  back to interpreted mode; AQE re-plans stages, so injection must happen in rules AQE
  re-runs (not one-shot plan surgery); per-record code must be branch-light.

**PySpark — two distinct stories:**
  - *DataFrame API (the one to build):* Python DataFrame calls construct the same JVM
    logical plans, so the SQL approach above covers PySpark DataFrames for free; only a
    thin py4j wrapper (`LineageContext`, `getLineage`, `goBack`, `show`) is Python work.
    Python UDFs are opaque but arrive as plan nodes (`BatchEvalPython`/Arrow eval) —
    record-level lineage through a UDF is 1-in-1-out per row, so the tap-at-exchange
    design is unaffected.
  - *RDD API (research-grade, defer):* PySpark RDD ops pipeline pickled batches through
    `PythonRDD`, and pair-shuffles hash keys Python-side — the JVM sees opaque byte
    blobs, so JVM-side taps cannot assign per-record ids without deserializing. Would
    need Python-side taps mirroring `TapLRDD` plus id-threading through the pickler.

## 2. Spark SQL: provenance by query rewrite

Titian's record-level iterator instrumentation does **not** translate to SQL execution:
whole-stage codegen fuses operators into generated Java (no per-operator iterator
boundary to wrap), and AQE rewrites physical plans mid-query.

The approach that survives both: **provenance by logical-plan rewrite** — a Catalyst rule
that augments the plan to propagate provenance annotations as extra columns
(PERM/GProM-style rewrites adapted to Spark semantics). Aggregates, joins, and exchanges
need care; apply the Titian insight (capture only at stage/exchange boundaries, stitch
lazily) instead of dragging full provenance through every operator. Because provenance
lives in the plan, Catalyst optimizes it, codegen compiles it, and AQE can't break it.

**Side effect:** plan-level provenance makes PySpark and Spark SQL nearly free — the
Python DataFrame API builds the same logical plans.

## 3. PySpark specifics

The hard part is opaque Python UDFs (now Arrow-batched). BigDebug features map to
**wrapping the UDF**:
- catch exceptions + capture the offending input record (crash culprit)
- evaluate watchpoint predicates
- sample latency

This is easier in Python than the bytecode-level tricks needed for Scala closures in 2016.

## 4. Spark Connect (plan for it from day one)

The decoupled client/server model is increasingly the default deployment; the driver is
no longer in the user's process, which changes BigDebug's interactive story. Spark
Connect has its own plugin mechanism (custom relations/commands over protobuf). Expose
the interactive debugging session — pause, inspect intermediates, trace backward,
resume — as Connect commands so the tooling works identically from Scala, PySpark, and
notebooks.

## 5. Why this is worth doing

Mainstream lineage tooling (Spline, OpenLineage, Unity Catalog) is coarse-grained —
dataset/column level from plans. Nobody does **record-level provenance with interactive
tracing**, and nobody does **interactive debugging primitives on live Spark jobs**. The
niche is still empty and more valuable now (data quality, ML training-data debugging).
A Spark-4-native library is also a better artifact story for follow-on papers than a fork.

## 6. Target environment

- Spark 4.1.x (stable; 4.2.0 in preview), Scala 2.13, JDK 17+
- AQE on by default; whole-stage codegen dominant in SQL
- Spark Connect first-class

---

## Status (2026-06-10)

Tasks 1–3 are **done**: the Titian core is ported and the three original `LineageSuite`
programs (Grep, WordCount, AverageLengthError) pass on stock Spark 4.1.2.

- Build: `bin/sbt compile` / `bin/sbt test` (wrapper pins the project-local JDK 17 and
  sbt under `tools/`). Spark 4.1.2 + RoaringBitmap + Guava are `Provided`; fastutil is
  bundled.
- Layout: `src/main/{scala,java}/org/apache/spark/...` — staying in Spark's package
  namespace preserves `private[spark]` access without forking.
- New shim classes (mechanisms replacing fork core-edits, semantics unchanged):
  `LineageTaskState` (per-task capture state + buffer pools), `RDDShims` (reflection
  DAG-splicing for tap injection), `LineageHashPartitioner` (per-shuffle murmur3
  partitioning), `LineageHashing` (single hash definition), reworked `LAggregator`
  (wrapped map-side combine functions carry the lineage tag through the stock sorter),
  `LBlockStoreShuffleReader` (Spark 4 fetch plumbing; the one version-coupled class),
  `LineageManager` (task-completion-listener materialization).
- Known divergences from the fork (all mechanism-level, behavior preserved — see
  TOUCHPOINT_INVENTORY.md "Status"): tag-at-tap instead of sorter edits; murmur3
  partitioning only on lineage shuffles; `murmur3_32_fixed` (internal consistency only);
  scheduler empty-task hack dropped (PartitionPruningRDD planned for trace-time pruning).

## Caveats and known limitations (as of 2026-06-10)

### Cluster deployment
- **Validated up to `local-cluster`, not multi-machine.** `ClusterLineageSuite` runs on
  `local-cluster[2,1,1024]` — real standalone master + two separate executor JVMs —
  which covers cross-JVM serialization, executor classpath provisioning, and remote
  BlockManager fetches of lineage blocks. Multi-*machine* concerns (networking, jar
  shipping, cloud deploy modes) are still unvalidated.
- **Real clusters: ship two jars.** `--jars titian.jar,fastutil.jar` — everything else
  is `Provided` (Spark ships RoaringBitmap and Guava).
- **Executor loss loses lineage.** Tap buffers are materialized into executor
  BlockManagers (`MEMORY_AND_DISK`, unreplicated). Dynamic allocation, preemption, or an
  executor crash discards that executor's lineage partitions — same property as the
  original Titian. Avoid `spark.dynamicAllocation` during capture/trace sessions, or
  add block replication if robustness is needed.
- **local-cluster tests need a real Spark distribution.** The standalone Worker launches
  executors through Spark's launcher, which requires `SPARK_HOME` with a `jars/` dir
  plus `SPARK_SCALA_VERSION=2.13` (normally set by `bin/load-spark-env.sh`, which the
  worker's command builder bypasses). Both are wired into `build.sbt` (`Test/envVars`)
  pointing at `tools/spark-4.1.2-bin-hadoop3/`.

### Distributed selective join (paper §4 optimization)
- Preserved: `LocalityAwarePartitioner` routes the trace cursor by the partition id
  packed in each record id; `rightJoin` is `zipPartitions` (narrow), so lineage tables
  are never reshuffled during recursive tracing; task placement follows cached blocks.
  `ClusterLineageSuite` asserts structurally that the trace join contains a
  `ZippedPartitions` ancestor — a regression to a full shuffle fails the build.
- **Dropped without replacement yet:** the fork's scheduler hack that skipped tasks for
  empty partitions at trace time. Planned equivalent: `PartitionPruningRDD` over the
  trace RDDs. Until then, traces over many-partition datasets schedule (cheap) empty
  tasks the fork avoided.
- No overhead re-benchmarking against the paper's numbers yet (capture ≤ ~30% target).

### Spark-version coupling (revisit at every Spark upgrade)
- `LBlockStoreShuffleReader` mirrors Spark 4.1.2's shuffle-fetch plumbing — the one
  class that copies internals rather than calling them. Upstream sources pinned in
  `reference/spark-4.1.2-snippets/` for diffing on upgrade.
- `RDDShims` reflects on private fields (`RDD.deps`, `RDD.dependencies_`,
  `ShuffleDependency._rdd`); field names are not API and can shift between versions
  (lookup is name-list based and fails loudly).
- The `org.apache.spark.*` packaging trick grants `private[spark]` access — internals
  stability, not API guarantee. Run the suite against each new Spark minor before
  claiming support; add a cross-version test matrix for 4.2.

### Ported but not yet validated (expected parity, no tests)
- `sortByKey` / `OrderedLRDDFunctions` — reader-side sort-capture path is implemented
  but unexercised; also sort shuffles use `RangePartitioner`, which is not hash-aligned,
  so forward tracing across a sort shuffle is unverified.
- Replay machinery (`setUpReplay` / `replay`).
- Union / zipPartitions / coalesce / parallelize taps.
- Pre-shuffle cache read path (`read(isCache = Some(true))`).
- Output sinks: `saveAsHadoopDataset` (rewritten to tap-then-delegate), `saveAsCSVFile`.

### Semantics notes (by design, matching the paper/fork)
- Backward trace through a shuffle returns **all** records sharing the key (key-hash
  granularity at stage boundaries), not only the single contributing record.
- **Control-flow dependencies through DISTINCT are captured.** `distinct` routes through
  the tapped shuffle (`map(x => (x, null)).reduceByKey`), and pre-shuffle capture
  records every input record *before* the combiner collapses duplicates — so a distinct
  output traces back to **all** of its duplicate witnesses, with their distinct source
  offsets (verified by `LineageOpsSuite."Distinct"`). Same shape applies to other
  existence-dependent ops built on cogroup/shuffle (`intersection`, `subtract`).
  *Not* captured: position/cardinality control dependencies that bypass the shuffle —
  `limit`/`take`/`top` (driver-side selection) and `sample` — these need their own tap
  design if required.
- `show()` deduplicates identical display strings per partition (fork behavior in
  `ShowRDD.collect`). The underlying trace records are not deduplicated — use
  `ShowRDD.dump` for the raw `(coordinate, text)` pairs.
- Key hashing is murmur3 of `key.toString` (`LineageHashing`, now `murmur3_32_fixed`);
  keys that are `equals`-equal but `toString`-different would mishash — same constraint
  as the fork. Only internal consistency matters, but don't compare stored hashes
  across library versions.
- Capture-time shuffles silently swap `HashPartitioner` for `LineageHashPartitioner`
  (murmur3): partition *assignment* differs between captured and uncaptured runs of the
  same job (output values identical). The fork did this globally; we scope it to
  captured L-shuffles.
- Capture buffers are pooled 8 MB / 64 MB arrays allocated lazily (fork pre-allocated
  ~1.1 GB per executor eagerly). Very small executors may still feel memory pressure
  under heavy capture.

### Minor API warts
- The 3-arg `combineByKey` is the stock `RDD`-typed signature; it dispatches to the
  lineage implementation at runtime but callers must cast to `Lineage[...]` (or use the
  partitioner overload).
- `CompactBuffer.toString` renders as `Seq(...)` on Scala 2.13 (was `CompactBuffer(...)`
  on 2.11) — affects `show()` output strings only.

## Task list

1. **Touchpoint inventory — DONE for Titian.** `TOUCHPOINT_INVENTORY.md` maps all 72
   files the fork touched (diff of `reference/titian-2.1` root commit → HEAD) to Spark 4
   extension points. Bucket (c) is small, as predicted: wrapper-ShuffleManager API
   drift, trace-time shuffle reads, hash/partitioner semantics. Remaining: inventory
   the `bdd`/1.2.1 branch for the BigDebug primitives.
2. **Project skeleton.** sbt/Maven multi-module library against Spark 4.1: `core`
   (lineage capture via ShuffleManager wrapper + plugin), `sql` (Catalyst rewrite rules
   via SparkSessionExtensions), `python` (UDF wrappers), `connect` (Connect plugin).
3. **Titian core port.** Stage-boundary lineage capture + recursive join tracing on RDDs.
4. **SQL provenance rewrite rules.** Design doc first: rewrite rules for projection,
   filter, join, aggregate, exchange; then implement behind a `SparkSessionExtensions`
   injection.
5. **BigDebug primitives.** Watchpoints, simulated breakpoints, crash culprit isolation,
   latency profiling — on the plugin framework + listeners + `observe()`.
6. **PySpark UDF wrapping layer.**
7. **Spark Connect commands** for the interactive session.
8. **Benchmarks** reproducing the papers' overhead numbers on Spark 4.

## Housekeeping

- This directory currently sits inside an unrelated parent git repo
  (`~/workspace/git` → `cs239-winter2018-students`). Run `git init` here (or move the
  parent's `.git` aside) before committing; add `reference/` to `.gitignore` (it has its
  own clone).
