# Titian-SQL: record-level provenance for Spark SQL / DataFrames

**Goal:** the same record-level capture-and-trace Titian provides for RDDs, for SQL and
DataFrame queries on stock Spark 4.1.x — attach-as-library via `spark.sql.extensions`,
no fork. PySpark DataFrames come along for free (same JVM plans).

**Architecture (settled in MIGRATION_PLAN §1.5):** hybrid of
1. **Tap physical operators at exchange boundaries** — custom `SparkPlan` nodes
   implementing `CodegenSupport`, injected by `SparkSessionExtensions` rules, whose
   per-record capture code is *fused into the whole-stage-codegen generated class*.
   They write the same `(inputId, outputId)` / `(keyHash, inputId)` buffers the RDD
   taps write, reusing `LineageTaskState`, `LineageManager`, `PackIntIntoLong`,
   `Int2RoaringBitMapOpenHashMap`, and the existing trace-join machinery.
2. **Associations keyed by group/join-key hash across exchanges** (`LineageHashing`),
   exactly the VLDB '16 stage-boundary design. Within a fused pipeline
   (scan→filter→project), the WSCG loop is depth-first per input row, so the
   `currentInputId` threading that works for RDD taps works inside a generated stage
   too. Aggregates and joins associate via key hash, not row threading — same as the
   RDD port (`LAggregator`, cogroup taps).

Titian's trace-side machinery (recursive joins, `LocalityAwarePartitioner` selective
routing, BlockManager materialization) is **reused as-is**: SQL capture produces the
same buffer shapes, so `getLineage`/`goBack`/`goNext`/`show` operate unchanged on top.

---

## Status (2026-06-10, later): Phases 0–3 DONE; phase 4 partially scoped

`SQLLineageOpsSuite` (11 tests) + `SQLLineageSuite` (5) all green; 26/26 project-wide.
Working and verified, each in AQE-on and AQE-off modes where it matters, plus
codegen-off variants:
- groupBy aggregates across the exchange (two scan partitions, backward + forward
  round-trip), multi-aggregate with HAVING, global aggregates (SinglePartition
  exchange), `SELECT DISTINCT` (all control-flow witnesses), grouping by expressions
  (`GROUP BY UPPER(c)` — grouping attrs resolved through result-expression aliases),
  filter/projection pipelines under aggregates;
- inner joins (SMJ) traced into **both** sources via `goBack(path)`;
- **join-then-aggregate two-hop traces**: result -> aggregate level -> joined-row
  level -> either source, and forward all the way back up.

Mechanism notes added during implementation:
- The generalized taps: `TapPreExchangeExec` (key-hash -> RoaringBitmap of input ids;
  below the partial aggregate when one exists, else directly below the exchange keyed
  by its HashPartitioning) and `TapPostKeyedExec` (packed outIdx -> key-hash above
  final aggregates/joins; threads outIdx as currentInputId). Key hash =
  `UnsafeProjection(keys).hashCode` — byte-stable murmur3, no toString allocation.
- Rule pass-throughs required in practice: command plans (CREATE VIEW etc.), the
  outer `AdaptiveSparkPlanExec` wrapper invocation, and already-tapped subtrees.
- Spark 4's `ResultQueryStageExec` is a leaf-like wrapper around the final stage —
  the trace-graph parser unwraps it (reflectively, field may move across versions).
- `show()` returns rows **as scanned** — i.e. column-pruned to what the query read
  (DISTINCT witnesses display identically but remain distinct records). Full-source-row
  reconstruction is a phase-4 nicety.
- Driver trace API: `TitianSQL.collectWithLineage / trace -> TraceCursor`
  (`goBack(path)/goNext/show/ids`); blocks discovered via BlockManagerMaster, read by
  `TapBlockRDD`. Trace joins currently collect to the driver — fine at test scale;
  swapping in the RDD machinery's distributed selective join is phase-4 work.

## Status (2026-06-10, phase 4 first pass): 33/33 tests green

`SQLLineagePhase4Suite` (7 tests) adds, all verified:
- **BroadcastHashJoin** with **exact probe-row provenance**: the post-join tap stores
  (keyHash, probeInputId) per output row, so the streamed side traces to the single
  contributing row, not the key group; the build side is captured by a pre tap under
  `BroadcastExchangeExec` keyed by the broadcast mode's (rewritten) keys, with the
  stream-side hash rewritten identically (`TitianJoinKeys` accessor for
  `HashJoin.rewriteKeyExpr` — multi-integral keys get packed, the hashes must match).
- **Outer/semi/anti joins** on SMJ/SHJ/BHJ (FullOuter via Coalesce(left,right) keys;
  BHJ FullOuter excluded): unmatched rows trace to themselves with an empty other side.
- **Window functions** at peer-set granularity (post tap keyed by the partition spec).
- **Generate/explode and Expand** thread exactly (depth-first per input row in the
  generated loop); `COUNT(DISTINCT)` two-level aggregate chains trace through both
  exchanges.
- **ROLLUP/CUBE still fail loudly**: the grouping-id column rarely survives into the
  output, so the post tap cannot key consistently with the pre tap — needs a dedicated
  design (next on the list).

**First overhead numbers** (`CaptureOverheadBenchmark`, 2M rows, local[2], median of 3
— short runs, so constants are exaggerated; rerun at cluster scale before quoting):
- SQL groupBy aggregate: **+9.8%** — the fused-codegen taps + UnsafeRow hashing pay off.
- SQL join + aggregate: **+39%** — three taps and bitmap puts across two shuffles.
- RDD reduceByKey: **+55%** — above the paper's ~30%; prime suspects are the
  LAggregator wrapper's per-merge Tuple2 allocation (TOUCHPOINT_INVENTORY notes it) and
  per-record `toString` murmur hashing. Optimization pass warranted.

## Status (2026-06-10, phase 4 second pass): 35/35 tests green

Added and verified:
- **Columnar scans (Parquet/ORC).** The row-based `TapScanExec` is inserted above the
  columnar scan in preColumnarTransitions; Spark's own transition inserter (which runs
  after) places `ColumnarToRowExec` between them, so ids are assigned at the row
  boundary, still inside whole-stage codegen. `show()` re-scans via `executeColumnar`
  + batch flattening (same row order).
- **ROLLUP/CUBE/GROUPING SETS** via visible-subset keying: both taps drop the
  synthetic `spark_grouping_id` from their key sets, so per-grouping-set rows and
  grand totals trace correctly. Coarsening caveat: grouping sets whose *visible* key
  values coincide (e.g. a natural NULL vs a rolled-up NULL) are conflated — a union of
  witnesses, never a wrong direction. If a *visible* grouping column is dropped from
  the output, capture still fails loudly.
- **Full-source-row `show(full = true)`**: re-executes the captured scan with the
  complete data schema (same FilePartitions/order) when nothing was pushed into the
  reader (`dataFilters` empty, unpartitioned table); falls back to the pruned view
  otherwise.

Phase 4 still remaining: Python UDF taps + PySpark wrapper (needs a pyspark test
harness), DSv2 scans, cached relations (`InMemoryRelation`), writes as result taps,
distributed trace joins (cursor collects to driver), block lifecycle (tap blocks not
ContextCleaner-tracked), at-scale benchmark. All unsupported shapes fail loudly per
caveat 4.

## Status (2026-06-10): Phases 0 and 1 DONE

`SQLLineageSuite` (5 tests, green; 15/15 project-wide):
- capture-off is a verified no-op; unsupported operators throw
  `TitianUnsupportedOperatorException` (strict allowlist: row-based
  FileSourceScan/Filter/Project);
- `TapScanExec`/`TapResultExec` verified **inside** `WholeStageCodegen` (no fallback),
  with working interpreted paths under `spark.sql.codegen.wholeStage=false`;
- backward trace + `showInputs` (deterministic re-scan) bridge filter-induced index
  shifts; driver API: `TitianSQL.collectWithLineage/backward/showInputs`.

**Plan deviation, deliberate:** injection uses `injectColumnar`
(`preColumnarTransitions`), not `injectQueryStagePrepRule` — the prep-rule hook fires
only under AQE, while the columnar hook runs in both modes and *before*
`CollapseCodegenStages` (so taps fuse), per stage under AQE. Phase-1 result-tap
placement assumes single-stage plans; per-stage placement is the phase-2 item.
Implementation notes: tap runtime objects are plain classes (`ScanTapRuntime`,
`ResultTapRuntime`) so generated Java calls them without Scala-object forwarder tricks;
result-tap block ids come from `SparkContext.newRddId()` (unique vs. real RDDs; not
ContextCleaner-tracked — TODO when block lifecycle management lands).

## Phases

### Phase 0 — injection plumbing (small)
- New source tree `org.apache.spark.sql.lineage` (same jar for now; `spark-sql` added
  as `Provided`).
- `TitianSQLExtension extends (SparkSessionExtensions => Unit)`; users enable with
  `spark.sql.extensions=...TitianSQLExtension` + `spark.titian.sql.capture=true`
  (SQLConf-registered flag, togglable per query).
- Inject a no-op physical rule through **`injectQueryStagePrepRule`** (the AQE-safe
  hook — runs on every query-stage re-plan) and verify it fires with AQE on and off.
- Exit criterion: a test asserting the rule observed every stage of a 2-stage query
  under AQE.

### Phase 1 — single-stage capture: scan → filter → project → collect
- `TapScanExec(child)`: assigns per-task row ids (`nextRecord` counter), sets
  `currentInputId` in `LineageTaskState`; codegen `doConsume` emits ~2 Java statements.
- `TapResultExec(child)` at the plan root: records `(outputIdx, currentInputId)`,
  materializes via task-completion listener (existing `LineageManager`).
- Backward trace for one stage + `show()` resolving to source rows by **deterministic
  re-scan**: re-run the same `FileSourceScanExec` partitions zipped with row indices.
- Exit criteria: trace equals known file content; `executedPlan` contains
  `WholeStageCodegen` *enclosing the taps* (no fallback); interpreted `doExecute`
  path also implemented and tested with `spark.sql.codegen.wholeStage=false`.

### Phase 2 — one exchange: groupBy / aggregate
- `TapPreExchangeExec` below the partial `HashAggregateExec`: records
  `(murmur3(groupKey), currentInputId)` into the RoaringBitmap buffer (pre-shuffle tap
  semantics).
- `TapPostExchangeExec` above the final aggregate: records `(groupKeyHash, outputId)`
  (post-shuffle tap semantics).
- Capture-side partition routing: record the **actual** partition id assigned by the
  exchange's partitioning (post-AQE), rather than recomputing from the hash — the
  fidelity-improving rule from TOUCHPOINT_INVENTORY C3.
- Trace across the exchange with the existing key-hash join; SQL equivalents of the
  WordCount/AverageLength acceptance tests.

### Phase 3 — joins and multi-stage queries
- SortMergeJoin / ShuffledHashJoin: the cogroup pattern — post-exchange taps on both
  inputs keyed by join-key hash; post-join tap keyed the same; `goBack(path = i)`
  follows join input *i* (re-use cogroup trace code).
- During capture, force broadcast joins off (`spark.sql.autoBroadcastJoinThreshold=-1`)
  so every join crosses an exchange — phase-4 removes this restriction.
- Multi-stage DAGs, forward tracing, `LineageOpsSuite`-style SQL test battery
  (join + groupBy + distinct), plus a `local-cluster` SQL test.

### Phase 4 — the long tail (each gated by a test)
- BroadcastHashJoin: tag the build side inside the broadcast relation (id column or
  hashed-relation wrapper); probe-side association at the join tap.
- `Expand` (rollup/cube/count-distinct), `Generate` (explode), Window, Sort, Limit.
- Python UDFs (`BatchEvalPython`/Arrow eval nodes — row-aligned, tap before/after),
  PySpark py4j wrapper for the trace API.
- DataSource V2 scans, `InMemoryRelation` (cached DataFrames) as a capture boundary,
  reused exchanges, writes (`InsertInto*`) as result taps.
- Overhead benchmark vs. the paper's ≤~30% capture target (TPC-H subset).

---

## Caveats — read before and during implementation

1. **This will be the most version-coupled module in the project.** `SparkPlan`,
   `CodegenSupport`/`CodegenContext`, AQE rule interfaces, and operator internals are
   `private[spark]` *and* churn every minor release — far more than anything the RDD
   port touches. Pin to 4.1.x; extend the cross-version test matrix before claiming
   4.2. Budget upgrade work at every Spark bump.
2. **Codegen fallback is a silent perf cliff, not a correctness bug.** Any plan shape
   our taps don't support in codegen drops the whole stage to interpreted mode. Every
   tap therefore needs both `doProduce`/`doConsume` *and* a correct interpreted
   `doExecute`, and tests must assert codegen actually engaged (inspect
   `WholeStageCodegenExec` nesting), or we'll ship a 10x slowdown without noticing.
   Related: the 64KB/`hugeMethodLimit` JIT thresholds — our injected statements add
   bytes and can push borderline stages over; keep per-record code to a few statements.
3. **AQE re-plans at runtime.** One-shot plan surgery loses or duplicates taps.
   Injection must live in `queryStagePrepRules` (re-run per stage). Partition
   coalescing changes reduce-partition counts → capture must use actual runtime
   partition ids (it does, by design). **Skew-join splitting** replicates a reduce
   partition across tasks → duplicate post-exchange capture entries; until handled,
   capture runs with `spark.sql.adaptive.optimizeSkewedJoin=false` (enforced + warned).
4. **Operator coverage is explicit, not implicit.** Anything outside the covered set
   must make capture *fail loudly* (analysis-time error listing the unsupported
   operator), never silently produce wrong lineage. The long tail is genuinely long:
   Expand multiplies rows (one input → N group-key copies), Window peeks across rows,
   Generate is 1→N — each changes the id-threading story and gets its own design+test.
5. **Columnar scans:** Parquet/ORC arrive as `ColumnarBatch`es; row ids get assigned at
   the `ColumnarToRow` boundary (batch offset + row index). Declare incompatibility
   with other columnar-rule engines (RAPIDS, Photon-alikes) — two plugins rewriting
   the same plan will fight.
6. **`show()` validity window = source immutability.** Mapping ids back to source rows
   relies on deterministic re-scan: same files, same splits, same in-split order. Holds
   for static file sources; does **not** hold for JDBC, streaming, or tables compacted/
   rewritten between capture and trace (Delta/Iceberg snapshots can pin this — later).
   Store `(file, splitStart, rowIndexInSplit)` so the assumption is checkable.
7. **Optimizer interactions are a feature here, but watch the edges.** We inject into
   the *physical* plan post-optimization, so pushdown has already happened and scan
   taps id exactly the rows the query reads (that's the right granularity). But rules
   that run *after* ours (or AQE local-shuffle-reader rewrites) must not reorder
   around taps — taps must declare output ordering/partitioning pass-through correctly
   or they'll break SMJ sort requirements and trigger spurious re-sorts.
8. **Aggregate/join granularity is key-hash granularity** — backward through an
   aggregate returns the whole group's inputs, as in the paper and the RDD port. SQL
   users may expect column-level "why" provenance; document that this is record-level
   lineage, deliberately.
9. **Subqueries execute as separate plans** (scalar/IN/EXISTS) — out of scope until
   phase 4+; capture must detect and refuse, per caveat 4.
10. **Don't break uncaptured queries.** With `spark.titian.sql.capture=false` the
    extension must be a strict no-op — measured-zero overhead, no plan changes. The
    flag flips per query, mirroring `setCaptureLineage` semantics for RDDs.
11. **Memory:** SQL row rates are much higher than typical RDD jobs; the pooled-buffer
    scheme carries over but the overhead benchmark (phase 4) gates any claim of the
    paper's ≤~30% capture target. Capture buffers compete with execution memory in
    unified memory management — watch for aggregation spill regressions under capture.
12. **Trace results surface as RDD-based `LineageRDD`s initially** (reusing the proven
    machinery). A DataFrame-native trace API (`df.lineage.goBack()` returning
    DataFrames) is sugar to add after phases 1–3 prove capture.
