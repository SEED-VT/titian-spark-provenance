# Touchpoint Inventory — Titian fork (Spark 2.1) → Spark 4 extension points

**Source:** clean clone of `maligulzar/bigdebug` branch `titian-2.1` at
`reference/titian-2.1/` (read-only). Root commit `8afbd1bf` is a pristine import of
Spark 2.1.1-SNAPSHOT (verified: no lineage code present), so
`git diff 8afbd1bf..HEAD` is exactly the Titian footprint:
**72 files, +5,475 / −71 lines.**

**Scope note:** the `titian-2.1` branch contains **Titian only**. The BigDebug
primitives (watchpoints, simulated breakpoints, crash culprit isolation, latency
profiling) live in the older 1.2.1-era branches (`bdd`, `spark-lineage` repo). A second
inventory pass over that branch is needed for the BigDebug feature set; its Spark 4
mapping is sketched in `MIGRATION_PLAN.md` §1 (plugin framework + listeners + UDF/closure
wrapping).

Buckets: **(A)** new code that ports into the library mostly as-is; **(B)** core
modification replaced by a Spark 4 extension point or library technique; **(C)** genuine
gap needing care/design.

---

## A. New code → ports into the library (~4,800 lines)

### `lineage/` module (25 files, 3,920 lines) — the Titian engine itself
| File | Role (paper semantics) |
|---|---|
| `LineageContext.scala` | User entry point. Wraps `SparkContext`; `setCaptureLineage`, `getLineage`, `getBackward`/`getForward` (trace navigation), `tapJob` injects Tap RDDs at stage boundaries — the core VLDB '16 design. |
| `rdd/Lineage.scala` | Trait mirroring the RDD API for lineage-aware RDDs (458 lines). |
| `rdd/TapLRDD.scala` | Base tap: assigns compact int record IDs, buffers `(inputId, outputId)` pairs per task into native buffers. |
| `rdd/TapHadoopLRDD.scala` | Input tap: maps record ID → HDFS byte offset. |
| `rdd/TapPreShuffleLRDD.scala` / `TapPostShuffleLRDD.scala` | Stage-boundary taps around shuffles — capture agent of the paper. |
| `rdd/TapPreCoGroupLRDD.scala` / `TapPostCoGroupLRDD.scala` | Same for cogroup/join. |
| `rdd/LineageRDD.scala`, `ShowRDD.scala` | Trace-time API: recursive join-based backward/forward tracing; `show()` to original records. |
| `rdd/PairLRDDFunctions.scala`, `OrderedLRDDFunctions.scala`, `ShuffledLRDD.scala`, `CoGroupedLRDD.scala`, `HadoopLRDD.scala`, `UnionLRDD.scala`, `ZippedPartitionsLRDD.scala`, `CoalescedLRDD.scala`, `MapPartitionsLRDD.scala`, `ParallelCollectionLRDD.scala`, `TapParallelCollectionLRDD.scala` | Lineage-aware mirrors of RDD transformations; build the tapped DAG. |
| `LAggregator.scala` | Wraps user combiner functions to propagate lineage through map-side combine (RoaringBitmap sets of input IDs). |
| `Int2RoaringBitMapOpenHashMap.java` | Compact lineage storage (866 lines). |
| `HashAwarePartitioner.scala`, `LocalityAwarePartitioner.scala` | Trace-time partitioners: route lineage joins to where the data already lives (the "stitch lazily, join locally" optimization). |

**Port note:** mechanical Scala 2.11→2.13 migration (collections, `Traversable`,
procedure syntax). Algorithms and semantics carry over unchanged. These classes extend
`RDD`, use `ShuffleDependency`, `SparkContext.runJob` — all still public/DeveloperApi in
Spark 4.

### Core-side helpers added by the fork (in `core/` only for package access — move to library)
| File | Role | Spark 4 home |
|---|---|---|
| `lineage/LineageManager.scala` | Async materialization of tap buffers to `BlockManager` at task end | Library; triggered by `TaskContext.addTaskCompletionListener` instead of `ShuffleMapTask`/`ResultTask` edits |
| `lineage/LBlockStoreShuffleReader.scala` | Reads lineage data from shuffle outputs at trace time ("shuffle cache") | Wrapper `ShuffleManager` (see B) or BlockManager-based storage (see C2) |
| `lineage/util/*ByteBuffer.scala`, `IntKeyAppendOnlyMap.scala` | Primitive-typed capture buffers (no boxing) | Library, as-is |
| `util/Utils.scala` → `PackIntIntoLong` | Pack (partitionId, recordId) into a Long | Library, as-is |

---

## B. Core modifications → Spark 4 extension points

| Fork change | What it was for | Spark 4 replacement |
|---|---|---|
| `Executor.scala`: pre-allocated byte-buffer pools created at executor start, handed to each task | Reusable capture buffers (avoid alloc/GC per task) | **Plugin framework** (`spark.plugins`): `ExecutorPlugin.init` owns the pools as a singleton; tasks fetch via the plugin object |
| `Task.scala` / `TaskContextImpl.scala`: `currentInputId`, `currentBuffer`, buffer/thread-pool fields threaded through task launch | Per-task capture state pipelined between taps in the same stage | Library-managed state keyed by `TaskContext.get().taskAttemptId()` (ConcurrentHashMap or ThreadLocal in the plugin); cleanup via `addTaskCompletionListener` |
| `ShuffleMapTask` / `ResultTask`: `LineageManager.finalizeTaskCache` in `finally` | Materialize tap buffers when a task ends | `TaskContext.addTaskCompletionListener` — exact sanctioned equivalent |
| `ShuffleWriter.write(records, isLineage)` + `SortShuffleWriter` / `BypassMergeSortShuffleWriter` / `UnsafeShuffleWriter` signature change | Tell the writer lineage is active | **Wrapper `ShuffleManager`** (`spark.shuffle.manager`) delegating to `SortShuffleManager`; lineage-active flag travels via the `ShuffleDependency` created by `ShuffledLRDD` (or a SparkContext local property) — no signature change needed |
| `ExternalSorter.insertAll` lineage variants + `writePartitionedFile` rewrite; `ExternalAppendOnlyMap.insert`; `PartitionedPairBuffer.insert` | Capture `(inputRecordId, keyHash/outputPartition)` while records pass through the shuffle write path | Wrapper `ShuffleWriter`: interpose on the record iterator before delegating to the real writer — tapped records arrive as `(key, (value, lineageId))`; the wrapper strips the ID, records `(id, murmur3(key), partition)`, forwards `(key, value)`. Same association, zero sorter changes |
| `SortShuffleManager.getReader(..., lineage)` + `ShuffleReader.read(isShuffleCache, shuffleId)` | Trace-time reading of lineage from shuffle outputs | Wrapper `ShuffleManager.getReader` returns a custom reader for lineage shuffles (note: Spark 4 signature has `startMapIndex/endMapIndex` for AQE — wrapper must pass through) |
| `Aggregator.scala`: made extendable/serializable, non-spilling path restored | `LAggregator extends Aggregator` to wrap combiner functions | No subclassing needed: instantiate stock `Aggregator(wrappedCreate, wrappedMerge, wrappedCombine)` with LAggregator's wrapped functions when `ShuffledLRDD` builds its `ShuffleDependency` |
| `Dependency.tapDependency`, `RDD.updateDependencies` | Mutate an existing DAG to splice taps in | Build the tapped DAG correctly at construction time (`LineageContext.tapJob` already walks the DAG; create new `*LRDD`s with correct deps rather than mutating) — pure library code |
| `RDD.scala`: `captureLineage`, `materializeBuffer`, `releaseBuffer` hooks | Polymorphic tap behavior on any RDD | Move onto the library's `Lineage` trait / `TapLRDD` — only tap RDDs ever implement them |
| `ShuffledRDD` / `PairRDDFunctions`: `private` → `protected` for subclassing | Let `ShuffledLRDD`/`PairLRDDFunctions` reuse internals | Compose instead of inherit: `ShuffledLRDD` extends `RDD` directly and builds its own `ShuffleDependency` (all public/DeveloperApi) |
| `HadoopRDD.reader` exposed | Tap needs record byte-offsets from the `RecordReader` | Library's `HadoopLRDD` constructs its own Hadoop RDD (via `newAPIHadoopFile` machinery) and owns the reader; offset tracking stays in `TapHadoopLRDD` |
| `Partitioner.scala`: default `HashPartitioner` switched to murmur3 of `key.toString` | Lineage stores a key *hash*; trace-time must recompute the same partition from the hash | Don't touch the default: `ShuffledLRDD` controls which partitioner its shuffles use, and trace-time uses the lineage module's `HashAwarePartitioner`. See C3 |
| `TaskMetrics`, `ShuffleMapStage.outputLocs` visibility; `SparkEnv`, `DAGScheduler` (commented-out only) | Debug access | Drop — not needed |
| `pom.xml`, `assembly/pom.xml` | Build the `lineage` module inside Spark | New standalone sbt build against `spark-core 4.1.x` (`provided`) |

### Fork hacks to **drop**, with their principled replacement
| Fork hack | Why it existed | Replacement |
|---|---|---|
| `TaskSetManager.dequeueTaskFromList` fabricating successful `DirectTaskResult`s for `null` tasks; `TaskResult.value()` returning null for empty buffers; `RDD.collect()` filtering nulls | Skip scheduling tasks for partitions known to contain no relevant lineage during trace replay (paper's trace-side optimization) | `PartitionPruningRDD` on the trace RDDs — prune empty partitions *before* the scheduler sees them. Same semantics, no scheduler surgery |
| `MapStatus.getSizeForBlock` bounds-check returning `1L` | Reading shuffle outputs with mismatched partition counts via `LBlockStoreShuffleReader` | Disappears if lineage storage moves to BlockManager blocks (C2); otherwise handled inside the wrapper ShuffleManager's own index |
| `ShuffleMapTask`: `System.gc()` when free heap < 9 GB | GC pressure from capture buffers | Plugin-owned pooled buffers + off-heap sizing; never call `System.gc()` |
| `spark.shuffle.sort.bypassMergeThreshold` default 200 → 0 | Force every shuffle through `ExternalSorter` so the capture hook runs | Wrapper-writer capture (above) works on *all three* writer paths, so bypass/unsafe writers no longer need to be disabled |

---

## Status (2026-06-10): Titian core PORTED AND PASSING

The library compiles against stock Spark 4.1.2 (Scala 2.13, JDK 17) and the three
original `LineageSuite` programs (Grep, WordCount, AverageLengthError) pass end-to-end:
capture, BlockManager materialization, backward/forward tracing, `show()`.
Resolutions chosen for the items below:

- **C2 resolved — tag at the tap, not in the sorter.** `TapPreShuffleLRDD.tap` emits
  `(k, (v, packedId))`; `LAggregator` wraps the map-side combine functions handed to the
  stock `ExternalSorter` so the tag survives (collapsing to `(partition, 0)` after a
  combine, as the fork's `writePartitionedFile` did). No `ShuffleWriter`/sorter changes
  at all — the wrapper-ShuffleManager turned out to be unnecessary for the RDD API.
- **C3 resolved — `LineageHashPartitioner`.** Capture-time L-shuffles substitute a
  murmur3-of-key partitioner for the default `HashPartitioner`, restoring the
  hash-locates-partition invariant per-shuffle instead of globally.
- The sort-path (`keyOrdering`) lineage capture is implemented in
  `LBlockStoreShuffleReader` by stripping tags on the iterator before the stock sorter.
- `LBlockStoreShuffleReader` mirrors Spark 4.1.2's fetch plumbing and is the single
  version-coupled class (C1); upstream sources pinned in `reference/spark-4.1.2-snippets/`.

## C. Genuine gaps / design care needed

1. **Wrapper-ShuffleManager API drift.** Spark 4's `ShuffleManager.getReader` takes
   `startMapIndex/endMapIndex` (AQE skew-join splits) and push-based shuffle exists.
   The wrapper must be a faithful pass-through for non-lineage shuffles and must decide
   how lineage capture interacts with AQE partition coalescing (record the *post*-AQE
   partition id). This is the one place where Spark-version coupling remains — isolate
   it in one module.
2. **Trace-time "shuffle cache" reads.** The fork read lineage directly out of foreign
   shuffle outputs (`LBlockStoreShuffleReader` + `MapStatus` hack). Decide:
   (a) reproduce via the wrapper ShuffleManager, or (b) store all tap buffers as
   BlockManager RDD blocks (the fork already half-did this via `LineageManager`) and
   trace purely over cached blocks. (b) is simpler and AQE-proof; benchmark against the
   paper's overhead numbers before committing.
3. **Hash semantics.** The fork changed the *global* `HashPartitioner` to murmur3 so
   stored key-hashes could locate partitions at trace time. A library cannot (and should
   not) change user jobs' partitioning. Rule: lineage-internal shuffles use the library's
   own murmur3-based partitioners; for *user* shuffles, capture must record the partition
   id actually assigned by the user's partitioner (available at write time) instead of
   recomputing it from the hash. Preserves paper semantics without perturbing user jobs
   (the fork's change technically altered user-job partitioning — this is a fidelity
   *improvement*).
4. **Map-side combine.** `LAggregator` wraps user combiners to merge RoaringBitmap ID
   sets. Verify the wrapped-function approach reproduces exact paper semantics for
   `combineByKey` with and without spilling (the fork restored a non-spill path in
   `Aggregator`; in Spark 4, spilling is always on — LAggregator's bitmap merge must be
   spill-safe).
5. **BigDebug primitives not in this branch** — inventory the `bdd`/1.2.1 branch next;
   targets: plugin framework (watchpoint/breakpoint coordination), task/stage listeners
   (latency alerts), closure/UDF wrapping (guarded watchpoints, crash culprits),
   `Dataset.observe` (aggregate watchpoints).

---

## Fidelity requirements (applies to all of the above)

The re-implementation must match the **semantics defined in the papers and the original
implementation**:

- **Titian (VLDB '16):** lineage captured at stage boundaries as compact
  `(inputId, outputId)` associations; nested-format storage; backward/forward tracing as
  recursive joins over tap tables, evaluated lazily; trace results joinable back to raw
  records (`show`). Overhead target ≤ ~30% over baseline for capture.
- **BigDebug (ICSE '16):** simulated breakpoints (no real pause of all executors;
  on-demand state reconstruction from lineage + watchpoint), guarded watchpoints with
  driver-pushed predicate updates, crash culprit isolation with remediation
  (skip/modify record) without job restart, real-time latency alerting per record.
- Validation: port `lineage/src/test/.../LineageSuite.scala` first and keep the three
  original test programs from the fork's history green as the acceptance bar.
