# TPC-DS lineage coverage harness

- `queries/` — the 103 TPC-DS v1.4 query files exactly as shipped in Apache Spark's
  test suite (`sql/core/src/test/resources/tpcds/`, Apache License 2.0; TPC-DS is a
  benchmark specification of the Transaction Processing Performance Council).
- `gen_data.py` — generates the 24 tables as Parquet with DuckDB's built-in `dsdgen`
  port (`pip install duckdb`; no C toolchain). Output lands in `data/sf<sf>/`
  (gitignored).
- Runner: `edu.vt.bigdebug.examples.TPCDSCoverage` (in `examples/`). Per query:
  capture-off baseline, capture-on `collectWithLineage` (answers must match), one
  backward trace to a scan with witness resolution, then `releaseLineage`. Prints a
  PASS / UNSUPPORTED(operator) / ERROR scoreboard.

```bash
python3 tpcds/gen_data.py 0.2
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage'
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage tpcds/data/sf0.2 tpcds/queries q3,q88'
```

Queries land in UNSUPPORTED when their plan needs an operator outside Titian's
verified set (capture fails loudly, never silently wrong) — the scoreboard's
per-operator histogram is the working list for extending coverage.

## How verification works

Every query is checked two ways: capture must not change the answer, and the
captured lineage must walk back to real source rows.

```
                              q<N>.sql
                                 │
              ┌──────────────────┴──────────────────┐
              │ 1. BASELINE                         │ 2. CAPTURE
              │    spark.titian.sql.capture=false   │    spark.titian.sql.capture=true
              │    rows = spark.sql(q).collect()    │    (rows', ids) = collectWithLineage(q)
              └──────────────────┬──────────────────┘
                                 │
                  3. ANSWER CHECK:  rows == rows' ?
                       │                        │
                      yes                       no ──────────────► ERROR[MISMATCH]
                       │
                  4. TRACE one result row (its packed id) backward:
                                                                      result row
                     TapResult ──────────────── ids ───────────────► (rows'[0], ids[0])
                        │ goBack(0)
                     TapPostKeyed ─ key hash of the ORDER BY/GROUP BY/join key
                        │ goBack(0)                  ▲ recorded at runtime in
                     TapPreExchange ─ key hash ──────┘ executor BlockManagers
                        │ goBack(0)        (repeat per shuffle/broadcast/limit level,
                        ▼                   ≤ 24 hops)
                     TapScan ── cursor.atScan == true ?  ── no ────► ERROR[TRACE]
                        │
                  5. WITNESSES: cursor.show()
                     re-scans the Parquet source deterministically and
                     returns the matching input rows — must not throw
                        │
                  6. releaseLineage(q)   (drop this query's blocks)
                        │
                        ▼
                       PASS
```

Any step may instead raise `TitianUnsupportedOperatorException` while the plan is
prepared (step 2) — the query is then bucketed as `UNSUPPORTED(<operator>)`: Titian
refused to capture rather than record lineage it cannot stand behind. Everything
else unexpected becomes `ERROR[<exception>]`.

What each outcome certifies:

- **PASS** — capture is semantically invisible (identical answers), the capture
  graph parsed end-to-end, every hop of the backward walk resolved through the
  materialized lineage blocks, and the witnesses re-materialized from the source
  files.
- **UNSUPPORTED** — the loud-failure contract held: an out-of-scope operator was
  named before any lineage was recorded.
- **ERROR** — a real defect (or a scale-factor data artifact, e.g. q90's
  divide-by-zero, which fails in the baseline too).

The trace check follows branch 0 at every multi-input level — one root-to-scan
path. It proves the id/key-hash chain is consistent across levels. Ground truth is
the oracle's job:

## Recall / precision oracle (`--oracle`)

For every PASS query the oracle re-derives ground truth by re-execution:

```
        traced result row r ──(trace through EVERY branch)──► witnesses per table
                                                                    │
   for each traced table T:  T' = T ⋉ witnesses(T)                  │
   (null-safe semi-join on the scan's columns — full schema,        │
    multiple scans of one table union their row-id sets)            │
                                                                    ▼
   re-run the same SQL on the substituted views (capture off)
                                                                    │
        ┌───────────────────────────────────────────────────────────┤
        ▼                                                           ▼
   RECALL:  r must reappear identically.                    PRECISION signals:
   The witness set is SUFFICIENT — for aggregates           • witness-set size per
   this is sharp: one missing contributor changes             table (key-hash
   the SUM/COUNT/AVG.                                         granularity cost)
                                                            • leave-one-out: drop 1
                                                              witness, re-run — row
                                                              changed ⇒ SENSITIVE
                                                              (witness necessary)
```

```bash
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage tpcds/data/sf0.2 tpcds/queries --oracle'
# per query:  q3  PASS  recall=OK witnesses{date_dim=7,item=1,store_sales=9} loo=SENSITIVE
```

Current results (sf=0.2): **61/103 queries pass**; of those, every query the oracle can
exercise passes recall — **45 recall-OK** (5 of them the verified zero-input global
aggregate case), **0 recall-FAIL**, 16 skipped (scalar subqueries / empty results at
this scale factor). Leave-one-out: 27 sensitive, 6 insensitive.

The oracle earned its keep on first contact: it caught **three silent
wrong-lineage bugs** that every consistency check had passed — a fused
partial+final aggregate pair (no shuffle between) recording stale ids, ambiguous
positional ids across partitioner-aligned union children, and probe-id corruption
in fan-out broadcast joins (depth-first match traversal overwrites
`currentInputId` between matches). All three are fixed with dedicated taps
(fused-pair pre taps, per-union-child pre taps, probe-mark taps).

Oracle caveats, stated honestly:

- Recall is the pass/fail half. Precision is reported, not asserted — key-hash
  granularity returns whole key groups across shuffles *by design* (joins with
  duplicate keys, window peer sets), so witness sets can legitimately exceed the
  minimal contributing set.
- `loo=INSENSITIVE` is a signal, not a verdict: a dropped witness can be
  value-neutral (non-extremal MIN/MAX contributor, granularity over-approximation).
- Queries with scalar subqueries are skipped (`recall=SKIP`): the subquery would be
  re-evaluated over the filtered tables and change value for reasons unrelated to
  lineage.
- Duplicate full rows in a source are conflated by the value-based semi-join (a
  missing contributor that is a byte-identical copy of a witness can't be detected).
```bash
# inspect one query's walk interactively
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage tpcds/data/sf0.2 tpcds/queries q3 --oracle'
```
