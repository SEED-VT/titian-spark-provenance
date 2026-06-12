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
path. It proves the id/key-hash chain is consistent across levels; it does not
re-derive ground truth for every row (that re-execution oracle is a roadmap item).
```bash
# inspect one query's walk interactively
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage tpcds/data/sf0.2 tpcds/queries q3'
```
