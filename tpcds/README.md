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
