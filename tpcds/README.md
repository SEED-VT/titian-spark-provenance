# TPC-DS lineage coverage harness

- `queries/` — the 103 TPC-DS v1.4 query files as shipped in Apache Spark's test suite.
- `gen_data.py` — generates the 24 tables as Parquet via DuckDB's `dsdgen`
  (`pip install duckdb`). Output lands in `data/sf<sf>/` (gitignored).
- Runner: `edu.vt.bigdebug.examples.TPCDSCoverage` (in `examples/`).

Full documentation — how verification works, the recall/precision oracle, and the
current scoreboard — is in the docs site: **[TPC-DS coverage](../docs/tpcds.md)**.

```bash
python3 tpcds/gen_data.py 0.2
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage --oracle'
```
