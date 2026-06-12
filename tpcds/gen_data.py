#!/usr/bin/env python3
"""Generate TPC-DS data as Parquet using DuckDB's dsdgen port (no C toolchain).

    pip install duckdb
    python3 tpcds/gen_data.py [scale_factor] [output_dir]

Defaults: scale factor 0.2 (~200 MB raw), output tpcds/data/sf<sf>/.
One Parquet file per table; the coverage runner registers each as a temp view.
"""
import sys
import os

import duckdb


def main() -> None:
    sf = sys.argv[1] if len(sys.argv) > 1 else "0.2"
    out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
        os.path.dirname(__file__), "data", f"sf{sf}")
    os.makedirs(out, exist_ok=True)

    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    print(f"generating TPC-DS sf={sf} ...")
    con.execute(f"CALL dsdgen(sf={sf})")
    # DuckDB's dsdgen names this column with an _sk suffix; Spark's TPC-DS queries
    # (and the spec) use the bare name
    con.execute("ALTER TABLE customer "
                "RENAME COLUMN c_last_review_date_sk TO c_last_review_date")
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    for t in tables:
        path = os.path.join(out, f"{t}.parquet")
        con.execute(f"COPY {t} TO '{path}' (FORMAT PARQUET)")
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"  {t:<24} {n:>10} rows -> {path}")
    print(f"done: {len(tables)} tables in {out}")


if __name__ == "__main__":
    main()
