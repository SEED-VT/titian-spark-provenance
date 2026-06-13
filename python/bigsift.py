# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""BigSift for PySpark: isolate the minimal fault-inducing rows of a base table.

Combines Titian's SQL provenance (``titian.py``) with delta debugging. Requires the
Titian jar on the classpath and
``spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension``.

Example::

    from bigsift import BigSift

    spark.read.csv(path, schema="category STRING, amount INT").createOrReplaceTempView("sales")
    result = BigSift(spark).debug(
        "sales",
        "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
        lambda r: r["total"] < 0)            # faulty = negative total
    print(result.fault_inducing_rows)         # the corrupt sales row(s)
"""

from titian import Titian


def ddmin(items, failing):
    """Zeller & Hildebrandt delta debugging: a 1-minimal subset of ``items`` for which
    ``failing(subset)`` holds, memoized so each subset is tested at most once."""
    items = list(items)
    memo = {}

    def fails(idx_tuple):
        if idx_tuple not in memo:
            memo[idx_tuple] = failing([items[i] for i in idx_tuple])
        return memo[idx_tuple]

    full = tuple(range(len(items)))
    if not items or not fails(full):
        return list(items)

    c_fail, n = full, 2
    progress = True
    while len(c_fail) >= 2 and progress:
        progress = False
        chunk = max(1, (len(c_fail) + n - 1) // n)
        subsets = [c_fail[i:i + chunk] for i in range(0, len(c_fail), chunk)]
        # reduce to a failing subset
        hit = next((s for s in subsets if fails(s)), None)
        if hit is not None:
            c_fail, n, progress = hit, 2, True
            continue
        # reduce to a failing complement
        comp = next((tuple(i for i in c_fail if i not in set(s))
                     for s in subsets
                     if fails(tuple(i for i in c_fail if i not in set(s)))), None)
        if comp is not None:
            c_fail, n, progress = comp, max(n - 1, 2), True
            continue
        if n < len(c_fail):
            n, progress = min(2 * n, len(c_fail)), True
    return [items[i] for i in c_fail]


class BigSiftResult:
    def __init__(self, fault_inducing_rows, faulty_outputs, provenance_size):
        self.fault_inducing_rows = fault_inducing_rows
        self.faulty_outputs = faulty_outputs
        self.provenance_size = provenance_size

    def __repr__(self):
        return ("BigSiftResult(fault_inducing_rows=%r, faulty_outputs=%d, "
                "provenance_size=%d)" % (self.fault_inducing_rows,
                                         len(self.faulty_outputs), self.provenance_size))


class BigSift:
    """Automated fault isolation for a Spark SQL query over a file-backed base table."""

    def __init__(self, spark):
        self.spark = spark
        self.titian = Titian(spark)

    def debug(self, base_table, query, test):
        """Isolate the minimal ``base_table`` rows that make ``query``'s output fail
        ``test`` (a predicate on output rows; True = faulty).

        The base table must be a file source so Titian captures its provenance, and the
        query's referenced columns must be present in the traced witnesses (the common
        single-source case). Returns a :class:`BigSiftResult`.
        """
        base = self.spark.table(base_table)
        base_schema = base.schema

        # 1. capture run -> faulty outputs
        self.titian.enable_capture()
        df = self.spark.sql(query)
        rows_with_ids = self.titian.collect_with_lineage(df)
        faulty = [(r, i) for r, i in rows_with_ids if test(r)]
        if not faulty:
            self.titian.disable_capture()
            return BigSiftResult([], [], 0)

        # 2. provenance: backward-trace the faulty outputs to base-table witnesses
        cursor = self.titian.trace(df, [i for _, i in faulty])
        hops = 0
        while not cursor.at_scan and hops < 32:
            cursor = cursor.go_back(0)
            hops += 1
        witnesses = cursor.show(full=True)        # list of dicts = candidate rows
        self.titian.release_lineage(df)
        self.titian.disable_capture()

        # 3. delta-debug: a subset "fails" if re-running the query with the base table
        #    restricted to those rows still yields a faulty output
        def reproduces(subset):
            if not subset:
                return False
            sub = self.spark.createDataFrame(
                [self._as_row(d, base_schema) for d in subset], base_schema)
            sub.createOrReplaceTempView(base_table)
            try:
                return any(test(r) for r in self.spark.sql(query).collect())
            finally:
                base.createOrReplaceTempView(base_table)

        if witnesses and reproduces(witnesses):
            cause = ddmin(witnesses, reproduces)
        else:
            cause = witnesses
        base.createOrReplaceTempView(base_table)
        return BigSiftResult(cause, [r for r, _ in faulty], len(witnesses))

    @staticmethod
    def _as_row(d, schema):
        from pyspark.sql import Row
        return Row(**{f.name: d.get(f.name) for f in schema.fields})
