# BigSift — automated fault isolation

**BigSift** (SoCC '17, FSE '18 demo) finds the *minimal set of input records* that
reproduce a faulty output of a Spark job. It combines Titian's data provenance with
**delta debugging**: provenance shrinks the search space from the whole input to just
the records feeding the faulty outputs, and delta debugging minimizes that to the
1-minimal fault-inducing records — re-running the job on subsets and re-applying a
user **test oracle**, with bitmap-memoized test verdicts.

You give it a **job**, a **test oracle** (a predicate marking output records faulty),
and the **input**; it returns the minimal fault-inducing input records.

```
input + job + test  ──►  run with capture  ──►  faulty outputs (per test)
                                │
                         backward provenance (Titian)
                                │
                     candidate fault-inducing inputs
                                │
                         delta debugging (ddmin):
                     re-run job on subsets, re-test, memoize
                                │
                                ▼
                  1-minimal fault-inducing input records
```

## Spark SQL / DataFrames

`BigSiftSQL.debug` isolates the minimal rows of a file-backed base table. It reuses the
same provenance-trace-and-restrict machinery as the TPC-DS re-execution oracle, so the
provenance step is exact.

```scala
import org.apache.spark.sql.bigsift.BigSiftSQL

spark.read.parquet(salesPath).createOrReplaceTempView("sales")   // file-backed
val result = BigSiftSQL.debug(spark, "sales",
  "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
  (r: Row) => r.getLong(1) < 0)                  // faulty = negative total

result.faultInducingRows.foreach(println)         // the corrupt sales row(s)
result.provenanceSize                             // candidates before ddmin
```

## PySpark

```python
from bigsift import BigSift

spark.read.csv(path, schema="category STRING, amount INT").createOrReplaceTempView("sales")
result = BigSift(spark).debug(
    "sales",
    "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
    lambda r: r["total"] < 0)
print(result.fault_inducing_rows)                 # the corrupt row(s)
```

Ship `python/bigsift.py` alongside `python/titian.py` (`--py-files titian.py,bigsift.py`).

## Scala RDD

Matches the original FSE '18 API (`runWithBigSift`). The job is a transformation of the
input; the test is a predicate on output records.

```scala
import org.apache.spark.bigsift.{BigSift, TestOracle}

val lc = new LineageContext(sc)
val result = new BigSift(lc).run(
  lc.textFile("transit.csv"),
  in => in.map(parseToKV).filter(_._2 < 45).reduceByKey(_ + _),
  (out: ((String, String), Int)) => out._2 < 0)   // faulty = negative total
result.faultInducingInputs.foreach(println)         // the culprit line(s)
```

## Predefined test oracles

The UI's built-in oracles (Scala `TestOracle`) build a fixed faulty predicate from the
*original* output — minimum, maximum, k-sigma-from-median, NaN/Null:

```scala
new BigSift(lc).runWithOracle(input, job,
  TestOracle.min[((String, String), Int)](_._2.toDouble))   // explain the minimum
```

## How a subset is judged "faulty"

The delta-debugging test re-runs the job with the input restricted to the candidate
subset and applies the oracle to the output: if any output row is still faulty, the
subset reproduces the failure. Distribution oracles (k-sigma, min/max) fix their
threshold from the original full output so the predicate stays stable across re-runs.

## Notes & limitations

- **SQL/PySpark** require the base table to be a **file source** (Parquet/CSV) so
  Titian captures its provenance; in-memory `LocalRelation` views are not captured.
  The query's referenced columns must appear in the traced witnesses (the common
  single-base-table case).
- **RDD** provenance pre-shrink is **best-effort**: the trace is validated by
  re-running, and falls back to the full input if it under-resolves — so the isolated
  culprit is always correct, but the candidate set may not always be pre-shrunk.
- Delta debugging re-runs the job many times; cost scales with the candidate-set size,
  which is why the provenance pre-shrink matters. Test verdicts are memoized so no
  subset is re-tested.
- BigSift builds on the same coverage and fail-loud guarantees as Titian SQL capture.
