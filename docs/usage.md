# Usage

Register the SQL extension once at session build (`spark.sql.extensions`), then toggle
capture per query with `spark.titian.sql.capture`. The RDD API wraps a
`SparkContext`. All three APIs expose the same backward/forward trace model.

## Spark SQL / DataFrames

```scala
// spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension
spark.conf.set("spark.titian.sql.capture", "true")

val df = spark.sql(
  """SELECT c.name, SUM(o.amount) AS total
    |FROM orders o JOIN customers c ON o.cid = c.cid
    |GROUP BY c.name""".stripMargin)

import org.apache.spark.sql.lineage.TitianSQL
val output = TitianSQL.collectWithLineage(df)        // Array[(Row, packedId)]
val bobId  = output.find(_._1.getString(0) == "Bob").get._2

var cursor = TitianSQL.trace(df, Seq(bobId))
cursor = cursor.goBack()                  // through the aggregation exchange
val orders    = cursor.goBack(0).show()   // culprit rows in orders
val customers = cursor.goBack(1).show()   // culprit rows in customers

TitianSQL.releaseLineage(df)              // drop this query's lineage blocks
```

Entry point: [`TitianSQL`](api/org/apache/spark/sql/lineage/TitianSQL$.html)
(`collectWithLineage`, `trace`, `traceAllSources`, `lineageSize`, `releaseLineage`);
the walk is a [`TraceCursor`](api/org/apache/spark/sql/lineage/TraceCursor.html)
(`goBack(branch)`, `goNext`, `show`).

## RDD API

```scala
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

val lc = new LineageContext(sparkContext)
lc.setCaptureLineage(true)

val totals = lc.textFile("sales.txt", 4)
  .map { line => val p = line.split(","); (p(0), p(1).toInt) }
  .reduceByKey(_ + _)
val output = totals.collectWithId()
lc.setCaptureLineage(false)

// trace the suspicious aggregate back to its exact input lines
var lin = totals.getLineage().filter(_ == suspiciousId)
lin = lin.goBack().goBack()
lin.show().collect().foreach(println)   // the culprit source lines
```

## PySpark

```python
# spark-submit --jars titian.jar,fastutil.jar \
#   --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
#   --py-files titian.py your_app.py
from titian import Titian

t = Titian(spark)
t.enable_capture()

df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
rows_with_ids = t.collect_with_lineage(df)

bad_id = next(i for r, i in rows_with_ids if r.total > 100000)
cursor = t.trace(df, [bad_id]).go_back()
print(cursor.show(full=True))     # witness source records, as dicts
t.release_lineage(df)
```

Python UDFs are traced *exactly* (the eval nodes are 1:1; Titian re-threads record ids
across their batching). Cached DataFrames (`df.cache()`) act as source boundaries:
traces stop at, and `show()` re-reads, the cached rows. The end-to-end test is
`python/tests/run.sh`.

Runnable demos live in `examples/` (`SalesAnalysis`, `OrderCustomerJoin`).

## Deploying on a cluster

```bash
spark-submit \
  --jars titian.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
  --conf spark.titian.sql.capture=true \
  your-app.jar
```

Everything else is `provided` by Spark. Lineage blocks live in executor
BlockManagers — **avoid dynamic allocation / executor preemption during a
capture-and-trace session** (losing an executor loses its lineage partitions), and
release per-query lineage with `releaseLineage` when done. See
[Getting started](install.md#cluster-deployment-notes) for the full notes.
