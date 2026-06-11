# End-to-end PySpark test for Titian SQL provenance, including Python UDFs.
#
# Run via spark-submit (see python/tests/run.sh):
#   spark-submit --jars titian.jar,fastutil.jar \
#     --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
#     --py-files python/titian.py python/tests/test_titian_pyspark.py

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

from titian import Titian

passed = []


def check(name, cond, detail=""):
    if cond:
        passed.append(name)
        print("PASS  %s" % name)
    else:
        print("FAIL  %s  %s" % (name, detail))
        sys.exit(1)


spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
t = Titian(spark)

base = os.path.dirname(os.path.abspath(__file__))
sales_dir = os.path.join(base, "..", "..", "src", "test", "resources", "sales_parts")
spark.read.schema("category STRING, amount INT").csv(sales_dir) \
    .createOrReplaceTempView("sales")

t.enable_capture()

# ---- 1. aggregate trace from PySpark --------------------------------------
df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
out = t.collect_with_lineage(df)
totals = {r.category: r.total for r, _ in out}
check("aggregate totals", totals["electronics"] == 101109 and totals["toys"] == 280,
      str(totals))

elec_id = next(i for r, i in out if r.category == "electronics")
cursor = t.trace(df, [elec_id]).go_back()
check("cursor at scan", cursor.at_scan)
witnesses = cursor.show()
check("aggregate witnesses",
      sorted(w["amount"] for w in witnesses) == [310, 380, 420, 99999],
      str(witnesses))

# forward retrace
up = cursor.go_next()
check("forward retrace", len(up.ids) == 1, str(up.ids))

# ---- 2. Python UDF in the pipeline (BatchEvalPython taps) ------------------
plus_fee = udf(lambda a: a + 7, "int")
df2 = spark.table("sales") \
    .withColumn("amt2", plus_fee(col("amount"))) \
    .groupBy("category").sum("amt2")
out2 = t.collect_with_lineage(df2)
totals2 = {r["category"]: r["sum(amt2)"] for r, _ in out2}
# each category has 4 rows: sum + 4*7
check("python-udf totals",
      totals2["electronics"] == 101109 + 28 and totals2["groceries"] == 355 + 28,
      str(totals2))

g_id = next(i for r, i in out2 if r["category"] == "groceries")
w2 = t.trace(df2, [g_id]).go_back().show()
check("python-udf witnesses",
      sorted(w["amount"] for w in w2) == [70, 80, 95, 110], str(w2))

# ---- 3. release lineage -----------------------------------------------------
t.release_lineage(df2)
check("release lineage", len(t.trace(df2, [g_id]).ids) == 0)

print("ALL %d PYSPARK TESTS PASSED" % len(passed))
spark.stop()
