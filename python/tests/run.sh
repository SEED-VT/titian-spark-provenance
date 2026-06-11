#!/bin/sh
# Runs the PySpark end-to-end test against the local build.
# Prereqs: bin/sbt package; Spark dist + JDK 17 + Python 3.9+ under tools/ (or set
# SPARK_HOME / JAVA_HOME / PYSPARK_PYTHON yourself).
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# prefer the project-local JDK 17 (an inherited JAVA_HOME often points at JDK 8)
if [ -d "$ROOT/tools/jdk-17.0.19+10/Contents/Home" ]; then
  export JAVA_HOME="$ROOT/tools/jdk-17.0.19+10/Contents/Home"
fi
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$ROOT/tools/python/bin/python3}"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
SPARK_HOME="${SPARK_HOME:-$ROOT/tools/spark-4.1.2-bin-hadoop3}"

TITIAN_JAR="$ROOT/target/scala-2.13/titian_2.13-4.0.0-SNAPSHOT.jar"
FASTUTIL_JAR="$(find ~/Library/Caches/Coursier ~/.cache/coursier -name 'fastutil-8.5.15.jar' 2>/dev/null | head -1)"

exec "$SPARK_HOME/bin/spark-submit" \
  --master 'local[2]' \
  --jars "$TITIAN_JAR,$FASTUTIL_JAR" \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
  --py-files "$ROOT/python/titian.py" \
  "$ROOT/python/tests/test_titian_pyspark.py"
