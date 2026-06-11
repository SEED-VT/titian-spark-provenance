#!/bin/sh
set -e

export SPARK_HOME="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export TITIAN_JAR="$(ls /opt/titian/target/scala-2.13/titian_*.jar | tail -1)"

case "${1:-lab}" in
  validate)
    # execute every notebook headlessly; any failed assertion fails the run
    cd /opt/titian/notebooks
    for nb in titian_sql.ipynb titian_pyspark.ipynb titian_rdd.ipynb; do
      echo "=== executing $nb"
      jupyter nbconvert --to notebook --execute --stdout \
        --ExecutePreprocessor.timeout=600 "$nb" > /dev/null
      echo "=== $nb OK"
    done
    echo "ALL NOTEBOOKS VALIDATED"
    ;;
  lab)
    cd /opt/titian/notebooks
    exec jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root \
      --NotebookApp.token=''
    ;;
  *)
    exec "$@"
    ;;
esac
