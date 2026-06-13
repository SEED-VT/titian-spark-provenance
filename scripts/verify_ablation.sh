#!/bin/sh
# Proves every ablation flag is semantics-preserving: the full
# Scala suite asserts exact lineage (exact witness sets, exact traces), so a green run
# with a flag forced on means that legacy path produces identical lineage to the
# optimized one. Spark auto-loads spark.* JVM system properties into SparkConf, so
# -Dspark.titian.ablation=<flags> reaches every test session.
#
#   sh scripts/verify_ablation.sh
set -e
cd "$(dirname "$0")/.."

CONFIGS="
none:
legacyHash:legacyHash
boxedCombiner:boxedCombiner
boxedJoinBuffer:boxedJoinBuffer
boxedBlocks:boxedBlocks
driverTrace:driverTrace
fully-legacy:legacyHash,boxedCombiner,boxedJoinBuffer,boxedBlocks,driverTrace
"

PASS=0; FAIL=0; SUMMARY=""
for entry in $CONFIGS; do
  name="${entry%%:*}"; flags="${entry#*:}"
  echo "=================================================================="
  echo "  verifying ablation config: $name  [flags: ${flags:-none}]"
  echo "=================================================================="
  if bin/sbt -batch "set Test/javaOptions += \"-Dspark.titian.ablation=$flags\"" test \
       > "/tmp/ablation-verify-$name.log" 2>&1; then
    line=$(grep -E 'Tests: succeeded' "/tmp/ablation-verify-$name.log" | tail -1)
    echo "  PASS  $line"
    SUMMARY="$SUMMARY\n  $name: PASS ($line)"
    PASS=$((PASS+1))
  else
    echo "  FAIL  (see /tmp/ablation-verify-$name.log)"
    grep -E 'FAILED|Exception' "/tmp/ablation-verify-$name.log" | head -3
    SUMMARY="$SUMMARY\n  $name: FAIL"
    FAIL=$((FAIL+1))
  fi
done

echo "\n================ ablation semantic verification ================"
printf "%b\n" "$SUMMARY"
echo "  configs passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ] || exit 1
