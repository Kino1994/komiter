#!/usr/bin/env bash
# Compiles the connector against every Spark release in tools/versions.json, and against
# both Scala binaries wherever Spark publishes both.
#
# This exists because the source variants are chosen by minor version, and a boundary that
# is right for the two releases it was derived from is not necessarily right for the ones
# in between. Rather than trust that, compile all of them.
#
#   tools/verify-all.sh              # everything
#   tools/verify-all.sh 3.5.0 3.5.1  # just these
#
# Results land in target/verify/<spark>-<binary>.log and are summarised at the end. The
# tree is left targeting whatever version ran last; retarget it before committing.
set -uo pipefail
cd "$(dirname "$0")/.."

JDK8_TARGET="$HOME/.sdkman/candidates/java/11.0.30-tem"   # Spark 3.x targets Java 8
JDK17="$HOME/.sdkman/candidates/java/17.0.18-tem"          # Spark 4.x targets Java 17

mkdir -p target/verify
RESULTS=target/verify/results.tsv
: > "$RESULTS"

versions() {
  python3 - "$@" <<'PY'
import json, sys
m = json.load(open("tools/versions.json"))
want = sys.argv[1:]
print("\n".join(v for v in m if not want or v in want))
PY
}

spec() {  # spec <version> <field>
  python3 - "$1" "$2" <<'PY'
import json, sys
m = json.load(open("tools/versions.json"))[sys.argv[1]]
f = sys.argv[2]
print(" ".join(sorted(m["scala"])) if f == "binaries" else m[f])
PY
}

for v in $(versions "$@"); do
  python3 tools/retarget.py "$v" > /dev/null || { echo -e "$v\t-\tRETARGET-FAIL" >> "$RESULTS"; continue; }

  java_target=$(spec "$v" java)
  if [ "$java_target" = "17" ]; then export JAVA_HOME="$JDK17"; else export JAVA_HOME="$JDK8_TARGET"; fi
  export PATH="$JAVA_HOME/bin:$PATH"

  for binary in $(spec "$v" binaries); do
    # The default binary needs no profile; the other one is what the profile exists for.
    if [ "$binary" = "$(spec "$v" default_binary)" ]; then profile=(); else profile=(-P"scala-$binary"); fi
    log="target/verify/$v-$binary.log"
    if timeout 900 mvn -B "${profile[@]}" test-compile > "$log" 2>&1; then
      echo -e "$v\t$binary\tOK" >> "$RESULTS"
      printf '  %-7s %-5s ok\n' "$v" "$binary"
    else
      echo -e "$v\t$binary\tFAIL" >> "$RESULTS"
      printf '  %-7s %-5s FAIL  (%s)\n' "$v" "$binary" "$log"
    fi
  done
done

echo
echo "=== resumen ==="
awk -F'\t' '{c[$3]++} END {for (k in c) printf "%-14s %d\n", k, c[k]}' "$RESULTS"
grep -c . "$RESULTS" | xargs echo "combinaciones probadas:"
if grep -q FAIL "$RESULTS"; then
  echo
  echo "fallos:"
  grep FAIL "$RESULTS" | awk -F'\t' '{printf "  Spark %-7s Scala %s\n", $1, $2}'
  exit 1
fi
