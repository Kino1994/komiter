#!/usr/bin/env bash
# Full audit of every supported Spark release. For each one:
#
#   1. asserts the generated pom's Scala, Kafka and Java target are exactly what Apache
#      Spark declares for that release — checked against Spark's own published pom, not
#      against versions.json, so a mistake in the matrix itself is caught rather than
#      confirmed;
#   2. compiles main and test sources for every Scala binary Spark ships;
#   3. installs the artifact and compiles the demo against it, which is the only thing
#      that proves the demo's own dependencies and the Scala suffix line up.
#
#   tools/audit.sh              # everything
#   tools/audit.sh 3.5.0 4.2.0  # just these
#
# Per-step logs in target/audit/. Exits non-zero if anything fails.
set -uo pipefail
cd "$(dirname "$0")/.."

JDK_FOR_JAVA8="$HOME/.sdkman/candidates/java/11.0.30-tem"
JDK17="$HOME/.sdkman/candidates/java/17.0.18-tem"

mkdir -p target/audit
RESULTS=target/audit/results.tsv
: > "$RESULTS"

note() { echo -e "$1\t$2\t$3" >> "$RESULTS"; printf '  %-7s %-22s %s\n' "$1" "$2" "$3"; }

versions() {
  python3 - "$@" <<'PY'
import json, sys
m = json.load(open("tools/versions.json"))
want = sys.argv[1:]
print("\n".join(v for v in m if not want or v in want))
PY
}

for v in $(versions "$@"); do
  python3 tools/retarget.py "$v" > /dev/null || { note "$v" "retarget" "FAIL"; continue; }

  # ---- 1. the pom against Spark's own pom -------------------------------------------
  if python3 tools/check-deps.py "$v" > "target/audit/$v-deps.log" 2>&1; then
    note "$v" "deps vs spark pom" "ok"
  else
    note "$v" "deps vs spark pom" "MISMATCH"
    sed 's/^/        /' "target/audit/$v-deps.log"
    continue
  fi

  java_target=$(python3 -c "import json;print(json.load(open('tools/versions.json'))['$v']['java'])")
  if [ "$java_target" = "17" ]; then export JAVA_HOME="$JDK17"; else export JAVA_HOME="$JDK_FOR_JAVA8"; fi
  export PATH="$JAVA_HOME/bin:$PATH"

  default=$(python3 -c "import json;print(json.load(open('tools/versions.json'))['$v']['default_binary'])")
  binaries=$(python3 -c "import json;print(' '.join(sorted(json.load(open('tools/versions.json'))['$v']['scala'])))")

  # ---- 2. compile every binary --------------------------------------------------------
  ok=1
  for binary in $binaries; do
    if [ "$binary" = "$default" ]; then profile=(); else profile=(-P"scala-$binary"); fi
    if timeout 900 mvn -B "${profile[@]}" test-compile > "target/audit/$v-$binary.log" 2>&1; then
      note "$v" "compile scala $binary" "ok"
    else
      note "$v" "compile scala $binary" "FAIL"; ok=0
    fi
  done
  [ "$ok" = 1 ] || continue

  # ---- 3. install, then build the demo against the real artifact ----------------------
  if timeout 900 mvn -B install -DskipTests > "target/audit/$v-install.log" 2>&1; then
    # The Java target only matters if it actually reaches the Scala compiler. Read the
    # class-file major version out of the jar rather than trusting the pom property:
    # a Spark 3.5 artifact emitted as Java 11 bytecode would not run on the Java 8
    # clusters that line targets, and nothing else in the build would notice.
    want=$(python3 -c "print({'1.8':52,'8':52,'11':55,'17':61}['$java_target'])")
    jar=$(ls target/komiter_*-"$v".jar 2>/dev/null | head -1)
    got=$(unzip -p "$jar" com/komiter/Komiter.class 2>/dev/null | head -c 8 | od -An -tu1 | awk '{print $7*256+$8}')
    if [ "$got" = "$want" ]; then
      note "$v" "bytecode java $java_target" "ok"
    else
      note "$v" "bytecode java $java_target" "WRONG (major $got, expected $want)"
    fi

    if timeout 900 mvn -B -f demo/pom.xml clean compile > "target/audit/$v-demo.log" 2>&1; then
      note "$v" "demo compile" "ok"
    else
      note "$v" "demo compile" "FAIL"
    fi
  else
    note "$v" "install" "FAIL"
  fi
done

echo
echo "=== resumen ==="
awk -F'\t' '{c[$3]++} END {for (k in c) printf "  %-10s %d\n", k, c[k]}' "$RESULTS"
if grep -qE "FAIL|MISMATCH" "$RESULTS"; then
  echo
  echo "problemas:"
  grep -E "FAIL|MISMATCH" "$RESULTS" | awk -F'\t' '{printf "  Spark %-7s %-22s %s\n", $1, $2, $3}'
  exit 1
fi
echo "  todo correcto"
