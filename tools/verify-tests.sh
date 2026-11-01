#!/usr/bin/env bash
# Runs the test suite — which spins up a real embedded Kafka — against one Spark release
# per source variant. Compiling every version proves the APIs line up; this proves the
# behaviour does. It is deliberately not run for all 24: within a variant the code is
# identical and the Spark patch releases are API-compatible, so the extra runs would cost
# a lot of wall clock to re-prove the same thing.
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/verify
: > target/verify/tests.tsv
for v in "$@"; do
  python3 tools/retarget.py "$v" > /dev/null
  jt=$(python3 -c "import json;print(json.load(open('tools/versions.json'))['$v']['java'])")
  if [ "$jt" = "17" ]; then export JAVA_HOME="$HOME/.sdkman/candidates/java/17.0.18-tem"
  else export JAVA_HOME="$HOME/.sdkman/candidates/java/11.0.30-tem"; fi
  export PATH="$JAVA_HOME/bin:$PATH"
  log="target/verify/tests-$v.log"
  if timeout 1800 mvn -B test > "$log" 2>&1; then
    n=$(grep -oE "Tests: succeeded [0-9]+" "$log" | tail -1)
    echo -e "$v\tOK\t$n" >> target/verify/tests.tsv
    printf '  %-7s ok   %s\n' "$v" "$n"
  else
    echo -e "$v\tFAIL" >> target/verify/tests.tsv
    printf '  %-7s FAIL (%s)\n' "$v" "$log"
  fi
done
