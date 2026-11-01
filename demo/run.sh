#!/usr/bin/env bash
# Compiles the demo and runs it against the Kafka started by docker-compose.
#
# Spark on JDK 17+ needs a set of --add-opens that spark-submit would normally supply. They
# are only added when the JVM is actually 17 or newer: the Spark 3.x line targets Java 8 and
# is commonly run on a JDK 8 that would refuse the flags outright.
set -euo pipefail
cd "$(dirname "$0")"

CP_FILE=target/classpath.txt
mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile="$CP_FILE"

# Probe the same `java` that will run the job, not one derived from JAVA_HOME, or the
# check could describe a different JVM than the one that ends up executing. "1.8.0_402"
# reports as 1.8 and must read as 8, not 1.
JAVA_MAJOR=$(java -version 2>&1 \
  | sed -n 's/.*version "\([0-9][0-9]*\)\.\([0-9][0-9]*\).*/\1 \2/p' | head -1 \
  | awk '{ print ($1 == 1 ? $2 : $1) }')

ADD_OPENS=()
if [ "${JAVA_MAJOR:-8}" -ge 17 ]; then
  ADD_OPENS=(
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/java.io=ALL-UNNAMED
    --add-opens=java.base/java.net=ALL-UNNAMED
    --add-opens=java.base/java.nio=ALL-UNNAMED
    --add-opens=java.base/java.util=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
    --add-opens=java.base/sun.security.action=ALL-UNNAMED
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
    -Djdk.reflect.useDirectMethodHandle=false
    -Dio.netty.tryReflectionSetAccessible=true
  )
fi

exec java "${ADD_OPENS[@]}" \
  -cp "target/classes:$(cat "$CP_FILE")" \
  "${MAIN_CLASS:-KomiterDemo}"
