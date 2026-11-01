#!/usr/bin/env python3
"""Retarget the working tree at one Spark version.

    python3 tools/retarget.py 3.5.6

Rewrites pom.xml, the three Scala sources, the README and the demo so the whole tree
targets that Spark release. It is idempotent: running it twice, or running it on a tree
already targeting a different version, produces the same result either way.

Two things drive it, both in versions.json:

  * the dependency versions Apache Spark itself declares for that release — Scala, Kafka
    and the Java target are copied from Spark's own pom rather than chosen here, so the
    connector never pulls a Kafka client or a Scala runtime that differs from the one the
    Spark it plugs into was built against;

  * which of four source variants the release needs. The connector reaches into Spark
    internals, and those internals moved three times across the supported range:

      variant a  Spark 3.5.0 – 3.5.9   the original shape
      variant b  Spark 4.0.0 – 4.0.5   StreamSourceAwareSparkPlan arrives; Scala 2.13 only
      variant c  Spark 4.1.0 – 4.1.4   StreamExecution moves to ...streaming.runtime
      variant d  Spark 4.2.0 – 4.3.0   DataSourceRDDPartition holds one Option, not a Seq

Each difference is expressed below as a pair of alternative texts, and retargeting is a
matter of swapping whichever one is present for the one the target needs. Storing them as
literal alternatives rather than as patches is what makes the operation reversible and
order-independent.
"""

import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
MATRIX = json.loads((ROOT / "tools" / "versions.json").read_text())

INSPECTOR = ROOT / "src/main/scala/org/apache/spark/sql/kafka010/KafkaSourceInspector.scala"
LISTENER = ROOT / "src/main/scala/com/komiter/Komiter.scala"
SUITE = ROOT / "src/test/scala/com/komiter/KomiterSuite.scala"


# --------------------------------------------------------------------------------------
# The four axes. Each is a list of mutually exclusive alternatives; exactly one is present
# in the tree at any time, and retargeting replaces it with the one the variant calls for.
# --------------------------------------------------------------------------------------

# 1. Java collection converters. scala.jdk arrived in 2.13, so the variant that still has
#    to compile under 2.12 uses the older, deprecated-but-present spelling.
CONVERTERS = {
    "a": "import scala.collection.JavaConverters._",
    "bcd": "import scala.jdk.CollectionConverters._",
}

# 2. Where StreamExecution and StreamingQueryWrapper live.
STREAMING_PKG = {
    "ab": "import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingQueryWrapper}",
    "cd": "import org.apache.spark.sql.execution.streaming.runtime.{StreamExecution, StreamingQueryWrapper}",
}

# 3. How the SparkDataStream behind a streaming scan is recovered: imports, the doc block
#    that explains the choice, and the match case itself.
STREAM_IMPORTS = {
    "a": (
        "import org.apache.spark.sql.execution.{RDDScanExec, RowDataSourceScanExec, SparkPlan}\n"
        "import org.apache.spark.sql.execution.datasources.v2.{ContinuousScanExec, DataSourceRDDPartition, DataSourceV2ScanExecBase, MicroBatchScanExec}"
    ),
    "bcd": (
        "import org.apache.spark.sql.execution.{RDDScanExec, RowDataSourceScanExec, SparkPlan, StreamSourceAwareSparkPlan}\n"
        "import org.apache.spark.sql.execution.datasources.v2.{ContinuousScanExec, DataSourceRDDPartition, DataSourceV2ScanExecBase}"
    ),
}

STREAM_DOC = {
    "a": """   * On this Spark version the [[SparkDataStream]] is recovered by a typed pattern match
   * against `MicroBatchScanExec` and `ContinuousScanExec`, both of which expose a public
   * `stream` accessor. Spark 4.0 later introduced a `StreamSourceAwareSparkPlan` trait as a
   * unified, supported way to do this, but it does not exist here, so the typed match is
   * the only route available.""",
    "bcd": """   * Spark 4.0 introduced `StreamSourceAwareSparkPlan` as the public way for external code to
   * recover the [[SparkDataStream]] of a streaming scan, and `MicroBatchScanExec` implements
   * it. We use that trait first because it is the supported public API for this purpose.
   * `ContinuousScanExec` does not implement the trait, so it is still handled with a separate
   * pattern-match case against its public `stream` accessor. On Spark 3.3–3.5 the trait does
   * not exist and a typed match against `MicroBatchScanExec` is used for both branches.""",
}

STREAM_CASE = {
    "a": "          case mbse: MicroBatchScanExec => Option(mbse.stream)",
    "bcd": "          case ssa: StreamSourceAwareSparkPlan => ssa.getStream",
}

# 4. Whether DataSourceRDDPartition holds a Seq of input partitions or a single Option.
INPUT_PARTITION = {
    "abc": """          // `DataSourceRDDPartition` holds a `Seq[InputPartition]` on this Spark
          // version; `collect` keeps only the Kafka ones and the outer `flatMap`
          // flattens the result.
          e.inputPartitions.collect {""",
    "d": """          // Spark 4.2 changed `DataSourceRDDPartition` to hold a single
          // `inputPartition: Option[InputPartition]` instead of a `Seq`; `collect`
          // works the same over the Option and is flattened by the outer `flatMap`.
          e.inputPartition.collect {""",
}


# The official apache/kafka image only exists from 3.7.0. Spark 3.5 pins Kafka clients at
# 3.4.1, which has no image, so the demo runs the oldest official broker instead — brokers
# serve older clients fine, and it keeps one compose file for every version rather than
# dragging in a second image family with different conventions. Spark 4.x pins 3.9.x, which
# does have an image, so those get an exact match.
OLDEST_OFFICIAL_KAFKA_IMAGE = "3.7.0"


def kafka_image(kafka_version):
    parts = tuple(int(x) for x in kafka_version.split("."))
    return kafka_version if parts >= (3, 7, 0) else OLDEST_OFFICIAL_KAFKA_IMAGE


def swap(text, alternatives, wanted, what):
    """Replace whichever alternative is present with `wanted`.

    Raises if none is present — that means the tree has drifted from what this tool knows
    how to retarget, and silently leaving it alone would produce a build that looks
    retargeted but is not.
    """
    target = alternatives[wanted]
    for key, alt in alternatives.items():
        if alt in text:
            return text.replace(alt, target) if key != wanted else text
    raise SystemExit(f"retarget: no recognised form of {what} found; tree has drifted")


def pick(alternatives, variant):
    """Choose the alternative whose key contains this variant letter."""
    for key in alternatives:
        if variant in key:
            return key
    raise SystemExit(f"retarget: no alternative covers variant {variant!r}")


def retarget_sources(variant):
    conv = pick(CONVERTERS, variant)
    for path in (INSPECTOR, LISTENER, SUITE):
        text = path.read_text()
        text = swap(text, CONVERTERS, conv, f"the converters import in {path.name}")
        path.write_text(text)

    text = LISTENER.read_text()
    text = swap(text, STREAMING_PKG, pick(STREAMING_PKG, variant), "the streaming package import")
    LISTENER.write_text(text)

    text = INSPECTOR.read_text()
    text = swap(text, STREAM_IMPORTS, pick(STREAM_IMPORTS, variant), "the plan imports")
    text = swap(text, STREAM_DOC, pick(STREAM_DOC, variant), "the stream-recovery doc block")
    text = swap(text, STREAM_CASE, pick(STREAM_CASE, variant), "the stream-recovery match case")
    text = swap(text, INPUT_PARTITION, pick(INPUT_PARTITION, variant), "the input-partition access")
    INSPECTOR.write_text(text)


def render_pom(spark, spec):
    """Emit pom.xml.

    The artifactId carries ${scala.binary.version} and a `scala-2.13` profile flips it, which
    is how Spark publishes its own cross-built artifacts. Where Spark ships both binaries the
    profile is emitted; where it ships only 2.13 there is nothing to cross-build and the
    profile is left out rather than sitting there unusable.

    The `release` profile is always present. It adds what Maven Central requires on top of
    the plain jar (sources, scaladoc as the javadoc jar, GPG signatures) and hands the result
    to the Central Portal; tools/publish.sh is what activates it. Keeping it in a profile is
    what lets an ordinary build run without a signing key.
    """
    default = spec["default_binary"]
    cross = len(spec["scala"]) > 1
    cross_profile = ""
    if cross:
        cross_profile = f"""
        <!-- Cross-build against the other Scala binary Spark {spark} publishes:
             mvn -Pscala-2.13 ... produces komiter_2.13 instead of komiter_2.12. -->
        <profile>
            <id>scala-2.13</id>
            <properties>
                <scala.version>{spec['scala']['2.13']}</scala.version>
                <scala.binary.version>2.13</scala.binary.version>
            </properties>
        </profile>"""

    profile = f"""
    <profiles>
        <!-- Publishing to Maven Central: tools/publish.sh runs `mvn -Prelease deploy`. -->
        <profile>
            <id>release</id>
            <build>
                <plugins>
                    <plugin>
                        <groupId>net.alchim31.maven</groupId>
                        <artifactId>scala-maven-plugin</artifactId>
                        <executions>
                            <execution>
                                <id>scaladoc-jar</id>
                                <goals>
                                    <goal>add-source</goal>
                                    <goal>doc-jar</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-source-plugin</artifactId>
                        <version>3.3.1</version>
                        <executions>
                            <execution>
                                <id>attach-sources</id>
                                <goals>
                                    <goal>jar-no-fork</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-gpg-plugin</artifactId>
                        <version>3.2.8</version>
                        <executions>
                            <execution>
                                <id>sign-artifacts</id>
                                <phase>verify</phase>
                                <goals>
                                    <goal>sign</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <groupId>org.sonatype.central</groupId>
                        <artifactId>central-publishing-maven-plugin</artifactId>
                        <version>0.12.0</version>
                        <extensions>true</extensions>
                        <configuration>
                            <publishingServerId>central</publishingServerId>
                            <autoPublish>${{central.autoPublish}}</autoPublish>
                            <waitUntil>validated</waitUntil>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>{cross_profile}
    </profiles>
"""

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!--
  Generated by tools/retarget.py for Spark {spark}. Do not hand-edit the versions below:
  scala.version, kafka.version and the Java target are copied from Apache Spark's own pom
  for this release, so the connector cannot drag in a Kafka client or Scala runtime that
  differs from the one Spark {spark} was built against.
-->
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.komiter</groupId>
    <artifactId>komiter_${{scala.binary.version}}</artifactId>
    <version>{spark}</version>
    <packaging>jar</packaging>

    <name>Komiter</name>
    <description>Kafka offset committer for Spark Structured Streaming, built for Spark {spark}</description>
    <url>https://komiter.com</url>

    <licenses>
        <license>
            <name>Apache License, Version 2.0</name>
            <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
            <distribution>repo</distribution>
        </license>
    </licenses>

    <developers>
        <developer>
            <id>Kino1994</id>
            <name>Kino1994</name>
            <url>https://github.com/Kino1994</url>
        </developer>
    </developers>

    <scm>
        <url>https://github.com/Kino1994/komiter/tree/v{spark}</url>
        <connection>scm:git:https://github.com/Kino1994/komiter.git</connection>
        <developerConnection>scm:git:ssh://git@github.com/Kino1994/komiter.git</developerConnection>
        <tag>v{spark}</tag>
    </scm>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>{spec['java']}</maven.compiler.source>
        <maven.compiler.target>{spec['java']}</maven.compiler.target>
        <scala.version>{spec['scala'][default]}</scala.version>
        <scala.binary.version>{default}</scala.binary.version>
        <spark.version>{spark}</spark.version>
        <kafka.version>{spec['kafka']}</kafka.version>
        <scalatest.version>3.2.19</scalatest.version>
        <!-- Leave a release as a draft in the Central Portal for review; CI passes true. -->
        <central.autoPublish>false</central.autoPublish>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${{scala.binary.version}}</artifactId>
            <version>${{spark.version}}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_${{scala.binary.version}}</artifactId>
            <version>${{spark.version}}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${{kafka.version}}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${{scala.binary.version}}</artifactId>
            <version>${{spark.version}}</version>
            <type>test-jar</type>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${{scala.binary.version}}</artifactId>
            <version>${{spark.version}}</version>
            <type>test-jar</type>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_${{scala.binary.version}}</artifactId>
            <version>${{spark.version}}</version>
            <type>test-jar</type>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-catalyst_${{scala.binary.version}}</artifactId>
            <version>${{spark.version}}</version>
            <type>test-jar</type>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_${{scala.binary.version}}</artifactId>
            <version>${{kafka.version}}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.scalatest</groupId>
            <artifactId>scalatest_${{scala.binary.version}}</artifactId>
            <version>${{scalatest.version}}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>4.9.2</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <scalaVersion>${{scala.version}}</scalaVersion>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.scalatest</groupId>
                <artifactId>scalatest-maven-plugin</artifactId>
                <version>2.2.0</version>
                <executions>
                    <execution>
                        <id>test</id>
                        <goals>
                            <goal>test</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
{profile}</project>
"""


def retarget_readme(spark, spec):
    path = ROOT / "README.md"
    text = path.read_text()
    default = spec["default_binary"]
    binaries = " · ".join(sorted(spec["scala"]))
    # Spark's pom spells the Java 8 target "1.8"; the pom keeps that spelling to match
    # Spark exactly, but prose reads better as "8".
    java = "8" if spec["java"] == "1.8" else spec["java"]

    table = (
        "| Spark | Scala | Java | Kafka clients |\n"
        "|-------|-------|------|---------------|\n"
        f"| {spark} | {binaries} | {java} | {spec['kafka']} |"
    )
    text = re.sub(
        r"\| Spark \| Scala \| Java \| Kafka clients \|\n\|[-|]+\|\n\|[^\n]*\|",
        table,
        text,
    )
    # tolera tanto komiter como komiter_2.12 / komiter_2.13
    text = re.sub(r"<artifactId>komiter(_[\d.]+)?</artifactId>",
                  f"<artifactId>komiter_{default}</artifactId>", text)
    text = re.sub(r"<version>[\d.]+</version>", f"<version>{spark}</version>", text)
    text = re.sub(r"(?:io\.crumb|crumb|com\.komiter):komiter(?:_[\d.]+)?:[\d.]+", f"com.komiter:komiter_{default}:{spark}", text)
    text = re.sub(r"spark-sql-kafka-0-10_[\d.]+:[\d.]+",
                  f"spark-sql-kafka-0-10_{default}:{spark}", text)
    text = re.sub(r"komiter(?:_[\d.]+)?-[\d.]+\.jar", f"komiter_{default}-{spark}.jar", text)
    scala_badge = binaries.replace(" · ", "%20%C2%B7%20")
    text = re.sub(r'badge/Spark-[\d.]+-(\w+)\?style=flat-square" alt="Spark [\d.]+"',
                  rf'badge/Spark-{spark}-\g<1>?style=flat-square" alt="Spark {spark}"', text)
    text = re.sub(r'badge/Scala-[\d.%\w]+-(\w+)\?style=flat-square" alt="Scala [^"]*"',
                  rf'badge/Scala-{scala_badge}-\g<1>?style=flat-square" alt="Scala {binaries}"', text)
    text = re.sub(r'badge/Java-[\d.]+-(\w+)\?style=flat-square" alt="Java [\d.]+"',
                  rf'badge/Java-{java}-\g<1>?style=flat-square" alt="Java {java}"', text)
    path.write_text(text)


def retarget_demo(spark, spec):
    path = ROOT / "demo" / "pom.xml"
    if not path.exists():
        return
    text = path.read_text()
    default = spec["default_binary"]
    text = re.sub(r"<scala\.version>[\d.]+</scala\.version>",
                  f"<scala.version>{spec['scala'][default]}</scala.version>", text)
    text = re.sub(r"<scala\.compat\.version>[\d.]+</scala\.compat\.version>",
                  f"<scala.compat.version>{default}</scala.compat.version>", text)
    text = re.sub(r"<spark\.version>[\d.]+</spark\.version>",
                  f"<spark.version>{spark}</spark.version>", text)
    # `komiter` gained a Scala suffix when the build went cross-version; tolerate the
    # unsuffixed spelling so an old demo pom is fixed rather than silently left broken.
    # `komiter-demo` must not be caught by this, hence the anchored closing tag.
    text = re.sub(r"<artifactId>komiter(?:_[\d.]+)?</artifactId>",
                  f"<artifactId>komiter_{default}</artifactId>", text)
    text = re.sub(r"(<artifactId>komiter[^<]*</artifactId>\s*<version>)[\d.]+", rf"\g<1>{spark}", text)
    text = re.sub(r"<maven\.compiler\.(source|target)>[\d.]+</maven\.compiler\.\1>",
                  lambda m: f"<maven.compiler.{m.group(1)}>{spec['java']}</maven.compiler.{m.group(1)}>", text)
    path.write_text(text)

    compose = ROOT / "demo" / "docker-compose.yml"
    if compose.exists():
        img = kafka_image(spec["kafka"])
        if img != spec["kafka"]:
            note = (f"    # Kafka {spec['kafka']} is what Spark {spark} builds against, but the official\n"
                    f"    # image only goes back to {OLDEST_OFFICIAL_KAFKA_IMAGE}; a newer broker serves an older\n"
                    f"    # client fine, so that is what runs here.\n")
        else:
            note = f"    # Matches the Kafka client Spark {spark} builds against.\n"
        # Drop any note this tool wrote before, then the image line, and put both back.
        # Matching the previous note explicitly is what keeps repeated runs idempotent.
        c = re.sub(r"(?:^[ \t]*#[ \t]*(?:Kafka [\d.]+ is what|image only goes back|client fine, so|Matches the Kafka)[^\n]*\n)*"
                   r"^[ \t]*image: apache/kafka:[\d.]+[ \t]*$",
                   note + f"    image: apache/kafka:{img}",
                   compose.read_text(), flags=re.M)
        compose.write_text(c)

    readme = ROOT / "demo" / "README.md"
    if readme.exists():
        t = readme.read_text()
        t = re.sub(r"(?:io\.crumb|crumb|com\.komiter):komiter(?:_[\d.]+)?:[\d.]+", f"com.komiter:komiter_{default}:{spark}", t)
        readme.write_text(t)


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in MATRIX:
        known = " ".join(MATRIX)
        raise SystemExit(f"usage: retarget.py <spark-version>\nknown versions: {known}")
    spark = sys.argv[1]
    spec = MATRIX[spark]

    retarget_sources(spec["variant"])
    (ROOT / "pom.xml").write_text(render_pom(spark, spec))
    retarget_readme(spark, spec)
    retarget_demo(spark, spec)

    binaries = ", ".join(sorted(spec["scala"]))
    print(f"targeting Spark {spark}: variant {spec['variant']}, Scala {binaries}, "
          f"Kafka {spec['kafka']}, Java {spec['java']}")


if __name__ == "__main__":
    main()
