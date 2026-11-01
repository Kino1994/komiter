#!/usr/bin/env python3
"""Assert the generated pom matches what Apache Spark declares for the target release.

    python3 tools/check-deps.py 3.5.6

Fetches Spark's own published pom and compares Scala, Kafka and the Java target against
what pom.xml currently says. It deliberately does *not* trust versions.json: that file is
the input to generation, so checking against it would only prove the generator ran, not
that the numbers in it are right. Comparing against Spark closes that loop.

Exits non-zero and prints the differences if anything is off.
"""

import json
import pathlib
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

NS = "{http://maven.apache.org/POM/4.0.0}"
ROOT = pathlib.Path(__file__).resolve().parent.parent
CACHE = ROOT / "target" / "audit" / "spark-poms"
BASE = "https://repo1.maven.org/maven2/org/apache/spark/spark-parent_{sc}/{v}/spark-parent_{sc}-{v}.pom"


def spark_pom(binary, version):
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / f"{binary}-{version}.pom"
    if not path.exists():
        try:
            path.write_bytes(urllib.request.urlopen(
                BASE.format(sc=binary, v=version), timeout=60).read())
        except urllib.error.HTTPError:
            return None
    return ET.parse(path).getroot()


def properties(el):
    p = el.find(f"{NS}properties")
    return {c.tag.replace(NS, ""): (c.text or "").strip() for c in p} if p is not None else {}


def spark_declares(binary, version):
    """What Spark itself says, for this Scala binary.

    For the _2.13 poms of the Spark 3.x line the effective scala.version lives in the
    `scala-2.13` profile while the top-level property still carries the 2.12 value, so the
    profile has to be layered on top or the answer is silently the wrong Scala.
    """
    root = spark_pom(binary, version)
    if root is None:
        return None
    props = properties(root)
    if binary == "2.13":
        for prof in root.iter(f"{NS}profile"):
            pid = prof.find(f"{NS}id")
            if pid is not None and pid.text == "scala-2.13":
                props = {**props, **properties(prof)}
    return {
        "scala": props.get("scala.version"),
        "kafka": props.get("kafka.version"),
        "java": props.get("java.version") or props.get("maven.compiler.target"),
    }


def ours():
    root = ET.parse(ROOT / "pom.xml").getroot()
    props = properties(root)
    out = {
        "version": root.find(f"{NS}version").text,
        "artifactId": root.find(f"{NS}artifactId").text,
        "groupId": root.find(f"{NS}groupId").text,
        "default": {
            "binary": props["scala.binary.version"],
            "scala": props["scala.version"],
            "kafka": props["kafka.version"],
            "java": props["maven.compiler.target"],
        },
        "profiles": {},
    }
    for prof in root.iter(f"{NS}profile"):
        pid = prof.find(f"{NS}id")
        if pid is not None:
            p = properties(prof)
            out["profiles"][p.get("scala.binary.version", pid.text)] = p.get("scala.version")
    return out


def main():
    version = sys.argv[1]
    matrix = json.loads((ROOT / "tools" / "versions.json").read_text())[version]
    mine = ours()
    problems = []

    if mine["version"] != version:
        problems.append(f"pom version is {mine['version']}, expected {version}")
    if mine["artifactId"] != "komiter_${scala.binary.version}":
        problems.append(f"artifactId is {mine['artifactId']!r}, expected the cross-build form")

    # Every Scala binary this project claims to build must be one Spark actually publishes,
    # with exactly Spark's own Scala patch version.
    claimed = {mine["default"]["binary"]: mine["default"]["scala"], **mine["profiles"]}
    for binary, scala in claimed.items():
        theirs = spark_declares(binary, version)
        if theirs is None:
            problems.append(f"claims Scala {binary}, but Spark {version} publishes no _{binary} artifacts")
            continue
        if theirs["scala"] != scala:
            problems.append(f"Scala {binary}: pom says {scala}, Spark says {theirs['scala']}")

    ref = spark_declares(mine["default"]["binary"], version)
    if ref:
        if ref["kafka"] != mine["default"]["kafka"]:
            problems.append(f"Kafka: pom says {mine['default']['kafka']}, Spark says {ref['kafka']}")
        if ref["java"] != mine["default"]["java"]:
            problems.append(f"Java target: pom says {mine['default']['java']}, Spark says {ref['java']}")

    # And the matrix must not claim binaries Spark does not ship.
    for binary in matrix["scala"]:
        if spark_declares(binary, version) is None:
            problems.append(f"versions.json lists Scala {binary}, which Spark {version} does not publish")

    if problems:
        print(f"Spark {version}:")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print(f"Spark {version}: pom matches Spark's own declarations")


if __name__ == "__main__":
    main()
