# Tooling

Komiter is published once per Apache Spark release, with the artifact version equal to the
Spark version it targets. `komiter_2.13:4.2.0` is the build for Spark 4.2.0; there is no
separate connector version to map. This directory holds what produces that.

## The shape of the problem

The connector reaches into Spark internals — it walks an executed plan and reads private
fields off the Kafka source — so it is bound to the Spark release it was compiled against.
Two things therefore vary across the supported range:

**Dependency versions.** Scala, the Kafka client and the Java target are not chosen here.
They are copied from Apache Spark's own pom for each release, so the connector can never
drag in a Kafka client or a Scala runtime that differs from the one the Spark it plugs into
was built against. That is the whole reason `versions.json` exists rather than a hand-kept
list.

**The source itself.** Spark's internals moved three times across the supported range, which gives four
source variants across twenty-four releases:

| Variant | Spark | What is different |
|---|---|---|
| `a` | 3.5.0 – 3.5.9 | `scala.collection.JavaConverters`; the `SparkDataStream` is recovered by a typed match on `MicroBatchScanExec`; `DataSourceRDDPartition` holds a `Seq` |
| `b` | 4.0.0 – 4.0.5 | `scala.jdk.CollectionConverters`; `StreamSourceAwareSparkPlan` arrives and replaces the typed match |
| `c` | 4.1.0 – 4.1.4 | `StreamExecution` and `StreamingQueryWrapper` move to `…execution.streaming.runtime` |
| `d` | 4.2.0 – 4.3.0 | `DataSourceRDDPartition` holds a single `Option[InputPartition]` instead of a `Seq` |

Variant `a` has to compile under both Scala 2.12 and 2.13, which is why it uses the older
`JavaConverters` spelling — `scala.jdk` only exists from 2.13.

## `versions.json`

The matrix. One entry per Spark release: the Scala versions Spark publishes for it, the
Kafka client version, the Java target, which source variant it needs, and which Scala binary
is the default. Regenerate it from Spark's poms rather than editing by hand.

Two traps if you do regenerate it. In the `_2.13` poms of Spark 3.x the real `scala.version`
lives inside the `scala-2.13` **profile**, while the top-level property still holds the 2.12
value. And from Spark 3.4 onwards the `<properties>` block sits *after* `<profiles>` in the
file. A regex over the raw XML gets both of these wrong; parse it as XML.

## `retarget.py`

```bash
python3 tools/retarget.py 3.5.6
```

Rewrites `pom.xml`, the three Scala sources, the README and the demo so the tree targets that
release. Idempotent, and independent of which version the tree was on before.

The four source differences are stored as **pairs of alternative texts**, not as patches, so
retargeting is a matter of swapping whichever alternative is present for the one the target
needs. If none of the known alternatives is found the tool stops with an error rather than
carrying on: a tree that has drifted would otherwise produce a build that looks retargeted
and is not.

## `verify-all.sh`

```bash
tools/verify-all.sh              # all 24 releases, 34 artifacts
tools/verify-all.sh 3.5.0 3.5.1  # just these
```

Compiles every release, and both Scala binaries wherever Spark ships both, each with the JDK
matching its Java target — 11 for the Spark 3.x line, which targets Java 8, and 17 for 4.x.

This is not ceremony. The variant boundaries are drawn at minor versions, and a boundary that
is right for the two releases it was derived from is not automatically right for the ones
in between that were never built. Compiling all of them is what turns that from an assumption
into a fact.

## `verify-tests.sh`

```bash
tools/verify-tests.sh 3.5.0 4.0.0 4.1.0 4.2.0
```

Runs the suite — which starts a real embedded Kafka — against one release per variant.
Compiling every version proves the APIs line up; this proves the behaviour does. Running it
for all twenty-four would spend a lot of wall clock re-proving the same thing, since within
a variant the source is identical and Spark's patch releases are API-compatible.

## `check-deps.py` and `audit.sh`

```bash
python3 tools/check-deps.py 3.5.6   # one release
tools/audit.sh                      # all of them
```

`check-deps.py` fetches Spark's own published pom and asserts that the generated `pom.xml`
declares exactly the same Scala, Kafka and Java target. It deliberately does **not** compare
against `versions.json`: that file is the input to generation, so checking against it would
only prove the generator ran, not that the numbers in it are right. Comparing against Spark
closes that loop.

`audit.sh` runs the whole thing per release — the dependency check, a compile of every Scala
binary, an install, a read of the class-file major version out of the resulting jar, and a
compile of the demo against the artifact that was just installed.

The bytecode check earns its place: `maven.compiler.target` only matters if it reaches the
Scala compiler, and a Spark 3.5 artifact emitted as Java 11 bytecode would fail on the Java 8
clusters that line targets while every other step still reported success.

Building the demo against the installed artifact is the only step that exercises the demo's
own dependencies and the Scala suffix on the artifact name. It is how the missing `_2.12` /
`_2.13` suffix in `demo/pom.xml` was caught.

One caution: do not edit these scripts while they are running. Bash reads a script by byte
offset as it executes, so an edit mid-run shifts the ground under it and produces syntax
errors that look like bugs in the script rather than what they are.

## `build-history.sh`

Rebuilds `main` as one commit per release, oldest first, each tagged `v<spark-version>`.

The history is a **version ladder, not a timeline**: commit N is "the connector, built for
Spark N", not "what changed on some date". Check out a tag to get a version; do not read the
log as chronology.

It is destructive — it replaces `main` — and leaves the previous `main` behind as
`pre-history-<sha>`.

## `publish.sh`

```bash
tools/publish.sh                # every tag, oldest first
tools/publish.sh v4.3.0         # just this one
```

Publishes to Maven Central **one tag at a time**. Each release is built from a clean worktree
of its own tag rather than by retargeting the current tree, so the artifact for Spark X is
exactly what `vX` holds. Where Spark ships both Scala binaries, both are published.

A release that is already on Central is skipped, so an interrupted run can just be started
again. By default each release is left as a draft in the Central Portal (Deployments) to be
reviewed and published by hand; `AUTO_PUBLISH=true` publishes straight away. `SKIP_TESTS=1`
skips the suite, for a backfill already proven by `verify-tests.sh`.

It activates the `release` profile that `retarget.py` writes into every pom: sources,
scaladoc as the javadoc jar, GPG signatures and the Central Portal plugin. A normal build
never touches any of it, so it needs no signing key.

It needs the Portal user token in `~/.m2/settings.xml`, under a `<server>` with id `central`,
and a GPG key whose public half is on a keyserver.

`.github/workflows/release.yml` runs it in CI for each `v*` tag pushed, with `AUTO_PUBLISH`
on. Publishing to Central is permanent — a version can never be replaced — so push a tag
only once its commit is the release.

## Adding a Spark release

1. Add it to `versions.json`, copying Scala, Kafka and the Java target from Spark's pom for
   that release. Pick the variant by minor version, or add a fifth if Spark moved something
   again.
2. `tools/verify-all.sh <version>` — if the internals moved, this is where it shows up.
   `tools/verify-tests.sh <version>` if it needed a new variant.
3. `python3 tools/retarget.py <version>`, commit, tag `v<version>`.
4. `git push origin main v<version>` — the release workflow publishes it.
