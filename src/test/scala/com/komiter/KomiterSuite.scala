/*
 * Copyright 2019 Jungtaek Lim "<kabhwan@gmail.com>"
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.komiter

import java.io.File
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import Komiter._
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{Consumer, MockConsumer, OffsetAndMetadata, OffsetResetStrategy}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.json4s.JsonAST.{JArray, JInt, JObject}
import org.json4s.jackson.JsonMethods.{compact, render}
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, StreamTest, Trigger}
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent


class KomiterSuite extends StreamTest {
  val brokerProps: Map[String, Object] = Map[String, Object]()
  var testUtils: KafkaTestUtils = _
  var adminClient: AdminClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(brokerProps)
    testUtils.setup()

    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, testUtils.brokerAddress)
    adminClient = AdminClient.create(props)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    if (adminClient != null) {
      adminClient.close()
    }
    super.afterAll()
  }

  private def assertOffsetsAreCommited(
      groupId: String,
      topics: Seq[String],
      numPartitions: Int,
      expectedOffsetsPerPartition: Int): Unit = {
    val committedOffsets = adminClient.listConsumerGroupOffsets(groupId)
      .partitionsToOffsetAndMetadata().get().asScala
      .map { case (tp, offsetAndMetadata) => tp -> offsetAndMetadata.offset() }
      .toMap
    val expectedOffsets = topics.flatMap { topic =>
      (0 until numPartitions).map(new TopicPartition(topic, _) -> expectedOffsetsPerPartition)
    }.toMap
    assert(committedOffsets === expectedOffsets)
  }

  private val numPartitionsForSingleSource = 4

  test("Run micro-batch query with Kafka source(s) - check offsets are committed") {
    def assertOffsetsAreNotCommited(topics: Seq[String]): Unit = {
      val groups = adminClient.listConsumerGroups().all().get().asScala
      val committedTopics = groups.flatMap { grp =>
        adminClient.listConsumerGroupOffsets(grp.groupId())
          .partitionsToOffsetAndMetadata().get().asScala
          .map { case (tp, offsetAndMetadata) => tp -> offsetAndMetadata.offset() }
          .filter { case (_, offset) => offset >= 0 }
          .map { case (tp, _) => tp.topic() }
          .toSet
      }.toSet
      assert(Set.empty[String] === committedTopics.intersect(topics.toSet))
    }

    val listener = new Komiter()
    try {
      spark.streams.addListener(listener)

      val topicPrefix = "sparkread0"
      val brokerAddress = testUtils.brokerAddress

      val groupId1 = "groupId1"
      val (df1, topicsToRead1) = kafkaDfSubscribePattern(topicPrefix, brokerAddress, Some(groupId1))

      val groupId2 = "groupId2"
      val (df2, topicsToRead2) = kafkaDfSubscribe(topicPrefix, brokerAddress, Some(groupId2))

      val (df3, topicsToRead3) = kafkaDfAssign(topicPrefix, brokerAddress, Some(groupId1))

      val topicPrefix2 = "sparkread1"
      val (df4, topicsToRead4) = kafkaDfSubscribe(topicPrefix2, brokerAddress, None)

      val topicsToRead = topicsToRead1 ++ topicsToRead2 ++ topicsToRead3 ++ topicsToRead4

      val topicToWrite = "sparkwrite"
      val topics = topicsToRead ++ Seq(topicToWrite)

      val numPartitions = 10
      val msgsPerPartition = 100
      topics.toSet[String].foreach { ti =>
        testUtils.createTopic(ti, numPartitions, overwrite = true)
      }

      val (_, _, checkpointDir) = createTempDirectories

      val df = df1.union(df2).union(df3).union(df4)

      val query = toKafkaSink(df, brokerAddress, topicToWrite, checkpointDir.getAbsolutePath)

      try {
        sendMessages(topicsToRead, numPartitions, msgsPerPartition)
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assertOffsetsAreCommited(groupId1, topicsToRead1 ++ topicsToRead3, numPartitions,
            msgsPerPartition)
          assertOffsetsAreCommited(groupId2, topicsToRead2, numPartitions, msgsPerPartition)
          assertOffsetsAreNotCommited(topicsToRead4)
        }
      } finally {
        query.stop()
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("Single Kafka source subscribed to N topics - offsets committed for every topic " +
      "under one group") {
    // One `readStream` with `subscribe=t1,t2,...,tN`, no unions, a single streaming
    // query and therefore a single SparkDataStream. The listener commits the
    // accumulated end offsets of all N topics to the single commit group.
    runSingleSourceMultiTopic(
      topicPrefix = "multitopic",
      groupId = "multiTopicGroup",
      topicToWrite = "multitopicwrite") { (readStream, topicsToRead) =>
      readStream.option("subscribe", topicsToRead.mkString(","))
    }
  }

  test("Single Kafka source with subscribePattern matching N topics - offsets committed " +
      "for every topic under one group") {
    // One `readStream` with `subscribePattern=...`, no unions, a single streaming
    // query. All topics matched by the pattern are read by one SparkDataStream, and
    // the listener commits their accumulated end offsets to the single commit group.
    val prefix = "patternonly"
    runSingleSourceMultiTopic(
      topicPrefix = prefix,
      groupId = "patternOnlyGroup",
      topicToWrite = "patternonlywrite") { (readStream, topicsToRead) =>
      readStream.option("subscribePattern", prefix + "[1-" + topicsToRead.size + "]")
    }
  }

  test("Single Kafka source with assign of N topics - offsets committed for every topic " +
      "under one group") {
    // One `readStream` with `assign={...}`, no unions, a single streaming query.
    // `assign` pins explicit topic-partitions, but they are still read by one
    // SparkDataStream whose end offsets accumulate every assigned partition, and the
    // listener commits them to the single commit group.
    runSingleSourceMultiTopic(
      topicPrefix = "assignonly",
      groupId = "assignOnlyGroup",
      topicToWrite = "assignonlywrite") { (readStream, topicsToRead) =>
      val jsonToAssign = {
        val r = JObject.apply {
          topicsToRead.map { topic =>
            (topic, JArray((0 until numPartitionsForSingleSource).map(JInt(_)).toList))
          }.toList
        }
        compact(render(r))
      }
      readStream.option("assign", jsonToAssign)
    }
  }

  test("DataSource V1 Kafka source - offsets committed") {
    // With the V2 micro-batch reader disabled Spark plans the Kafka source as an RDD scan.
    // There is no stream handle on that leaf, so the Kafka params come from the source RDD
    // by reflection and the single reported source is used to find the end offsets.
    withSQLConf(DISABLED_V2_MICROBATCH_READERS -> KAFKA_SOURCE_PROVIDER) {
      runSingleSourceMultiTopic(
        topicPrefix = "v1source",
        groupId = "v1SourceGroup",
        topicToWrite = "v1sourcewrite") { (readStream, topicsToRead) =>
        readStream.option("subscribe", topicsToRead.mkString(","))
      }
    }
  }

  test("Two DataSource V1 Kafka sources - never commits another source's offsets") {
    // Whether a V1 leaf can be matched to its source depends on the Spark release: from 4.0
    // the RDD scan carries its stream and each group gets its own source's offsets; on 3.5
    // it does not, and with two sources there is no telling which end offsets belong to
    // which leaf. Either way a group must end up with its own topic's offsets or with
    // nothing, never with the other source's.
    withSQLConf(DISABLED_V2_MICROBATCH_READERS -> KAFKA_SOURCE_PROVIDER) {
      val listener = new Komiter()
      try {
        spark.streams.addListener(listener)
        val brokerAddress = testUtils.brokerAddress
        val topics = Seq("v1twoa", "v1twob")
        val topicToWrite = "v1twowrite"
        (topics :+ topicToWrite).foreach(testUtils.createTopic(_, 2, overwrite = true))
        sendMessages(topics, numPartitions = 2, msgsPerPartition = 10)

        val Seq(dfA, dfB) = topics.map { topic =>
          spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", brokerAddress)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, topic + "Group")
            .load()
        }
        val (_, _, checkpointDir) = createTempDirectories
        val query = toKafkaSink(dfA.union(dfB), brokerAddress, topicToWrite,
          checkpointDir.getAbsolutePath)
        try {
          query.processAllAvailable()
          // Give the asynchronous listener bus time to deliver the progress events.
          Thread.sleep(2000)
          topics.foreach { topic =>
            val committed = committedOffsets(topic + "Group")
            val own = (0 until 2).map(new TopicPartition(topic, _) -> 10L).toMap
            assert(committed.isEmpty || committed === own)
          }
        } finally {
          query.stop()
        }
      } finally {
        spark.streams.removeListener(listener)
      }
    }
  }

  test("Progress of a query that is no longer active is ignored") {
    // The listener bus is asynchronous, so a progress event can arrive after its query has
    // stopped. The listener must log and move on rather than fail.
    val captured = new java.util.concurrent.ConcurrentLinkedQueue[QueryProgressEvent]()
    val capture = new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}
      override def onQueryProgress(event: QueryProgressEvent): Unit = captured.add(event)
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    }
    spark.streams.addListener(capture)
    val brokerAddress = testUtils.brokerAddress
    val topic = "stoppedquery"
    val topicToWrite = "stoppedquerywrite"
    Seq(topic, topicToWrite).foreach(testUtils.createTopic(_, 1, overwrite = true))
    sendMessages(Seq(topic), numPartitions = 1, msgsPerPartition = 5)
    val group = "stoppedQueryGroup"
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, group)
      .load()
    val (_, _, checkpointDir) = createTempDirectories
    val query = toKafkaSink(df, brokerAddress, topicToWrite, checkpointDir.getAbsolutePath)
    try {
      query.processAllAvailable()
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        assert(!captured.isEmpty)
      }
    } finally {
      query.stop()
      spark.streams.removeListener(capture)
    }

    // Komiter was not registered while the query ran, so nothing was committed; replaying
    // its progress now that it is gone must not commit either.
    val listener = new Komiter()
    captured.asScala.foreach(listener.onQueryProgress)
    assert(committedOffsets(group).isEmpty)
  }

  test("A failing commit does not affect the query, and the broken consumer is replaced") {
    // Every commit fails, and so does closing the consumer afterwards. The listener drops the
    // consumer, closes it quietly and rethrows; Spark's listener bus logs that and the query
    // carries on. The next batch builds a fresh consumer instead of reusing the broken one.
    val created = new AtomicInteger()
    val closeAttempts = new AtomicInteger()
    val listener = new Komiter() {
      override protected def createConsumer(
          params: Map[String, Object]): Consumer[String, String] = {
        created.incrementAndGet()
        new MockConsumer[String, String](OffsetResetStrategy.NONE) {
          override def commitSync(
              offsets: java.util.Map[TopicPartition, OffsetAndMetadata],
              timeout: Duration): Unit = throw new KafkaException("commit refused")
          override def close(timeout: Duration): Unit = {
            closeAttempts.incrementAndGet()
            throw new KafkaException("close refused")
          }
        }
      }
    }
    try {
      spark.streams.addListener(listener)
      val brokerAddress = testUtils.brokerAddress
      val topic = "failingcommit"
      val topicToWrite = "failingcommitwrite"
      Seq(topic, topicToWrite).foreach(testUtils.createTopic(_, 2, overwrite = true))

      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "failingCommitGroup")
        .load()
      val (_, _, checkpointDir) = createTempDirectories
      val query = toKafkaSink(df, brokerAddress, topicToWrite, checkpointDir.getAbsolutePath)
      try {
        sendMessages(Seq(topic), numPartitions = 2, msgsPerPartition = 10)
        query.processAllAvailable()
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assert(closeAttempts.get() >= 1)
        }
        sendMessages(Seq(topic), numPartitions = 2, msgsPerPartition = 10)
        query.processAllAvailable()
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assert(created.get() >= 2)
        }
        assert(query.isActive)
        assert(query.exception.isEmpty)
        assert(committedOffsets("failingCommitGroup").isEmpty)
      } finally {
        query.stop()
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("A non-Kafka source in the same query is left alone") {
    // The rate source sits next to the Kafka source in the plan. Its leaf carries no Kafka
    // params and must neither break the inspection nor steal the Kafka source's offsets.
    val listener = new Komiter()
    try {
      spark.streams.addListener(listener)
      val brokerAddress = testUtils.brokerAddress
      val topic = "mixedsources"
      val topicToWrite = "mixedsourceswrite"
      Seq(topic, topicToWrite).foreach(testUtils.createTopic(_, 2, overwrite = true))
      sendMessages(Seq(topic), numPartitions = 2, msgsPerPartition = 10)

      val kafka = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "mixedSourcesGroup")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
      val rate = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "5")
        .option("numPartitions", "1")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
      val (_, _, checkpointDir) = createTempDirectories
      val query = toKafkaSink(kafka.union(rate), brokerAddress, topicToWrite,
        checkpointDir.getAbsolutePath)
      try {
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assertOffsetsAreCommited("mixedSourcesGroup", Seq(topic), 2, 10)
        }
      } finally {
        query.stop()
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("Continuous processing is not covered - nothing committed, query unaffected") {
    // Spark reports no QueryProgressEvent for a continuous query, so the listener never sees
    // one: nothing is committed, and the query runs exactly as without the listener.
    val listener = new Komiter()
    try {
      spark.streams.addListener(listener)
      val brokerAddress = testUtils.brokerAddress
      val topic = "continuous"
      val topicToWrite = "continuouswrite"
      Seq(topic, topicToWrite).foreach(testUtils.createTopic(_, 2, overwrite = true))
      sendMessages(Seq(topic), numPartitions = 2, msgsPerPartition = 10)

      val (_, _, checkpointDir) = createTempDirectories
      val query = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "continuousGroup")
        .load()
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("topic", topicToWrite)
        .option("checkpointLocation", checkpointDir.getAbsolutePath)
        .trigger(Trigger.Continuous("200 milliseconds"))
        .start()
      try {
        eventually(timeout(30.seconds), interval(200.milliseconds)) {
          assert(testUtils.getLatestOffsets(Set(topicToWrite)).values.sum === 20L)
        }
        assert(query.isActive)
        assert(query.exception.isEmpty)
        assert(committedOffsets("continuousGroup").isEmpty)
      } finally {
        query.stop()
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("Idle triggers with nothing to read commit nothing") {
    // With nothing to read, Spark still reports progress for idle triggers. There are no
    // offsets to move, and nothing may be committed.
    withSQLConf("spark.sql.streaming.noDataProgressEventInterval" -> "100") {
      val progressEvents = new AtomicInteger()
      val listener = new Komiter() {
        override def onQueryProgress(event: QueryProgressEvent): Unit = {
          super.onQueryProgress(event)
          progressEvents.incrementAndGet()
        }
      }
      try {
        spark.streams.addListener(listener)
        val brokerAddress = testUtils.brokerAddress
        val topic = "nodata"
        val topicToWrite = "nodatawrite"
        Seq(topic, topicToWrite).foreach(testUtils.createTopic(_, 1, overwrite = true))
        val df = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerAddress)
          .option("subscribe", topic)
          .option("startingOffsets", "latest")
          .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "noDataGroup")
          .load()
        val (_, _, checkpointDir) = createTempDirectories
        val query = toKafkaSink(df, brokerAddress, topicToWrite, checkpointDir.getAbsolutePath)
        try {
          eventually(timeout(30.seconds), interval(100.milliseconds)) {
            assert(progressEvents.get() >= 1)
          }
          assert(query.isActive)
          assert(query.exception.isEmpty)
          assert(committedOffsets("noDataGroup").isEmpty)
        } finally {
          query.stop()
        }
      } finally {
        spark.streams.removeListener(listener)
      }
    }
  }

  test("Stream-static join - only the streaming source is committed") {
    // The static side is a plain batch read of Kafka, planned as a scan of a KafkaRelation.
    // It has no progress of its own, so even if it is given a commit group, nothing may be
    // committed to it; the streaming side commits as usual.
    val listener = new Komiter()
    try {
      spark.streams.addListener(listener)
      val brokerAddress = testUtils.brokerAddress
      val streamTopic = "joinstream"
      val staticTopic = "joinstatic"
      val topicToWrite = "joinwrite"
      Seq(streamTopic, staticTopic, topicToWrite).foreach(
        testUtils.createTopic(_, 2, overwrite = true))
      sendMessages(Seq(staticTopic), numPartitions = 2, msgsPerPartition = 10)
      sendMessages(Seq(streamTopic), numPartitions = 2, msgsPerPartition = 10)

      val static = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("subscribe", staticTopic)
        .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "staticSideGroup")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
      val stream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("subscribe", streamTopic)
        .option("startingOffsets", "earliest")
        .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "streamSideGroup")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
      val (_, _, checkpointDir) = createTempDirectories
      val query = toKafkaSink(stream.join(static, "value"), brokerAddress, topicToWrite,
        checkpointDir.getAbsolutePath)
      try {
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assertOffsetsAreCommited("streamSideGroup", Seq(streamTopic), 2, 10)
        }
        assert(committedOffsets("staticSideGroup").isEmpty)
      } finally {
        query.stop()
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("Progress for a query that has not executed a plan yet is ignored") {
    // A query restarted from its checkpoint keeps its id, and with no new data it has not
    // executed anything yet: there is no plan to inspect. A progress event for it arriving
    // then (the listener bus is asynchronous) must be skipped, not fail the listener.
    val captured = new java.util.concurrent.ConcurrentLinkedQueue[QueryProgressEvent]()
    val capture = new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}
      override def onQueryProgress(event: QueryProgressEvent): Unit = captured.add(event)
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    }
    val brokerAddress = testUtils.brokerAddress
    val topic = "restarted"
    val topicToWrite = "restartedwrite"
    val group = "restartedGroup"
    Seq(topic, topicToWrite).foreach(testUtils.createTopic(_, 1, overwrite = true))
    sendMessages(Seq(topic), numPartitions = 1, msgsPerPartition = 5)
    val (_, _, checkpointDir) = createTempDirectories
    def start(): StreamingQuery = toKafkaSink(
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, group)
        .load(),
      brokerAddress, topicToWrite, checkpointDir.getAbsolutePath)

    spark.streams.addListener(capture)
    val first = start()
    try {
      first.processAllAvailable()
      eventually(timeout(30.seconds), interval(100.milliseconds)) {
        assert(!captured.isEmpty)
      }
    } finally {
      first.stop()
      spark.streams.removeListener(capture)
    }

    val restarted = start()
    try {
      assert(restarted.id === first.id)
      val listener = new Komiter()
      assert(listener.streamExecutionOf(restarted).exists(_.lastExecution == null))
      captured.asScala.foreach(listener.onQueryProgress)
      assert(restarted.isActive)
      assert(committedOffsets(group).isEmpty)
    } finally {
      restarted.stop()
    }
  }

  test("Stream-static join with a V2 batch read of Kafka - static side never committed") {
    // With Kafka out of spark.sql.sources.useV1SourceList the static side is a V2 batch scan,
    // whose partitions are Kafka's just like a streaming scan's. It is still not a streaming
    // source: with a single streaming source in the query, taking it for one would commit
    // the streaming source's offsets to the static side's group.
    withSQLConf("spark.sql.sources.useV1SourceList" -> "") {
      val listener = new Komiter()
      try {
        spark.streams.addListener(listener)
        val brokerAddress = testUtils.brokerAddress
        val streamTopic = "v2joinstream"
        val staticTopic = "v2joinstatic"
        val topicToWrite = "v2joinwrite"
        Seq(streamTopic, staticTopic, topicToWrite).foreach(
          testUtils.createTopic(_, 2, overwrite = true))
        sendMessages(Seq(staticTopic, streamTopic), numPartitions = 2, msgsPerPartition = 10)

        val static = spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerAddress)
          .option("subscribe", staticTopic)
          .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "v2StaticSideGroup")
          .load()
          .selectExpr("CAST(value AS STRING) AS value")
        val stream = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerAddress)
          .option("subscribe", streamTopic)
          .option("startingOffsets", "earliest")
          .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "v2StreamSideGroup")
          .load()
          .selectExpr("CAST(value AS STRING) AS value")
        val (_, _, checkpointDir) = createTempDirectories
        val query = toKafkaSink(stream.join(static, "value"), brokerAddress, topicToWrite,
          checkpointDir.getAbsolutePath)
        try {
          eventually(timeout(30.seconds), interval(100.milliseconds)) {
            assertOffsetsAreCommited("v2StreamSideGroup", Seq(streamTopic), 2, 10)
          }
          assert(committedOffsets("v2StaticSideGroup").isEmpty)
        } finally {
          query.stop()
        }
      } finally {
        spark.streams.removeListener(listener)
      }
    }
  }

  test("A static DataFrame built from an RDD is left alone") {
    // An RDD-backed DataFrame on the static side is planned as an RDD scan, like a V1
    // streaming source, but its partitions are not Kafka's: it carries no Kafka params.
    val listener = new Komiter()
    try {
      spark.streams.addListener(listener)
      val brokerAddress = testUtils.brokerAddress
      val topic = "rddjoin"
      val topicToWrite = "rddjoinwrite"
      Seq(topic, topicToWrite).foreach(testUtils.createTopic(_, 2, overwrite = true))
      sendMessages(Seq(topic), numPartitions = 2, msgsPerPartition = 10)

      val static = spark.createDataFrame(
        spark.sparkContext.parallelize((1 to 10).map(i => Tuple1(i.toString)))).toDF("value")
      val stream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, "rddJoinGroup")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
      val (_, _, checkpointDir) = createTempDirectories
      val query = toKafkaSink(stream.join(static, "value"), brokerAddress, topicToWrite,
        checkpointDir.getAbsolutePath)
      try {
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assertOffsetsAreCommited("rddJoinGroup", Seq(topic), 2, 10)
        }
      } finally {
        query.stop()
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("Source resolution: by stream identity, else only when there is a single source") {
    def resolve(stream: Option[String], sources: String*): Option[String] =
      resolveSource(stream, sources)(identity)

    // Matched by the stream's description, among any number of sources.
    assert(resolve(Some("b"), "a", "b", "c") === Some("b"))
    // No stream handle, or no match: a single source can only be that one ...
    assert(resolve(None, "a") === Some("a"))
    assert(resolve(Some("z"), "a") === Some("a"))
    // ... but with several there is no telling, and nothing is attributed.
    assert(resolve(None, "a", "b") === None)
    assert(resolve(Some("z"), "a", "b") === None)
    assert(resolve(None) === None)
  }

  test("The execution behind a query handle, whatever form the handle takes") {
    val listener = new Komiter()
    val query = spark.readStream.format("rate").load().writeStream.format("noop").start()
    try {
      // What StreamingQueryManager hands out: a wrapper around the execution.
      val execution = listener.streamExecutionOf(query)
      assert(execution.isDefined)
      // The execution itself is a query handle too.
      assert(listener.streamExecutionOf(execution.get) === execution)
      // Anything else is logged and skipped rather than failing the listener.
      val foreign = java.lang.reflect.Proxy.newProxyInstance(
        getClass.getClassLoader,
        Array[Class[_]](classOf[StreamingQuery]),
        new java.lang.reflect.InvocationHandler {
          override def invoke(
              proxy: Object, method: java.lang.reflect.Method, args: Array[Object]): Object = null
        })
        .asInstanceOf[StreamingQuery]
      assert(listener.streamExecutionOf(foreign) === None)
    } finally {
      query.stop()
    }
  }

  test("ReflectionHelper returns None for a field it cannot read") {
    // Spark's internals move between releases; a missing or unreadable field must degrade to
    // None (and a warning), never to an exception in the listener.
    val holder = ReflectionHolder(42)
    assert(ReflectionHelper.reflectFieldWithContextClassloader[Int](holder, "present") ===
      Some(42))
    assert(ReflectionHelper.reflectFieldWithContextClassloader[Int](holder, "absent") === None)
    // A method, not a field: found by name, but it cannot be read as one.
    assert(ReflectionHelper.reflectFieldWithContextClassloader[Any](holder, "toString") === None)
  }

  private val DISABLED_V2_MICROBATCH_READERS = "spark.sql.streaming.disabledV2MicroBatchReaders"
  private val KAFKA_SOURCE_PROVIDER = "org.apache.spark.sql.kafka010.KafkaSourceProvider"

  private def committedOffsets(groupId: String): Map[TopicPartition, Long] = {
    adminClient.listConsumerGroupOffsets(groupId)
      .partitionsToOffsetAndMetadata().get().asScala
      .map { case (tp, offsetAndMetadata) => tp -> offsetAndMetadata.offset() }
      .toMap
  }

  /**
   * Runs a single-source / multi-topic scenario: creates `numTopics` topics, builds one
   * `readStream` (no unions) whose subscription is provided by `subscribe`, sends messages
   * to every topic, and asserts the accumulated offsets of all topics are committed to the
   * single commit group.
   */
  private def runSingleSourceMultiTopic(
      topicPrefix: String,
      groupId: String,
      topicToWrite: String,
      numTopics: Int = 5)(
      subscribe: (org.apache.spark.sql.streaming.DataStreamReader, Seq[String])
        => org.apache.spark.sql.streaming.DataStreamReader): Unit = {
    val listener = new Komiter()
    try {
      spark.streams.addListener(listener)

      val brokerAddress = testUtils.brokerAddress
      val topicsToRead = (1 to numTopics).map(idx => topicPrefix + idx)

      val numPartitions = numPartitionsForSingleSource
      val msgsPerPartition = 50
      (topicsToRead :+ topicToWrite).toSet[String].foreach { ti =>
        testUtils.createTopic(ti, numPartitions, overwrite = true)
      }

      val (_, _, checkpointDir) = createTempDirectories

      val baseReader = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("startingOffsets", "earliest")
        .option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, groupId)

      val df = subscribe(baseReader, topicsToRead).load()

      val query = toKafkaSink(df, brokerAddress, topicToWrite, checkpointDir.getAbsolutePath)

      try {
        sendMessages(topicsToRead, numPartitions, msgsPerPartition)
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assertOffsetsAreCommited(groupId, topicsToRead, numPartitions, msgsPerPartition)
        }
      } finally {
        query.stop()
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  private def kafkaDfSubscribePattern(
      topicNamePrefix: String,
      brokerAddress: String,
      commitGroupId: Option[String]): (Dataset[Row], Seq[String]) = {
    val topics = (1 to 3).map(idx => topicNamePrefix + idx)

    // test for 'subscribePattern'
    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribePattern", topicNamePrefix + "[1-3]")
      .option("startingOffsets", "earliest")

    val df = commitGroupId match {
      case Some(gid) => stream.option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, gid).load()
      case _ => stream.load()
    }

    (df, topics)
  }

  private def kafkaDfSubscribe(
      topicNamePrefix: String,
      brokerAddress: String,
      commitGroupId: Option[String]): (Dataset[Row], Seq[String]) = {
    val topics = (4 to 5).map(idx => topicNamePrefix + idx)

    // test for 'subscribe'
    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribe", topics.mkString(","))
      .option("startingOffsets", "earliest")

    val df = commitGroupId match {
      case Some(gid) => stream.option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, gid).load()
      case _ => stream.load()
    }

    (df, topics)
  }

  private def kafkaDfAssign(
      topicNamePrefix: String,
      brokerAddress: String,
      commitGroupId: Option[String]): (Dataset[Row], Seq[String]) = {
    val topics = (6 to 7).map(idx => topicNamePrefix + idx)
    // test for 'assign'
    val jsonToAssignTopicToRead = {
      val r = JObject.apply {
        topics.map {
          (_, JArray((0 until 10).map(JInt(_)).toList))
        }.toList
      }
      compact(render(r))
    }

    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("assign", jsonToAssignTopicToRead)
      .option("startingOffsets", "earliest")

    val df = commitGroupId match {
      case Some(gid) => stream.option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, gid).load()
      case _ => stream.load()
    }

    (df, topics)
  }

  private def toKafkaSink(
      df: Dataset[Row],
      brokerAddress: String,
      topic: String,
      checkpointPath: String): StreamingQuery = {
    df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topic)
      .option("checkpointLocation", checkpointPath)
      .start()
  }

  private def createTempDirectories: (File, File, File) = {
    val tempDir = Files.createTempDirectory("kafka-offset-committer-listener-suite-temp")

    val srcDir = new File(tempDir.toFile, "src")
    val destDir = new File(tempDir.toFile, "dest")
    val checkpointDir = new File(tempDir.toFile, "checkpoint")

    // remove temporary directory in shutdown
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run(): Unit = {
          FileUtils.forceDelete(tempDir.toFile)
        }
      }, 10)

    Files.createDirectories(srcDir.toPath)

    (srcDir, destDir, checkpointDir)
  }

  private def sendMessages(
      topicsToRead: Seq[String],
      numPartitions: Int,
      msgsPerPartition: Int): Unit = {
    topicsToRead.foreach { topic =>
      (0 until numPartitions).foreach { part =>
        val messages = (1 to msgsPerPartition).map(_.toString).toArray
        testUtils.sendMessages(topic, messages, Some(part))
      }
    }
  }
}

/** Top level on purpose: Scala runtime reflection cannot reflect on a class local to a method. */
case class ReflectionHolder(present: Int)
