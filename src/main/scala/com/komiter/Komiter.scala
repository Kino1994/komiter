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

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.runtime.{StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.kafka010.KafkaSourceInspector
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryListener}


class Komiter extends StreamingQueryListener with Logging {
  import Komiter._

  // KafkaConsumer instances are reused across micro-batches so the bootstrap cost
  // (Metadata + FindCoordinator) is paid once per consumer instead of on every commit.
  // They are keyed by the full set of Kafka params, which includes the bootstrap
  // servers, the security settings and the target group id, so a consumer is shared
  // only when it would commit to the same group on the same cluster with the same
  // connection settings. StreamingQueryListener callbacks are dispatched by a single
  // thread, but the cache is concurrent to stay safe and a JVM shutdown hook closes
  // every cached consumer on exit.
  private val consumers =
    new ConcurrentHashMap[Map[String, Object], KafkaConsumer[String, String]]()

  Runtime.getRuntime.addShutdownHook(new Thread("kafka-offset-committer-shutdown") {
    override def run(): Unit = closeAll()
  })

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val query = SparkSession.active.streams.get(event.progress.id)
    if (query != null) {
      val streamExecOpt: Option[StreamExecution] = query match {
        case wrapper: StreamingQueryWrapper => Option(wrapper.streamingQuery)
        case se: StreamExecution => Option(se)
        case _ =>
          logWarning(s"Unexpected type of streaming query: ${query.getClass}")
          None
      }

      streamExecOpt.foreach { streamExec =>
        val ex = streamExec.lastExecution
        if (ex != null) {
          // Resolve each Kafka plan leaf to its entry in
          // `StreamingQueryProgress.sources` by stream identity. Spark reports one
          // `SourceProgress` per distinct source and sets its `description` to the
          // stream's `toString`, so matching the leaf's stream `toString` against
          // `SourceProgress.description` maps every leaf to its progress entry, even
          // when a single Kafka source subscribes to many topics and therefore reads
          // the accumulated end offsets of all of them.
          val progressByDescription: Map[String, SourceProgress] =
            event.progress.sources.map(sp => sp.description -> sp).toMap

          val inspector = new KafkaSourceInspector(ex.executedPlan)
          inspector.populateKafkaParams.foreach { case (streamOpt, params) =>
            params.get(CONFIG_KEY_GROUP_ID) match {
              case Some(groupId) =>
                val sourceProgressOpt = streamOpt.map(_.toString)
                  .flatMap(progressByDescription.get)
                  .orElse {
                    // With a single Kafka source in the query its offsets can only
                    // belong to the one reported source, so use it directly when the
                    // stream handle is not available to match by identity.
                    if (event.progress.sources.length == 1) {
                      event.progress.sources.headOption
                    } else {
                      None
                    }
                  }
                sourceProgressOpt match {
                  case Some(sourceProgress) =>
                    val tpToOffsets = inspector.partitionOffsets(sourceProgress.endOffset)

                    val newParams = new scala.collection.mutable.HashMap[String, Object]
                    newParams ++= params
                    newParams += "group.id" -> groupId

                    val key = newParams.toMap
                    val kafkaConsumer = getOrCreateConsumer(key)
                    try {
                      val offsetsToCommit = tpToOffsets.map { case (tp, offset) =>
                        tp -> new OffsetAndMetadata(offset)
                      }
                      kafkaConsumer.commitSync(offsetsToCommit.asJava, Duration.ofSeconds(10))
                    } catch {
                      case NonFatal(e) =>
                        // Drop the possibly-broken consumer so the next batch
                        // rebuilds a fresh one instead of reusing bad state.
                        consumers.remove(key, kafkaConsumer)
                        closeQuietly(kafkaConsumer)
                        throw e
                    }

                  case None =>
                    logWarning("Could not match a Kafka plan leaf with group id " +
                      s"'$groupId' to any streaming source in query ${event.progress.id}; " +
                      "offsets will not be committed for this leaf.")
                }

              case None =>
            }
          }
        }
      }
    } else {
      logWarning(s"Cannot find query ${event.progress.id} from active spark session!")
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

  private def getOrCreateConsumer(
      params: Map[String, Object]): KafkaConsumer[String, String] = {
    consumers.computeIfAbsent(
      params,
      (p: Map[String, Object]) => new KafkaConsumer[String, String](p.asJava))
  }

  private def closeQuietly(consumer: KafkaConsumer[String, String]): Unit = {
    try {
      consumer.close(Duration.ofSeconds(5))
    } catch {
      case NonFatal(e) => logWarning(s"Failed to close Kafka consumer. $e")
    }
  }

  private def closeAll(): Unit = {
    consumers.values().asScala.foreach(closeQuietly)
    consumers.clear()
  }
}

object Komiter {
  private val CONFIG_KEY_GROUP_ID = "consumer.commit.groupid"
  val CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION: String = "kafka." + CONFIG_KEY_GROUP_ID
}
