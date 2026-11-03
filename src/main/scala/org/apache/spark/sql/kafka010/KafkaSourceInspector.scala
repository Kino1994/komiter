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

package org.apache.spark.sql.kafka010

import scala.jdk.CollectionConverters._

import _root_.com.komiter.ReflectionHelper
import org.apache.kafka.common.TopicPartition

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.{RDDScanExec, SparkPlan, StreamSourceAwareSparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, MicroBatchScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.kafka010.{JsonUtils => KafkaJsonUtils}


class KafkaSourceInspector(sparkPlan: SparkPlan) extends AdaptiveSparkPlanHelper {
  /**
   * Returns Kafka params for each Kafka source leaf in the executed plan, paired with the
   * [[SparkDataStream]] that the leaf reads from (when the leaf is a streaming scan).
   *
   * The stream identity is what allows the listener to map a leaf back to the correct entry
   * in `StreamingQueryProgress.sources` — relying on the position of the leaf inside
   * `executedPlan.collectLeaves()` is unsafe, because Spark's micro-batch execution only
   * emits scans for the sources that have new data in the current batch, while
   * `StreamingQueryProgress.sources` always lists every source in the query.
   *
   * The leaves are collected through [[AdaptiveSparkPlanHelper]], which descends into
   * adaptive query execution: once a query has a join or an aggregation Spark may wrap the
   * plan in an `AdaptiveSparkPlanExec`, which is itself a leaf, and a plain `collectLeaves()`
   * would stop there and find no Kafka source at all.
   *
   * Only streaming scans are considered. A batch read of Kafka on the static side of a
   * stream-static join is planned as a scan of a `KafkaRelation`, or as a V2 `BatchScanExec`
   * when Kafka is left out of `spark.sql.sources.useV1SourceList`. Either way it has no
   * progress of its own, so it is not a leaf whose offsets could be committed, and a V2 batch
   * scan carries the same Kafka partitions as a streaming one: hence `MicroBatchScanExec`
   * rather than any V2 scan. Continuous processing is not
   * covered either: Spark reports no `QueryProgressEvent` for continuous queries.
   *
   * Spark 4.0 introduced `StreamSourceAwareSparkPlan` as the public way for external code to
   * recover the [[SparkDataStream]] of a streaming scan, and `MicroBatchScanExec` implements
   * it. We use it because it is the supported public API for this purpose. On Spark 3.3–3.5
   * the trait does not exist and a typed match against `MicroBatchScanExec` is used instead.
   */
  def populateKafkaParams: Seq[(Option[SparkDataStream], Map[String, Object])] = {
    collectLeaves(sparkPlan).flatMap { plan =>
      val paramsOpt = plan match {
        case r: RDDScanExec =>
          extractKafkaParamsFromDataSourceV1(r)
        case r: MicroBatchScanExec =>
          extractSourceTopicsFromDataSourceV2(r)
        case _ => None
      }
      paramsOpt.map { params =>
        val streamOpt: Option[SparkDataStream] = plan match {
          case ssa: StreamSourceAwareSparkPlan => ssa.getStream
          case _ => None
        }
        (streamOpt, params)
      }
    }
  }

  private def extractKafkaParamsFromDataSourceV1(r: RDDScanExec): Option[Map[String, Object]] = {
    extractKafkaParamsFromDataSourceV1(r.rdd)
  }

  private def extractKafkaParamsFromDataSourceV1(
      rddContainingPartition: RDD[_]): Option[Map[String, Object]] = {
    rddContainingPartition.partitions
      .collectFirst { case _: KafkaSourceRDDPartition => rddContainingPartition }
      .flatMap(extractKafkaParamsFromKafkaSourceRDDPartition)
  }

  private def extractKafkaParamsFromKafkaSourceRDDPartition(
    rddContainingPartition: RDD[_]): Option[Map[String, Object]] = {
    def collectLeaves(rdd: RDD[_]): Seq[RDD[_]] = {
      // this method is being called with chains of MapPartitionRDDs
      // so this recursion won't stack up too much
      if (rdd.dependencies.isEmpty) {
        Seq(rdd)
      } else {
        rdd.dependencies.map(_.rdd).flatMap(collectLeaves)
      }
    }

    collectLeaves(rddContainingPartition)
      .collectFirst { case r: KafkaSourceRDD => r }
      .flatMap(extractKafkaParamsFromKafkaSourceRDD)
  }

  private def extractKafkaParamsFromKafkaSourceRDD(
      rdd: KafkaSourceRDD): Option[Map[String, Object]] = {
    val map = ReflectionHelper.reflectFieldWithContextClassloader[java.util.Map[String, Object]](
      rdd, "executorKafkaParams")
    map.map(_.asScala.toMap)
  }

  private def extractSourceTopicsFromDataSourceV2(r: MicroBatchScanExec): Option[Map[String, Object]] = {
    val mapsList: Seq[Map[String, Object]] = r.inputRDDs().flatMap { rdd =>
      rdd.partitions.flatMap {
        case e: DataSourceRDDPartition =>
          // Spark 4.2 changed `DataSourceRDDPartition` to hold a single
          // `inputPartition: Option[InputPartition]` instead of a `Seq`; `collect`
          // works the same over the Option and is flattened by the outer `flatMap`.
          e.inputPartition.collect {
            case part: KafkaBatchInputPartition =>
              part.executorKafkaParams.asScala.toMap
          }
      }
    }
    val combinedMap = mapsList.foldLeft(Map.empty[String, Object])(_ ++ _)
    if (combinedMap.nonEmpty) Some(combinedMap) else None
  }

  def partitionOffsets(str: String): Map[TopicPartition, Long] = {
    KafkaJsonUtils.partitionOffsets(str)
  }
}
