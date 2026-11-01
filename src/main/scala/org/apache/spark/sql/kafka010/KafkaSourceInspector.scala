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
import org.apache.spark.sql.execution.{RDDScanExec, RowDataSourceScanExec, SparkPlan, StreamSourceAwareSparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{ContinuousScanExec, DataSourceRDDPartition, DataSourceV2ScanExecBase}
import org.apache.spark.sql.kafka010.{JsonUtils => KafkaJsonUtils}
import org.apache.spark.sql.sources.BaseRelation


class KafkaSourceInspector(sparkPlan: SparkPlan) {
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
   * Spark 4.0 introduced `StreamSourceAwareSparkPlan` as the public way for external code to
   * recover the [[SparkDataStream]] of a streaming scan, and `MicroBatchScanExec` implements
   * it. We use that trait first because it is the supported public API for this purpose.
   * `ContinuousScanExec` does not implement the trait, so it is still handled with a separate
   * pattern-match case against its public `stream` accessor. On Spark 3.3–3.5 the trait does
   * not exist and a typed match against `MicroBatchScanExec` is used for both branches.
   */
  def populateKafkaParams: Seq[(Option[SparkDataStream], Map[String, Object])] = {
    sparkPlan.collectLeaves().flatMap { plan =>
      val paramsOpt = plan match {
        case r: RowDataSourceScanExec if isKafkaRelation(r.relation) =>
          extractKafkaParamsFromKafkaRelation(r.relation)
        case r: RDDScanExec =>
          extractKafkaParamsFromDataSourceV1(r)
        case r: DataSourceV2ScanExecBase =>
          extractSourceTopicsFromDataSourceV2(r)
        case _ => None
      }
      paramsOpt.map { params =>
        val streamOpt: Option[SparkDataStream] = plan match {
          case ssa: StreamSourceAwareSparkPlan => ssa.getStream
          case cse: ContinuousScanExec => Option(cse.stream)
          case _ => None
        }
        (streamOpt, params)
      }
    }
  }

  private def isKafkaRelation(rel: BaseRelation): Boolean = {
    rel match {
      case _: KafkaRelation => true
      case _ => false
    }
  }

  private def extractKafkaParamsFromKafkaRelation(rel: BaseRelation)
      : Option[Map[String, Object]] = {
    require(isKafkaRelation(rel))
    ReflectionHelper.reflectFieldWithContextClassloader[Map[String, String]](
      rel, "specifiedKafkaParams")
  }

  private def extractKafkaParamsFromDataSourceV1(r: RDDScanExec): Option[Map[String, Object]] = {
    extractKafkaParamsFromDataSourceV1(r.rdd)
  }

  private def extractKafkaParamsFromDataSourceV1(
      rddContainingPartition: RDD[_]): Option[Map[String, Object]] = {
    rddContainingPartition.partitions.flatMap {
      case _: KafkaSourceRDDPartition =>
        extractKafkaParamsFromKafkaSourceRDDPartition(rddContainingPartition)
      case _ => None
    }.headOption
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

    collectLeaves(rddContainingPartition).flatMap {
      case r: KafkaSourceRDD => Some(r)
      case _ => None
    }.headOption.flatMap(extractKafkaParamsFromKafkaSourceRDD).orElse(None)
  }

  private def extractKafkaParamsFromKafkaSourceRDD(
      rdd: KafkaSourceRDD): Option[Map[String, Object]] = {
    val map = ReflectionHelper.reflectFieldWithContextClassloader[java.util.Map[String, Object]](
      rdd, "executorKafkaParams")
    map.map(_.asScala.toMap).orElse(None)
  }

  private def extractSourceTopicsFromDataSourceV2(r: DataSourceV2ScanExecBase): Option[Map[String, Object]] = {
    val mapsList: Seq[Map[String, Object]] = r.inputRDDs().flatMap { rdd =>
      rdd.partitions.flatMap {
        case e: DataSourceRDDPartition =>
          // `DataSourceRDDPartition` holds a `Seq[InputPartition]` on this Spark
          // version; `collect` keeps only the Kafka ones and the outer `flatMap`
          // flattens the result.
          e.inputPartitions.collect {
            case part: KafkaBatchInputPartition =>
              part.executorKafkaParams.asScala.toMap
            case part: KafkaContinuousInputPartition =>
              part.kafkaParams.asScala.toMap
            case _ => Map.empty[String, Object]
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
