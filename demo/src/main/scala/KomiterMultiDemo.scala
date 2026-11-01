import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * The case the upstream project could not handle: one query, two Kafka sources, and one of
 * those sources subscribed to more than one topic.
 *
 *   source A -> orders + payments  -> committed as "komiter-sales"
 *   source B -> shipments          -> committed as "komiter-logistics"
 *
 * Two things make this harder than the single-source case. A source reading several topics
 * reports the accumulated end offsets of all of them, so the committer has to attribute each
 * partition to the right source. And Spark only emits a scan for the sources that had new data
 * in the current batch, while `StreamingQueryProgress.sources` always lists every source — so
 * matching a plan leaf to its progress entry by position is wrong as soon as one source idles.
 * Komiter matches them by stream identity instead.
 */
object KomiterMultiDemo {

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("BOOTSTRAP", "localhost:9092")
    val checkpoint = sys.env.getOrElse("CHECKPOINT", "/tmp/komiter-multi-checkpoint")

    val spark = SparkSession.builder()
      .appName("komiter-multi-demo")
      .master("local[3]")
      .config("spark.sql.streaming.streamingQueryListeners", "com.komiter.Komiter")
      .config("spark.sql.shuffle.partitions", "6")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    def source(subscribeKey: String, subscribeValue: String, groupId: String) =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option(subscribeKey, subscribeValue)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 9000)
        .option("kafka.consumer.commit.groupid", groupId)
        .load()
        .selectExpr("CAST(value AS STRING) AS value", "topic", "partition", "offset")

    // One source, two topics.
    val sales = source("subscribePattern", "orders|payments", "komiter-sales")
    // A second, independent source in the same query.
    val logistics = source("subscribe", "shipments", "komiter-logistics")

    val query = sales.union(logistics)
      .writeStream
      .format("console")
      .option("truncate", value = true)
      .option("numRows", 3)
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .option("checkpointLocation", checkpoint)
      .start()

    query.awaitTermination()
  }
}
