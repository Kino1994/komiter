import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * Reads the `orders` topic and does nothing interesting with it — the point is the
 * consumer group that Komiter creates on the side, which you can watch in Kafka UI at
 * http://localhost:8081 or with `kafka-consumer-groups --describe --group komiter-demo`.
 *
 * `maxOffsetsPerTrigger` deliberately throttles the query so it drains the backlog
 * slowly and the lag is visible falling to zero instead of vanishing in one batch.
 */
object KomiterDemo {

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("BOOTSTRAP", "localhost:9092")
    val checkpoint = sys.env.getOrElse("CHECKPOINT", "/tmp/komiter-demo-checkpoint")

    val spark = SparkSession.builder()
      .appName("komiter-demo")
      .master("local[3]")
      // This is all it takes to enable Komiter.
      .config("spark.sql.streaming.streamingQueryListeners", "com.komiter.Komiter")
      .config("spark.sql.shuffle.partitions", "6")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val orders = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", "orders")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 8000)
      // The one option Komiter looks for. Without it this source is ignored.
      .option("kafka.consumer.commit.groupid", "komiter-demo")
      .load()

    val query = orders
      .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value", "topic", "partition", "offset")
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
