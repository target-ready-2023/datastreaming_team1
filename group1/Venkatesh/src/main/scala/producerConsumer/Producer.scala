package producerConsumer

import constants.ApplicationConstants._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Producer {
  def produce(spark: SparkSession, path:String, format: String, kafkaServerPort:String, topic:String): Unit ={

    /** Reading the dataset from the specified path. */
    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load(path)

    /** Appending the header to the dataset. */
    val dfWithHeader = df.toDF(DATA_STREAM_COLUMN_NAMES: _*)
//      .limit(5)

    /** Writing data into the kafka topic. */
    dfWithHeader.select((struct(KEY_COLUMN_NAME).cast("string")).alias(KEY), to_json(struct("*")).alias(VALUE)).write.format(format)
      .option("kafka.bootstrap.servers", kafkaServerPort)
      .option("topic", topic).save()
  }
}