package producerConsumer

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

object Consumer {
  def consume(spark:SparkSession, format:String, kafkaServer:String, topic:String): DataFrame ={

    /** Reading the data from the kafka topic. */
    val df = spark.read.format(format)
      .option("kafka.bootstrap.servers",kafkaServer)
      .option("subscribe", topic)
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .load()

    /** Selecting the value part of the consumed records. */
    val decodedData = df.selectExpr("CAST (value AS STRING)")

    /** Defining the schema of consumed dataset. */
    val schema = new StructType()
      .add("product_id",StringType)
      .add("location_id",StringType)
      .add("selling_channel",StringType)
      .add("prod_description",StringType)
      .add("retail_price",StringType)
      .add("onhand_quantity",StringType)
      .add("create_date",StringType)
      .add("promotion_eligibility",StringType)

    /** Converting the consumed json messages into Dataframe. */
    val kafkaDF = decodedData.select(from_json(col("value"),schema).alias("data")).select("data.*")
    kafkaDF
  }
}