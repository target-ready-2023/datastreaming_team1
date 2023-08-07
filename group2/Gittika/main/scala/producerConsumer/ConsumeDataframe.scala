package producerConsumer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}
// if instaed of Consumer Object i call this method then the code keep
object ConsumeDataframe {
  def consumerDataFrame(spark: SparkSession, format: String, topic: String): DataFrame = {
    val kafkaDF = spark.read
      .format(format)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .load()

    //converting binary to string
    val kafkaData = kafkaDF
      .selectExpr( "CAST(value AS STRING)")

//         Define the schema for the JSON data
        val schema = new StructType()
          .add("product_id", StringType)
          .add("location_id", StringType)
          .add("selling_channel", StringType)
          .add("prod_description", StringType)
          .add("retail_price", StringType)
          .add("onhand_quantity", StringType)
          .add("create_date", StringType)
          .add("promotion_eligibility", StringType)



        val parsedDf = kafkaData.select(from_json(col("value"), schema).as("data"))
          .select("data.*")

parsedDf
  }


}
