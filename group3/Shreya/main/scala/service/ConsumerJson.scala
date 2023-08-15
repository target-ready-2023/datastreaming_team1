package service

import exceptions.Exceptions.DataframeIsEmptyException
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ConsumerJson {
  def ConsumerJSON(topic: String, bootstrapServer: String)(implicit spark: SparkSession): DataFrame = {

    val kafkaDF = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topic)
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .load()

    val kafkaData = kafkaDF.selectExpr("CAST(value AS STRING)")

    //define the schema for the JSON data
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

    //if dataframe does not contain any records
    if (parsedDf.count() == 0) {
      throw DataframeIsEmptyException("The dataFrame is empty")
    }
    parsedDf
  }
}


/*
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "datastreaming")
      .option("includeHeaders", "true")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val q1 = df.writeStream
      .format("json")
      .option("path", "C:/tmp/data/out/jsonDataset/")
      .option("checkpointLocation", "C:/tmp/data/out/checkpointJD/")
      .start()

    q1.awaitTermination()

 */
