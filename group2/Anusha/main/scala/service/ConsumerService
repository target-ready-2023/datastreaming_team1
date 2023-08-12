package service

import exceptions.Exceptions.DataframeIsEmptyException
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object ConsumerService {

  def consumer(topicName: String, bootstrapServer: String, format : String, schema : StructType)(implicit sparkSession: SparkSession): DataFrame = {

    val kafkaDF = sparkSession.read.format(format)
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topicName)
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .load()

    val decodedData = kafkaDF.selectExpr("CAST (value AS STRING)")

    val outputDF = decodedData.select(from_json(col("value"),schema).alias("data")).select("data.*")

    if (outputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataFrame is empty")
    }
    outputDF
  }
}
