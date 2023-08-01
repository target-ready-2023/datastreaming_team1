package template.spark

import org.apache.spark.sql.SparkSession
object consumeNewScala {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").
      appName("Kafka JSON Consumer").
      getOrCreate()

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "datastream")
      .option("includeHeaders", "true")
//      .option("maxOffsetsPerTrigger", "1100100")
//      .option("failOnDataLoss", "false")
      .load()

    val kafkaData = kafkaDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val query = kafkaData
      .writeStream
      .format("json")
      .option("path","C:/Users/DELL/Dekstop/datastreaming_team/json_dataset_consumed")
      .option("checkpointLocation", "C:/Users/DELL/Dekstop/datastreaming_team/kafka_checkpoints/latestJ1")
      .start()
//    C:\Users\DELL\Dekstop\datastreaming_team
    val q2=kafkaData.writeStream
      .format("console")
      .start()


    query.awaitTermination()
    q2.awaitTermination()
    spark.stop()
  }
}
