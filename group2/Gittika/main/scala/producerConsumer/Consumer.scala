package producerConsumer

import org.apache.spark.sql.SparkSession

object Consumer {

  def consumer(spark: SparkSession, format: String, topic: String): Unit = {

    val kafkaDF = spark.readStream
      .format(format)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("includeHeaders", "true")
            .option("maxOffsetsPerTrigger", "1100100")
            .option("failOnDataLoss", "false")
      .load()

    val kafkaData = kafkaDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val query = kafkaData
      .writeStream
      .format("json")
      .option("path", "C:\\Users\\DELL\\Dekstop\\datastreaming_team - Copy\\data\\json_dataset_consumed")
      .option("checkpointLocation", "C:\\Users\\DELL\\Dekstop\\datastreaming_team - Copy\\kafka_checkpoints\\latestJ1")
      .start()


    //    C:\Users\DELL\Dekstop\datastreaming_team
    val q2 = kafkaData.writeStream
      .format("console")
      .start()


    query.awaitTermination()
    q2.awaitTermination()
    spark.stop()
  }
}
