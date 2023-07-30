
  import org.apache.spark.sql.{SparkSession, DataFrame}
  import org.apache.spark.sql.functions._

  object Consumer {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("KafkaConsumerToFile")
        .master("local[*]")
        .getOrCreate()

      val kafkaTopic = "targetReady-sample"
      val kafkaBrokers = "localhost:9092"

      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBrokers)
        .option("subscribe", kafkaTopic)
        .load()

      val kafkaData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

      val outputPath = "C:\\Users\\acer\\Desktop\\TargetReady\\OutputDir"

      val query = kafkaData.writeStream
        .format("csv")
        .option("path", outputPath)
        .outputMode("append")
        .start()

      query.awaitTermination()
    }

}
