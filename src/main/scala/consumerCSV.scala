import org.apache.spark.sql.SparkSession

object consumerCSV {
  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .appName("consumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.implicits._

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "datastream")
      .load()

    val kafkaData = kafkaDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val query = kafkaData
      .writeStream
      .format("json")
      .option("path", "/Users/z083276/Downloads/abcd")
      .option("checkpointLocation", "/Users/z083276/Downloads/kafka_checkpoints")
      .start()

    val q2 = kafkaData.writeStream
      .format("console")
      .start()


    query.awaitTermination(20000)
    q2.awaitTermination(20000)
    spark.stop()
  }
}
