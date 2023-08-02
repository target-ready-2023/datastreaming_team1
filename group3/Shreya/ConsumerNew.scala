import org.apache.spark.sql.SparkSession

object ConsumerNew{
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ConsumerCSV").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

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

    val q2 = df.writeStream
      .format("console")
      .start()

    q1.awaitTermination()
    q2.awaitTermination()
  }
}