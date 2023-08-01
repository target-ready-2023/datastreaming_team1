import org.apache.spark.sql.SparkSession

object NewConsumer {
    def main(args:Array[String]): Unit ={

        val spark = SparkSession.builder()
          .appName("consumer")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("error")
        import spark.implicits._

//        val df = spark.readStream
//          .format("kafka")
//          .option("kafka.bootstrap.servers","localhost:9092")
//          .option("subscribe","stream")
//          .load()
//          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
//          .writeStream
//          .format("console")
//          .option("truncate",false)
//          .outputMode("append")
//          .start()
//
//        df.awaitTermination()

        val df = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "stream")
          .load()
          .selectExpr("CAST(value AS STRING)").as[String]

        val q1 = df.writeStream
          .format("json")
          .option("path","D:/phase2/data/out2/Records")
          .option("checkpointLocation","D:/phase2/data/out2/CP")
          .start()

        val q2 = df.writeStream
          .format("console")
          .option("truncate",false)
          .start()

        q1.awaitTermination()
        q2.awaitTermination()
    }
}
