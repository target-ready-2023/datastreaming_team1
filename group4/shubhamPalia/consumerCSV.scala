import org.apache.spark.sql.SparkSession
import utils.globalVariables._
object consumerCSV {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().appName("consumerCSV").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    import spark.implicits._
    /* reading data from kafka topic "datastreaming_database" */
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "datastreaming_database")
      .option("includeHeaders", "true")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    val query = kafkaDF
      .writeStream
      .format("json")
      .option("path", "D:/targetReadyDataSet1/consumed_data")
      .option("checkpointLocation", "D:/targetReadyDataSet1/consumed_data/kafka_checkpoints")
      .start()
    val q2 = kafkaDF.writeStream
      .format("console")
      .option("truncate",false)
      .start()
    query.awaitTermination()
    q2.awaitTermination()
    spark.stop()
  }
}
