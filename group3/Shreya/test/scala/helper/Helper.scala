package helper

import org.apache.spark.sql.SparkSession

object Helper{

  def createSparkSession(): SparkSession = {
    implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("UpCurve Data Pipeline Test")
        .master("local[*]")
        .getOrCreate())
    spark
  }

  val TEST_INPUT_FILE_PATH = "C:\\Users\\Shreya\\Target_Ready\\data_streaming_project\\datastreaming\\data\\testData.csv"
  val TEST_TOPIC_NAME = "datastream_test"

  val TEST_ERROR_TABLE = "error_table_test"

}