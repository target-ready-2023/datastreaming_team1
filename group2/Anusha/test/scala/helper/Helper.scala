package helper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object Helper {
  def createSparkSession(): SparkSession = {
    implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("Data Processor Test")
        .master("local[*]")
        .getOrCreate())
    spark
  }

  val TEST_INPUT_FILE_PATH = "C:\\Users\\acer\\Desktop\\datastreaming_team1-main\\datastreaming_team1-main\\data\\targetReadyTestDataset.csv"
  val TEST_TOPIC_NAME = "datastreamtest"
  val TEST_KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
  val TEST_COLUMN_NAMES = Seq("product_id",
    "location_id",
    "selling_channel",
    "prod_description",
    "retail_price",
    "onhand_quantity",
    "create_date",
    "promotion_eligibility")
  val TEST_FILE_FORMAT = "kafka"
  val TEST_TABLE_SCHEMA = new StructType()
    .add("product_id",StringType)
    .add("location_id",StringType)
    .add("selling_channel",StringType)
    .add("prod_description",StringType)
    .add("retail_price",StringType)
    .add("onhand_quantity",StringType)
    .add("create_date",StringType)
    .add("promotion_eligibility",StringType)

}
