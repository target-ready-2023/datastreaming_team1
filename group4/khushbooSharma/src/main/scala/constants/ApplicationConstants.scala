package constants

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ApplicationConstants {
  val APP_NAME = "spark.app.name"
  val APP_MASTER = "spark.app.master"

  //input path
  val FILE_INPUT_PATH: String = "spark.app.dataStreamInputPath"

  //primary keys
  val DATA_STREAM_PRIMARY_KEYS: Seq[String] = Seq("product_id", "location_id")

  val KAFKA_FORMAT = "kafka"
  val KAFKA_SERVER_PORT = "localhost:9092"
  val KAFKA_TOPIC = "datastream3"

  /** THIS IS THROWING ERROR NEED TO CHECK THIS*/
  val VALID_CHANNELS: List[String] =
    List(
      "Cross Over"
      , "Store Only"
      , "Online Only"
    )

  val ENCRYPTED_DATABASE_PASSWORD: String = "D:/TargetCoorporationPhaseSecond/latest/datastreaming_team1/passwords/encrypted_password.txt"

  val ERROR_SELLING_CHANNEL="error_selling_channel"
  val ERROR_RETAIL_PRICE="error_retail_price"
  val ERROR_PRODUCT_ID="error_product_id"

  val CROSS_OVER_SELLING_CHANNEL = "Cross Over"
  val ONLINE_ONLY_SELLING_CHANNEL = "Online Only"
  val STORE_ONLY_SELLING_CHANNEL = "Store Only"

  val DATA_STREAM_COLUMN_NAMES = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")


  val KEY_COLUMN_NAME:String = "product_id"
  val KEY = "key"
  val VALUE = "value"

  val SELLING_CHANNEL = "selling_channel"
  val RETAIL_PRICE = "retail_price"
  val PRODUCT_ID = "product_id"


  val DATABASE_URL: String = "spark.app.databaseURL"

  val ERR_TABLE_NULL_CHECK = "error_table_nullCheck"
  val ERR_TABLE_DUP_CHECK = "error_table_duplicateCheck"

  val TABLE_NAME = "table_try_6"

  val DB_SOURCE = "jdbc"
  val JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
  val DB_USER = "root"
  val DB_PASSWORD = "root"
  val DB_WRITE_MODE = "overwrite"

}