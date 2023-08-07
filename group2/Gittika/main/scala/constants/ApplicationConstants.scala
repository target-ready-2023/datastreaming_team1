package constants
import org.apache.spark.sql.types.{BooleanType, DateType, FloatType, IntegerType, StringType}

object ApplicationConstants {

  val APP_NAME = "spark.app.name"
  val APP_MASTER = "spark.app.master"
  val db_password = "gittika"
  val DATA_INPUT_PATH = "spark.app.dataInputPath"
  val SOURCE_DATA_FORMAT="com.databricks.spark.csv"
  val SUBSCRIBER="kafka"

  val KAFKA_READ_DATA_PATH="spark.app.kafkaReadDataPath"
  val TOPIC="datastream"
  val ERROR_TABLE="final_error_table" //later modify it to selling_error_table
  val CREATE_DATE="create_date"
  val PROMOTION_ELIGIBILITY="promotion_eligibility"
  val SELLING_CHANNEL="selling_channel"



  val columnsDataType = Map(
    "product_id" -> IntegerType,
    "location_id" -> IntegerType,
    "selling_channel" -> StringType,
    "prod_description" -> StringType,
    "retail_price" -> FloatType,
    "onhand_quantity" -> FloatType,
    "create_date" -> DateType,
    "promotion_eligibility" -> BooleanType,
    "update_date" -> DateType
  )


}
