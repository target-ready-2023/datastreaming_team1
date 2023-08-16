package constants

object ApplicationConstants {

  /** Config constants. */
  val APP_NAME = "spark.app.name"
  val APP_MASTER = "spark.app.master"
  val FILE_INPUT_PATH: String = "spark.app.dataStreamInputPath"

  /** PRIMARY KEY constant */
  val DATA_STREAM_PRIMARY_KEYS: Seq[String] = Seq("product_id", "location_id")

  /** Kafka constants. */
  val KAFKA_FORMAT = "kafka"
  val KAFKA_SERVER_PORT = "localhost:9092"
  val KAFKA_TOPIC = "data_stream"
  val DATA_STREAM_COLUMN_NAMES = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")

  /** Dataset constants. */
  val KEY_COLUMN_NAME = "product_id"
  val KEY = "key"
  val VALUE = "value"
  val SELLING_CHANNEL = "selling_channel"
  val RETAIL_PRICE = "retail_price"
  val PRODUCT_ID = "product_id"

  /** Database table constants. */
  val ERROR_TABLE  = "datastream_error_table"
  val ONLINE_ONLY_TABLE = "online_only_table"
  val STORE_ONLY_TABLE = "store_only_table"
  val CROSS_OVER_TABLE = "cross_over_table"

  /** Database connection constants. */
  val DB_SOURCE = "jdbc"
  val JDBC_DRIVER = "org.postgresql.Driver"
  val DB_USER = "postgres"
  val DB_PASSWORD = "Venky@44"
  val DB_WRITE_MODE = "overwrite"
}