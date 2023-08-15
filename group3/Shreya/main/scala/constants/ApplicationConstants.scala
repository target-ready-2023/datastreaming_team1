package constants

object ApplicationConstants{

  val APP_NAME = "spark.app.name"
  val APP_MASTER = "spark.app.master"

  val DATA_FROM_KAFKA_PATH = "spark.app.jsonOutputPath"

  val SOURCE_DATA_PATH = "spark.app.csvInputPath"
  val BOOTSTRAP_SERVER = "localhost:9092"
  val KAFKA_TOPIC = "datastream"

  val VALID_SELLING_CHANNEL = Seq("Online Only", "Store Only", "Cross Over")
  val PRIMARY_KEY_COLS = Seq("product_id", "location_id")

  val ERROR_TABLE = "error_table"
  val ONLINE_PRODUCTS_TABLE = "online_products"
  val CROSSOVER_PRODUCTS_TABLE = "crossover_products"
  val STORE_PRODUCTS_TABLE = "store_products"

  val PROMOTION_ELIGIBILITY_COL= "promotion_eligibility"
  val ONHAND_QUANTITY_COL = "onhand_quantity"
  val CREATE_DATE_COL = "create_date"

  //database connection
  val DB_SOURCE = "jdbc"
  val DB_DRIVER = "com.mysql.cj.jdbc.Driver"
  val DB_USERNAME = "root"
  val DB_PASSWORD = "@dm!n"

}