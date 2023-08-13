package utils
import utils.DBConnect.databaseWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object businessRules {
  def updateSellingChannel(df: DataFrame): DataFrame = {
    val selling_channel_error_table = df.filter(
      col("selling_channel") =!= "Cross Over"
        && col("selling_channel") =!= "Store Only"
        && col("selling_channel") =!= "Online Only"
    )
    println("creating selling_channel_error_table")
    databaseWriter(selling_channel_error_table, "selling_channel_error_table")
    val finalDF = df.except(selling_channel_error_table)
    finalDF
  }

  def updateRetailPrice(df: DataFrame): DataFrame = {
    val retail_price_error_table = df.filter(col("retail_price").isNull || col("retail_price") === 0.0)
    println("creating retail_price_error_table")
    databaseWriter(retail_price_error_table, "retail_price_error_table")
    val finalDF = df.except(retail_price_error_table)
    finalDF
  }

  def updateProductID(df: DataFrame): DataFrame = {
    val numericProductId = df.filter(col("product_id").cast("int").isNotNull)
    val nonNumericProductId = df.except(numericProductId)
    val validProductId = numericProductId.filter(length(col("product_id")) === 8)
    val illegal_product_id = numericProductId.except(validProductId)
    val product_id_error_table = illegal_product_id.union(nonNumericProductId)
    println("creating product_id_error_table")
    databaseWriter(product_id_error_table, "product_id_error_table")
    validProductId
  }

  def integerToBoolean(df: DataFrame, colName: String): DataFrame = {
    val newDF = df.withColumn(colName, when(col(colName) === 1, "yes").otherwise("no"))
    newDF
  }

  def stringToDate(spark: SparkSession, df: DataFrame, dateCol: String): DataFrame = {
    val extractedDateDF = df.withColumn("status", when(col("status") === 1, "yes").otherwise("no"))
    extractedDateDF
  }
}
