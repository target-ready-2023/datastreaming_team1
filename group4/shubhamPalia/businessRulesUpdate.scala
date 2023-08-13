package utils
import utils.businessRules._
import DBConnect.databaseWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, length}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object businessRulesUpdate{
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder.master("local[*]").appName("businessRulesUpdate").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("error")
    val jsonDirectory = "D:\\targetReadyDataSet1\\consumed_data"
    val dfRaw = spark.read.option("header", "true").json(jsonDirectory)
    val schema = new StructType()
      .add("product_id", StringType, true)
      .add("location_id", StringType, true)
      .add("selling_channel", StringType, true)
      .add("prod_description", StringType, true)
      .add("retail_price", StringType, true)
      .add("onhand_quantity", StringType, true)
      .add("create_date", StringType, true)
      .add("promotion_eligibility", StringType, true)
    /* Reading the dataframe and displaying the first 20 rows*/
    val df = dfRaw.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
      .select(col("parsed_value.*"))
    df.show()
    /* Applying business logics as
    * 1. removing all selling channels other than ONLINE, STORE & CROSSOVER
    * 2. removing all null or 0 values in retail price column
    * 3. removing all rows with invalid product ids*/
    val updatedSellingChannelDF = updateSellingChannel(df)
    val updatedRetailPriceDF = updateRetailPrice(updatedSellingChannelDF)
    var finalDF = updateProductID(updatedRetailPriceDF)
    finalDF = finalDF.dropDuplicates("product_id", "location_id")
    finalDF = integerToBoolean(finalDF, "promotion_eligibility")
    databaseWriter(finalDF, "datastream_after_businessLogics")
    finalDF.show()
  }
}