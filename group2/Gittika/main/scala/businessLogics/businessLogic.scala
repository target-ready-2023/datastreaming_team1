package businessLogics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import service.DbConnection.errorTableWriter

object businessLogic {

  def json_to_Df(spark: SparkSession, path: String) = {
//    val sparkSession = SparkSession.builder().master("local[*]").appName("TargetReady1").getOrCreate()

    val df = spark.read.option("header", "true").format("json").load(path)

    //the above dataframe have 2 columns key and value and the entire table is present in value column
    val data = df.select("value").collect().toSeq

    // Define the schema for the JSON data
    val schema = new StructType()
      .add("product_id", StringType)
      .add("location_id", StringType)
      .add("selling_channel", StringType)
      .add("prod_description", StringType)
      .add("retail_price", StringType)
      .add("onhand_quantity", StringType)
      .add("create_date", StringType)
      .add("promotion_eligibility", StringType)

    // Parse the JSON data into individual columns
    val parsedDf = df.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    println(parsedDf.count())
    parsedDf
  }


  def sellingChannelCheck(df: DataFrame, errorTable: String): DataFrame = {
    val validSellingChannels = Seq("Online Only", "Store Only", "Cross Over")
    val filteredDF = df.filter(col("selling_channel").isin(validSellingChannels: _*))
    val errorDF = df.filter(!col("selling_channel").isin(validSellingChannels: _*))
    val error_DF = errorDF.withColumn("reason", lit("selling_channel"))
    errorTableWriter(error_DF, errorTable)
    filteredDF
  }



  /** *******************************************************Check price is not null or 0.If so, move it to error table************************************************* */
  def nullCheckInRetailPrice(df: DataFrame, errorTable: String): DataFrame = {
    val filteredDF = df.filter(col("retail_price").isNotNull && col("retail_price").cast(IntegerType) != 0)
    val nullDF = df.filter(col("retail_price").isNull || col("retail_price").cast(IntegerType) === 0)
    val error_DF = nullDF.withColumn("reason", lit("retail_price"))
    println(nullDF.count())
    nullDF.show()
    errorTableWriter(error_DF, errorTable)
    filteredDF
  }


  /** ***************************************************Check Product id should be only numeric and 8 digit.********************************************** */
  def productIDCheckNumeric(df: DataFrame, errorTable: String): DataFrame = {
    val validProduct_id_DF = df.filter(s"CAST(${col("product_id")} AS DECIMAL(8,0)) IS NOT NULL")
    val errorDF = df.filter(s"CAST(${col("product_id")} AS DECIMAL(8,0)) IS  NULL")
    val error_DF = errorDF.withColumn("reason", lit("product_id"))
    errorTableWriter(error_DF, errorTable)
    validProduct_id_DF
  }


  /** ***************************************Remove duplicate if any based on product_id and location_id **************************************************** */
  def deduplicate(df: DataFrame): DataFrame = {
    val deduplicateDF = df.dropDuplicates("product_id", "location_id")
    println(deduplicateDF.count())
    deduplicateDF
  }



/**************************************************String to Date format **********************************************************/
  def stringToDate(spark: SparkSession, df: DataFrame, dateCol: String): DataFrame = {
    val extractedDateDF: DataFrame =
      df.withColumn(dateCol, substring(col(dateCol), 1, 10))
    extractedDateDF
  }


  /*****************************************************Integer to boolean promotion eligiblity******************************************/
  def integerToBoolean(df: DataFrame, colName: String): DataFrame = {
    val newDF = df.withColumn(colName, when(col(colName) === 1, true).otherwise(false))
    newDF
  }


}
