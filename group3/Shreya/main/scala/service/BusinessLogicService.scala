package service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length, lit, substring, when}
import org.apache.spark.sql.types.IntegerType
import service.DbWriter.databaseWriter
import utils.ApplicationUtils.check

object BusinessLogicService {

  //Move records with different selling channel other than ONLINE, STORE, CROSSOVER into an error table
  def sellingChannelCheck(df: DataFrame, errorTable: String, validSellingChannels : Seq[String]): DataFrame = {
    val errorDf = df.filter(!col("selling_channel").isin(validSellingChannels: _*))
    val errorDfWithReason = errorDf.withColumn("reason", lit("selling channel"))
    databaseWriter(errorDfWithReason, errorTable, "append")
    val validSellingChannelDf = df.except(errorDf)
    validSellingChannelDf
  }

  //Move records with price null or 0 into an error table
  def retailPriceCheck(df: DataFrame, errorTable: String): DataFrame = {
    val errorDf = df.filter(col("retail_price").isNull || col("retail_price") === 0.0)
    val errorDfWithReason = errorDf.withColumn("reason", lit("retail price"))
    databaseWriter(errorDfWithReason, errorTable, "append")
    val validPriceDf = df.except(errorDf)
    validPriceDf
  }

  //Checking if product_id is numeric and 8 digit
  def productIdCheck(df: DataFrame, errorTable: String): DataFrame = {
    val numericProductIdDf = df.filter(col("product_id").cast("int").isNotNull)
    val validProductIdDf = numericProductIdDf.filter(length(col("product_id")) === 8)
    val errorDf = df.except(validProductIdDf)
    val errorDfWithReason = errorDf.withColumn("reason", lit("product id"))
    databaseWriter(errorDfWithReason, errorTable, "append")
    validProductIdDf
  }

  //Deduplication based on product_id and location_id
  def deduplication(df: DataFrame, primaryKeyCols : Seq[String]): DataFrame = {
    check(df, primaryKeyCols)
    val deduplicateDf = df.dropDuplicates(primaryKeyCols)
    deduplicateDf
  }

  //Convert the promotion_eligibility column from integer to boolean
  def integerToBoolean(df: DataFrame, colName: String): DataFrame={
    check(df, Seq(colName))
    val booleanColumnDf = df.withColumn(colName, when(col(colName) === 1, true).otherwise(false))
    booleanColumnDf
  }

  //Round off the onhand_quantity column to integer
  def convertToInteger(df: DataFrame, colName: String): DataFrame={
    check(df, Seq(colName))
    val intColumnDf = df.withColumn(colName,col(colName).cast(IntegerType))
    intColumnDf
  }

  //Extract the date from create_date column
  def extractDate(df: DataFrame, dateCol: String): DataFrame={
    check(df, Seq(dateCol))
    val extractedDateDf = df.withColumn(dateCol, substring(col(dateCol), 1, 10))
    extractedDateDf
  }
}
