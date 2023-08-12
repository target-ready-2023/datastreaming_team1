package service

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, functions}
import service.DatabaseConnectionService.FileWriter

object BusinessLogicService {

  //remove invalid selling channels from the dataframe
  def sellingChannelValidator(inputDF: DataFrame, tableName: String, validSellingChannels : Seq[String]): DataFrame = {
    val errorDF = inputDF.filter(!col("selling_channel").isin(validSellingChannels: _*))
    val errorDFWithReason = errorDF.withColumn("reason", lit("selling_channel"))
    FileWriter(errorDFWithReason, tableName, "append")
    val validSellingChannelDF = inputDF.except(errorDF)
    validSellingChannelDF
  }

  //remove invalid price values such as 0 and nulls from the retail price
  def retailPriceValidator(inputDF: DataFrame, tableName: String): DataFrame = {
    val errorDF = inputDF.filter(inputDF("retail_price") === 0 || inputDF("retail_price").isNull)
    val errorDFWithReason = errorDF.withColumn("reason", lit("retail_price"))
    FileWriter(errorDFWithReason, tableName, "append")
    val validRetailPriceDF = inputDF.except(errorDF)
    validRetailPriceDF
  }

  //remove invalid product Id with less than
  def productIdValidator(inputDF: DataFrame, tableName: String): DataFrame = {
    val numericDF = inputDF.filter(col("product_id").cast("int").isNotNull)
    val validProductIdDF = numericDF.filter(functions.length(col("product_id")) === 8)
    val errorDF = inputDF.except(validProductIdDF)
    val errorDFWithReason = errorDF.withColumn("reason", lit("product_id"))
    FileWriter(errorDFWithReason, tableName, "append")
    validProductIdDF
  }

  //removing duplicates from the dataframe
  def deduplicate(inputDF: DataFrame): DataFrame = {
    val deduplicateDF = inputDF.dropDuplicates("product_id", "location_id")
    deduplicateDF
  }
  
  //converting the integer values into boolean values
  //1 corresponds to true and 0 corresponds to false
  def integerToBoolean(inputDF: DataFrame, colName: String): DataFrame = {
    val convertedDF = inputDF.withColumn(colName, when(col(colName) === 1, true).otherwise(false))
    convertedDF
  }

}
