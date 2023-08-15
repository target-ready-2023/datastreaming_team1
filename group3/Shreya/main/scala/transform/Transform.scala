package transform

import constants.ApplicationConstants.{CROSSOVER_PRODUCTS_TABLE, ONLINE_PRODUCTS_TABLE, STORE_PRODUCTS_TABLE}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import service.DbWriter.databaseWriter

object Transform{
  def segregateSellingChannel(df: DataFrame): Unit ={

    val onlineProductsDf = df.filter(col("selling_channel") ==="Online Only")
    databaseWriter(onlineProductsDf, ONLINE_PRODUCTS_TABLE, "overwrite")

    val crossoverProductsDf = df.filter(col("selling_channel") ==="Cross Over")
    databaseWriter(crossoverProductsDf, CROSSOVER_PRODUCTS_TABLE, "overwrite")

    val storeProductsDf = df.filter(col("selling_channel") ==="Store Only")
    databaseWriter(storeProductsDf, STORE_PRODUCTS_TABLE, "overwrite")
  }

  def enrichDataFrame(df: DataFrame): DataFrame = {
    val updateDateDf = df.withColumn("update_date", col("create_date"))
    updateDateDf
  }

}