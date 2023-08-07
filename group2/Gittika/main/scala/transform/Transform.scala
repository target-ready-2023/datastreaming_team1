package transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}

object Transform {

  def enrichDataFrame(df: DataFrame): DataFrame = {
    val newDF = df.withColumn("update_date", col("create_date"))
    newDF
  }

  def selling_channel_seggregate(df: DataFrame, columnName: String): DataFrame = {
    val newDF = df.withColumn("ONLINE", when(col(columnName) === "Online Only", true).otherwise(lit(null)))
      .withColumn("CROSSOVER", when(col(columnName) === "Cross Over", true).otherwise(lit(null)))
      .withColumn("STORE", when(col(columnName) === "Store Only", true).otherwise(lit(null)))
      .drop(columnName)
    newDF

  }

}
