package helper

import org.apache.spark.sql.SparkSession

object Helper {
  def createSparkSession(): SparkSession = {
    implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("Data Processor Test")
        .master("local[*]")
        .getOrCreate())
    spark
  }

}
