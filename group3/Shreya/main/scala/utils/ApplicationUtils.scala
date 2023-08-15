package utils

import com.typesafe.config.{Config, ConfigFactory}
import constants.ApplicationConstants.{APP_MASTER, APP_NAME}
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object ApplicationUtils{

  //configuration
  def configuration(inputPath: String): Config = {
    val parsedConfig = ConfigFactory.parseFile(new File(inputPath))
    val appConf: Config = ConfigFactory.load(parsedConfig)
    appConf
  }

  //creating the spark session
  def createSparkSession(inputAppConf: Config): SparkSession = {
    implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName(inputAppConf.getString(APP_NAME))
        .master(inputAppConf.getString(APP_MASTER))
        .getOrCreate()
    )
    spark
  }

  //checking for exceptions
  def check(inputDF: DataFrame, colName: Seq[String]): Unit = {
    if (inputDF.count() == 0) {
      throw DataframeIsEmptyException("The dataframe is empty")
    } else {
      colName.foreach { (element: String) =>
        if (!inputDF.columns.contains(element))
          throw ColumnNotFoundException("The specified column does not exist")
      }
    }
  }

}