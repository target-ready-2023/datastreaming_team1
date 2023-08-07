package utils

import com.typesafe.config.{Config, ConfigFactory}
import constants.ApplicationConstants.{APP_MASTER, APP_NAME}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import scala.io.Source

object ApplicationUtils {

  //configuration
  def configuration(inputPath: String): Config = {
    val parsedConfig = ConfigFactory.parseFile(new File(inputPath))
    val appConf: Config = ConfigFactory.load(parsedConfig)
    appConf
  }

  //Creating the spark session
  def createSparkSession(inputAppConf: Config): SparkSession = {
    val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName(inputAppConf.getString(APP_NAME))
        .master(inputAppConf.getString(APP_MASTER))
        .getOrCreate()
    )
    spark
  }

}