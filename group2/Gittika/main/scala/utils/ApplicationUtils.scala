package utils
import com.typesafe.config.{Config, ConfigFactory}
import constants.ApplicationConstants.{APP_MASTER, APP_NAME}
import org.apache.spark.sql.SparkSession
import java.io.File

object ApplicationUtils {
  def configuration(inputPath: String): Config = {
    val parsedConfig = ConfigFactory.parseFile(new File(inputPath))
    val appConf: Config = ConfigFactory.load(parsedConfig)
    appConf
  }
  def createSparkSession(inputAppConf: Config): SparkSession = {
    implicit val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder()
        .appName(inputAppConf.getString(APP_NAME))
        .master(inputAppConf.getString(APP_MASTER))
        .getOrCreate()
    )
    spark
  }
}


