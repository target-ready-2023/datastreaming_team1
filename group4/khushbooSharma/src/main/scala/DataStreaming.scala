import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import utils.ApplicationUtils.{configuration, createSparkSession}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.Level
object DataStreaming {
  def main(args: Array[String]) {
    val confPath = args(0)
    val appConf: Config = configuration(confPath)
    val spark: SparkSession = createSparkSession(appConf)

    val context: LoggerContext = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = context.getConfiguration
    config.getRootLogger.setLevel(Level.INFO)
    config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME).addAppender(config.getAppender("ConsoleAppender"), Level.INFO, null)
    Configurator.initialize(config)
    val logger = LogManager.getLogger(getClass)

    service.DataStreamingServices.execute(spark,appConf,logger)

    spark.stop()
  }
}