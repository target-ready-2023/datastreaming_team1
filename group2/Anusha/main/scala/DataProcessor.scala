import com.typesafe.config.Config
import kafka.utils.Logging
import org.apache.spark.sql.SparkSession
import service.Pipeline.execute
import utils.AppUtils.{configuration, createSparkSession}

object DataProcessor extends Logging {
  def main(args: Array[String]): Unit = {
    val confPath = args(0)
    val appConf: Config = configuration(confPath)
    implicit val spark: SparkSession = createSparkSession(appConf)
    var exitCode = 0

    try{
      execute(appConf)
    }

  }

  }
