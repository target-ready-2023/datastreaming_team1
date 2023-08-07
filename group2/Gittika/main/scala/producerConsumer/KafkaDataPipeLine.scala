package producerConsumer

import com.typesafe.config.Config
import constants.ApplicationConstants.{DATA_INPUT_PATH, SOURCE_DATA_FORMAT, SUBSCRIBER, TOPIC}
import org.apache.spark.sql.SparkSession
import utils.ApplicationUtils.{configuration, createSparkSession}


object KafkaDataPipeLine {
  def main(args: Array[String]): Unit = {
    
    val confPath: String = args(0)
    val appConf: Config = configuration(confPath)
    implicit val spark: SparkSession = createSparkSession(appConf)

    val dataInputPath: String = appConf.getString(DATA_INPUT_PATH)



    Producer.producerJSON(spark, dataInputPath, SOURCE_DATA_FORMAT, SUBSCRIBER, TOPIC)
    Consumer.consumer(spark,SUBSCRIBER, TOPIC)


  }
}
