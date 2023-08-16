package producerConsumer

import com.typesafe.config.Config
import constants.ApplicationConstants._
import org.apache.spark.sql.SparkSession
import utils.ApplicationUtils._

object kafkaPipeLine {
  def main(args:Array[String]): Unit ={

    /** args(0) = ./src/conf/local.conf */
    val confPath: String = args(0)
    val appConf: Config = configuration(confPath)

    /** Creating the Spark Session. */
    val spark: SparkSession = createSparkSession(appConf)
    spark.sparkContext.setLogLevel("ERROR")

    /** Reading the input file path. */
    val dataInputPath = appConf.getString(FILE_INPUT_PATH)

    /** Calling the produce method to write the dataset to kafka topic. */
    producerConsumer.Producer.produce(spark, "sample.csv", KAFKA_FORMAT, KAFKA_SERVER_PORT, KAFKA_TOPIC)

    /** Calling the consume method to read the dataset from kafka topic into an Dataframe. */
    val consumedDF = producerConsumer.Consumer.consume(spark, KAFKA_FORMAT, KAFKA_SERVER_PORT, KAFKA_TOPIC)

    consumedDF.show(false)
    /** Calling the LogicImplementation to perform business specific Transformations and Actions.*/
    service.BusinessLogicService.LogicImplementation(spark, consumedDF)
    println("SUCCESSFUL.")
    spark.stop()
  }
}
