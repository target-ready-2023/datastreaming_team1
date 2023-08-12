package service

import org.apache.spark.internal.Logging
import constants.AppConstants.{COLUMN_NAMES, CROSS_OVER_TABLE_NAME, ERROR_TABLE, FILE_FORMAT, FILE_INPUT_PATH, KAFKA_BOOTSTRAP_SERVER, ONLINE_ONLY_TABLE_NAME, PRIMARY_KEY_COLS, STORE_ONLY_TABLE_NAME, TABLE_SCHEMA, TOPIC_NAME, VALID_SELLING_CHANNEL}
import org.apache.spark.sql.SparkSession
import service.ConsumerService.consumer
import BusinessLogicService.{deduplicate, integerToBoolean, productIdValidator, retailPriceValidator, sellingChannelValidator}
import com.typesafe.config.Config
import exceptions.Exceptions.{ColumnNotFoundException, DataframeIsEmptyException, EmptyFilePathException, FilePathNotFoundException, FileReaderException, InvalidInputFormatException}
import service.DatabaseConnectionService.FileWriter
import service.ProducerService.producer
import utils.AppUtils.{configuration, createSparkSession}

object DataProcessorService extends Logging {
  def main(args: Array[String]): Unit = {
    val confPath = args(0)
    val appConf: Config = configuration(confPath)
    implicit val spark: SparkSession = createSparkSession(appConf)
    var exitCode = 0

    try {
      val fileInputPath = appConf.getString(FILE_INPUT_PATH)

      //producing and consuming the data to the desired kafka topic
      producer(fileInputPath, TOPIC_NAME, KAFKA_BOOTSTRAP_SERVER, COLUMN_NAMES)
      val inputDF = consumer(TOPIC_NAME, KAFKA_BOOTSTRAP_SERVER, FILE_FORMAT, TABLE_SCHEMA)

      //applying business logic on the dataframe
      val deduplicateDF = deduplicate(inputDF, PRIMARY_KEY_COLS)
      val validSellingChannelDF = sellingChannelValidator(inputDF, ERROR_TABLE, VALID_SELLING_CHANNEL)
      val validRetailPriceDF = retailPriceValidator(validSellingChannelDF, ERROR_TABLE)
      val validProductIdDF = productIdValidator(validRetailPriceDF, ERROR_TABLE)
      val booleanConvertedDF = integerToBoolean(validProductIdDF, "promotion_eligibility")

      booleanConvertedDF.show(false)

      //loading the records into the database and segregated based on their selling channel
      val crossOverDF = booleanConvertedDF.filter(deduplicateDF("selling_channel") === "Cross Over")
      FileWriter(crossOverDF, CROSS_OVER_TABLE_NAME, "overwrite")

      val onlineOnlyDF = booleanConvertedDF.filter(deduplicateDF("selling_channel") === "Online Only")
      FileWriter(onlineOnlyDF, ONLINE_ONLY_TABLE_NAME, "overwrite")

      val storeOnlyDF = booleanConvertedDF.filter(deduplicateDF("selling_channel") === "Store Only")
      FileWriter(storeOnlyDF, STORE_ONLY_TABLE_NAME, "overwrite")
    }

    catch {
      case ex: FileReaderException => log.error("File Reader Exception: " + ex.message)
        ex.printStackTrace()
        exitCode = 1
      case ex: DataframeIsEmptyException => log.error("DataFrameIsEmptyException:" + ex.message)
        ex.printStackTrace()
        exitCode = 1
      case ex: ColumnNotFoundException => log.error("ColumnNotFoundException:" + ex.message)
        ex.printStackTrace()
        exitCode = 1
      case ex: EmptyFilePathException => log.error("EmptyFilePathException:" + ex.message)
        ex.printStackTrace()
        exitCode = 1
      case ex: FilePathNotFoundException => log.error("FilePathNotFoundException:" + ex.message)
        ex.printStackTrace()
        exitCode = 1
      case ex: InvalidInputFormatException => log.error("InvalidInputFormatException: " + ex.message)
        ex.printStackTrace()
        exitCode = 1
    }

    finally {
      if (exitCode == 1) {
        System.exit(exitCode)
      }
      else {
        log.info("Data Processor execution completed")
      }
      spark.stop()

    }
  }
}
