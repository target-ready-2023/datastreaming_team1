package service

import Producer.producer
import constants.AppConstants.{COLUMN_NAMES, ERROR_TABLE, FILE_INPUT_PATH, KAFKA_BOOTSTRAP_SERVER, TOPIC_NAME}
import org.apache.spark.sql.SparkSession
import service.Consumer.consumer
import businessLogic.BusinessLogic.{productIdValidator, retailPriceValidator, sellingChannelValidator}
import com.typesafe.config.Config

object Pipeline {

  def execute(appConf : Config)(implicit sparkSession: SparkSession): Unit ={

    val fileInputPath = appConf.getString(FILE_INPUT_PATH)

    producer(fileInputPath, TOPIC_NAME, KAFKA_BOOTSTRAP_SERVER, COLUMN_NAMES )
    val inputDF = consumer(TOPIC_NAME, KAFKA_BOOTSTRAP_SERVER,"kafka")

    //applying business logic on the dataframe
    val validSellingChannelDF = sellingChannelValidator(inputDF, ERROR_TABLE)
    val validRetailPriceDF = retailPriceValidator(validSellingChannelDF, ERROR_TABLE)
    val validProductIdDF = productIdValidator(validRetailPriceDF, ERROR_TABLE)

    validProductIdDF.show(false)
    print(validProductIdDF.count())

  }



}
