package service

import com.typesafe.config.Config
import constants.ApplicationConstants._
import org.apache.spark.sql.SparkSession
import ConsumerJson.ConsumerJSON
import ProducerJson.producerJSON
import service.BusinessLogicService.{convertToInteger, deduplication, extractDate, integerToBoolean, productIdCheck, retailPriceCheck, sellingChannelCheck}
import service.JsonReader.jsonReader
import transform.Transform.{enrichDataFrame, segregateSellingChannel}

object DataPipeline {
  def execute(appConf: Config)(implicit sparkSession: SparkSession): Unit = {

    //get data from conf file
    val jsonDataPath: String = appConf.getString(DATA_FROM_KAFKA_PATH)
    val sourceDataPath: String = appConf.getString(SOURCE_DATA_PATH)

    //producing and consuming the data
    producerJSON(sourceDataPath, BOOTSTRAP_SERVER, KAFKA_TOPIC)
    val df = ConsumerJSON(KAFKA_TOPIC, BOOTSTRAP_SERVER)
    //val initialDf = jsonReader(jsonDataPath)

    //applying business logic
    val deduplicateDf = deduplication(df, PRIMARY_KEY_COLS)
    val validSellingChannelDf = sellingChannelCheck(deduplicateDf, ERROR_TABLE, VALID_SELLING_CHANNEL)
    val validRetailPriceDf = retailPriceCheck(validSellingChannelDf, ERROR_TABLE)
    val validProductIdDf = productIdCheck(validRetailPriceDf, ERROR_TABLE)

    val booleanColumnDf = integerToBoolean(validProductIdDf, PROMOTION_ELIGIBILITY_COL)
    val integerColumnDf = convertToInteger(booleanColumnDf, ONHAND_QUANTITY_COL)
    val extractedDateDf = extractDate(integerColumnDf, CREATE_DATE_COL)

    val updateDateColEnrichedDf = enrichDataFrame(extractedDateDf)

    //loading the records to the database based on the selling channels
    segregateSellingChannel(updateDateColEnrichedDf)

  }
}