package service

import bussinessLogicsPackage.BSlogics.{dumpInDB, gettingInvalidRecords, invalidProductIDRemover, invalidRetailPriceRemover, invalidSellingChannelRemover, removeDuplicates}
import constants.ApplicationConstants.{CROSS_OVER_SELLING_CHANNEL, DATABASE_URL, FILE_INPUT_PATH, KAFKA_FORMAT, KAFKA_SERVER_PORT, KAFKA_TOPIC, ONLINE_ONLY_SELLING_CHANNEL, STORE_ONLY_SELLING_CHANNEL}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import org.apache.logging.log4j.Logger
import org.slf4j.LoggerFactory


object DataStreamingServices{
  def execute(spark:SparkSession,appConf:Config,logger: Logger) {


    //inputting data from conf file
    val fileInputPath: String = appConf.getString(FILE_INPUT_PATH)
    val database_URL = appConf.getString(DATABASE_URL)

    /** PRODUCE MESSAGES*/

    logger.info("*************************************** PRODUCING MESSAGES TO KAFKA ***************************************")
//    println("*************************************** PRODUCING MESSAGES TO KAFKA ***************************************")
    producerConsumer.Producer.prodcue(spark,fileInputPath,KAFKA_FORMAT,KAFKA_SERVER_PORT,KAFKA_TOPIC)

    /** CONSUME MESSAGES*/

    logger.info("*************************************** CONSUMING MESSAGES FROM KAFKA ***************************************")
//    println("*************************************** CONSUMING MESSAGES FROM KAFKA ***************************************")
    val df = producerConsumer.Consumer.consume(spark,KAFKA_FORMAT,KAFKA_SERVER_PORT,KAFKA_TOPIC)

    /** GET INVALID RECORDS*/
    val error_df = gettingInvalidRecords(spark, df)

    /** STORE THE ERRORED DATA INTO DATABASE :: 87370106 --> NULL \'\'\' 86872508-->0.0*/
    logger.info("*************************************** DUMPING ERROR DATA INTO DATABASE ***************************************")
//    println("*************************************** DUMPING ERROR DATA INTO DATABASE ***************************************")
    dumpInDB(error_df,"error_data",database_URL)

    /**
     * TESTING RECORDS WHICH ARE NOT VALID*
    error_df.filter(error_df("product_id")==="87370106").show()
    error_df.filter(error_df("retail_price")===0.0).show()
     */
    /** REMOVING INVALID RECORDS*/
    logger.info("*************************************** REMOVING INVALID RECORDS ***************************************")
//    println("*************************************** REMOVING INVALID RECORDS ***************************************")

    val removedInvalidSellingChannel = invalidSellingChannelRemover(df)
    val removedInvalidRetailPrice = invalidRetailPriceRemover(removedInvalidSellingChannel)
    val removedInvalidProductID = invalidProductIDRemover(spark,removedInvalidRetailPrice)

    removedInvalidProductID.show(false)

    /**Remove duplicates*/
    val deduplicatedDF = removeDuplicates(removedInvalidProductID)
    deduplicatedDF.show(false)


    /**SEGREGATION*/

    logger.info("*************************************** SEGREGATING CROSS OVER ***************************************")
//    println("*************************************** SEGREGATING CROSS OVER ***************************************")
    val crossOverDF = deduplicatedDF.filter(deduplicatedDF("selling_channel")===CROSS_OVER_SELLING_CHANNEL)
    dumpInDB(crossOverDF,"cross_over_selling_channel",database_URL)

    logger.info("*************************************** SEGREGATING ONLINE ONLY ***************************************")
//    println("*************************************** SEGREGATING ONLINE ONLY ***************************************")
    val onlineOnlyDF = deduplicatedDF.filter(deduplicatedDF("selling_channel")===ONLINE_ONLY_SELLING_CHANNEL)
    dumpInDB(onlineOnlyDF,"online_only_selling_channel",database_URL)

    logger.info("*************************************** SEGREGATING STORE ONLY ***************************************")
//    println("*************************************** SEGREGATING STORE ONLY ***************************************")
    val storeOnlyDF = deduplicatedDF.filter(deduplicatedDF("selling_channel")===STORE_ONLY_SELLING_CHANNEL)
    dumpInDB(storeOnlyDF,"store_only_selling_channel",database_URL)

  }
}