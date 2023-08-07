import businessLogics.businessLogic.{integerToBoolean, nullCheckInRetailPrice, productIDCheckNumeric, sellingChannelCheck, stringToDate}
import com.typesafe.config.Config
import constants.ApplicationConstants._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import producerConsumer.{ConsumeDataframe, Producer}
import transform.Transform.{enrichDataFrame, selling_channel_seggregate}
import utils.ApplicationUtils.{configuration, createSparkSession}

object DataStreamPipe extends Logging {
  def main(args: Array[String]): Unit = {
    val confPath: String = args(0)
    val appConf: Config = configuration(confPath)
    implicit val spark: SparkSession = createSparkSession(appConf)
    val dataInputPath: String = appConf.getString(KAFKA_READ_DATA_PATH)


    /** ********************************************BUSINESS LOGICS************************************************************* */
//    val initialDF = json_to_Df(spark, dataInputPath)
    val error_table = ERROR_TABLE

    Producer.producerJSON(spark,"C:\\Users\\DELL\\Dekstop\\datastreaming_team - Copy\\data\\targetReadyTestDataset.csv" , SOURCE_DATA_FORMAT, SUBSCRIBER, TOPIC)
   val df= ConsumeDataframe.consumerDataFrame(spark,SUBSCRIBER, TOPIC)
//    df.show()
    val sellingChannelCheckDF = sellingChannelCheck(df, error_table)
    val retailPriceCheckDF = nullCheckInRetailPrice(sellingChannelCheckDF, error_table)
    val product_id_check_DF = productIDCheckNumeric(retailPriceCheckDF, error_table)
    val dateTypeChangeDF = stringToDate(spark, product_id_check_DF, CREATE_DATE)
    val integerToBooleanConvertedDF = integerToBoolean(dateTypeChangeDF, PROMOTION_ELIGIBILITY)

    /** **********************Enriching and transforming the Dataframe************************************************** */
    val enrichDF = enrichDataFrame(integerToBooleanConvertedDF)


    val newDf = check.DataTypeCheck.redefineColumnTypes(enrichDF, columnsDataType)
    val seggretarnewDF = selling_channel_seggregate(newDf, SELLING_CHANNEL)
    seggretarnewDF.show()
    //        dbConnection.errorTableWriter(product_id_check_DF,"selling_table")


  }

}
