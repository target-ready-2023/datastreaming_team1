package bussinessLogicsPackage

import constants.ApplicationConstants.{DATABASE_URL, DATA_STREAM_PRIMARY_KEYS, DB_PASSWORD, DB_USER, DB_WRITE_MODE, ENCRYPTED_DATABASE_PASSWORD, ERROR_PRODUCT_ID, ERROR_RETAIL_PRICE, ERROR_SELLING_CHANNEL, JDBC_DRIVER, PRODUCT_ID, RETAIL_PRICE, SELLING_CHANNEL, VALID_CHANNELS}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import java.util.Base64

object BSlogics {

  def decryptPassword(encryptedPasswordPath: String): String = {
    //read from file and decrypt password
    val source = scala.io.Source.fromFile(encryptedPasswordPath)
    val encryptedPassword = try source.mkString finally source.close()
    val decoded = Base64.getDecoder.decode(encryptedPassword)

    //return the decrypted password as a string
    val password = new String(decoded, StandardCharsets.UTF_8)
    password
  }

  def dumpInDB(error_df:DataFrame,tableName:String,database_URL:String): Unit ={
    val password = decryptPassword(ENCRYPTED_DATABASE_PASSWORD)
    error_df.write
      .format("jdbc")
      .option("driver",JDBC_DRIVER)
      .option("url",database_URL)
      .option("dbtable", tableName)
      .option("user", DB_USER)
      .option("password", password)
      .mode(DB_WRITE_MODE)
      .save()

  }

  def invalidSellingChannelRemover(df:DataFrame):DataFrame ={
    val valid_channel_df = df.filter(df(SELLING_CHANNEL).isin("Cross Over"
      , "Store Only"
      , "Online Only"))
    valid_channel_df
  }

  def invalidRetailPriceRemover(valid_channel_df:DataFrame): DataFrame ={

    /** REMOVING Nulls/0 FROM retail_price COLUMN */
    val removed_null_price_df = valid_channel_df.filter(valid_channel_df(RETAIL_PRICE)=!=0 && valid_channel_df(RETAIL_PRICE).isNotNull)
    removed_null_price_df
  }

  def invalidProductIDRemover(spark:SparkSession,removed_null_price_df:DataFrame): DataFrame={

    /** Remove record product_id which is not numeric and 8 digit into DATABASE*/

    val only_numeric_df = removed_null_price_df.filter(removed_null_price_df.col(PRODUCT_ID).cast("int").isNotNull)
    only_numeric_df.createOrReplaceTempView("TAB")

    val final_df = spark.sql("select * " +
      "from TAB where length(product_id) = 8")

    final_df
  }

  def removeDuplicates(final_df:DataFrame): DataFrame ={
//    val primaryKeyCols: Seq[String] = Seq("product_id", "location_id")

    val dfRemoveDuplicates = final_df.withColumn("rn", row_number().over(Window.partitionBy(DATA_STREAM_PRIMARY_KEYS.map(col): _*).orderBy(desc("create_date"))))
      .filter(col("rn") === 1).drop("rn")
    dfRemoveDuplicates
  }

  def gettingInvalidRecords(spark:SparkSession, raw_df:DataFrame): DataFrame ={


    val error_df_selling_channel = raw_df
      .withColumn(ERROR_SELLING_CHANNEL, when(raw_df(SELLING_CHANNEL).isin("Cross Over"
        , "Store Only"
        , "Online Only"), "").otherwise(raw_df(SELLING_CHANNEL)))


    val error_df_price = error_df_selling_channel.withColumn(ERROR_RETAIL_PRICE, when(raw_df(RETAIL_PRICE) === 0.0 || raw_df(RETAIL_PRICE).isNull,error_df_selling_channel("retail_price")).otherwise(""))

//    val error_df_product_id = error_df_price
//      .withColumn("error_product_id", when(!(length(col(PRODUCT_ID).cast("int").cast("string")) === 8), raw_df(PRODUCT_ID)).otherwise(""))
    val error_df_product_id = error_df_price
      .withColumn(ERROR_PRODUCT_ID, when(col(PRODUCT_ID).cast("int").isNull || length(col(PRODUCT_ID).cast("int").cast("string")) =!= 8, raw_df(PRODUCT_ID)).otherwise(""))
    error_df_product_id

  }

 /* def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bussinessLogicsPackage").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    /** PRODUCE MESSAGES*/
    producerConsumer.producerCSV.prodcue(spark,"D:/TargetCoorporationPhaseSecond/Data/targetReadyDataSet.csv","kafka","localhost:9092","datastreamTest12")

    /** CONSUME MESSAGES*/
    val df = producerConsumer.newConsumerCSV.consume(spark,"kafka","localhost:9092","datastreamTest12")

    /** GET INVALID RECORDS*/
    val error_df = gettingInvalidRecords(spark, df)

    /** STORE THE ERRORED DATA INTO DATABASE :: 87370106 --> NULL \'\'\' 86872508-->0.0*/
    println("*************************************** DUMPING ERROR DATA INTO DATABASE ***************************************")
    dumpInDB(error_df,"error_data")

    /**
     * TESTING RECORDS WHICH ARE NOT VALID*
    error_df.filter(error_df("product_id")==="87370106").show()
    error_df.filter(error_df("retail_price")===0.0).show()
     */
    /** REMOVING INVALID RECORDS*/
    println("*************************************** REMOVING INVALID RECORDS ***************************************")

    val removedInvalidSellingChannel = invalidSellingChannelRemover(df)
    val removedInvalidRetailPrice = invalidRetailPriceRemover(removedInvalidSellingChannel)
    val removedInvalidProductID = invalidProductIDRemover(spark,removedInvalidRetailPrice)

//    removedInvalidProductID.show(false)

    val deduplicatedDF = removeDuplicates(removedInvalidProductID)
    deduplicatedDF.show(false)

    /**SEGREGATION*/

    /**CROSS OVER*/
    val crossOverDF = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Cross Over")
    dumpInDB(crossOverDF,"cross_over_selling_channel")

    /**ONLINE ONLY*/
    val onlineOnlyDF = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Online Only")
    dumpInDB(onlineOnlyDF,"online_only_selling_channel")


    /**STORE ONLY*/
    val storeOnlyDF = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Store Only")
    dumpInDB(storeOnlyDF,"store_only_selling_channel")

    spark.stop()
  }

  */
}
