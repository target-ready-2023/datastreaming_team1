package service

import constants.ApplicationConstants._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object BusinessLogicService {
  def LogicImplementation(spark:SparkSession, df:DataFrame): DataFrame ={

    /** Move messages with different selling  channel other than ONLINE,STORE,CROSSOVER into an error table. */
    val selling_channel_errors = df.filter(df(SELLING_CHANNEL)=!="Cross Over" && df(SELLING_CHANNEL)=!="Store Only" && df(SELLING_CHANNEL)=!="Online Only")
//    println("selling_channel_errors.")
//    selling_channel_errors.show(false)
    DatabaseConnectionService.DatabaseTableWriter(selling_channel_errors.select(col("*"),lit("selling_channel_error").as("error")),ERROR_TABLE)

    /** Check price is not null or 0. If so, move it to error table. */
    val valid_records = df.filter(df(SELLING_CHANNEL)==="Cross Over" || df(SELLING_CHANNEL)==="Store Only" || df(SELLING_CHANNEL)==="Online Only")
    val retail_price_errors = valid_records.filter(valid_records(RETAIL_PRICE)===0 || valid_records(RETAIL_PRICE).isNull)
//    println("retail_price_errors.")
//    retail_price_errors.show(false)
    DatabaseConnectionService.DatabaseTableWriter(retail_price_errors.select(col("*"),lit("retail_price_error").as("error")),ERROR_TABLE)

    /** Product id should be only numeric and 8 digit. */
    val removed_null_retail_prices = valid_records.filter(valid_records(RETAIL_PRICE)=!=0 && valid_records(RETAIL_PRICE).isNotNull)
    val only_numeric_product_ids = removed_null_retail_prices.filter(removed_null_retail_prices(PRODUCT_ID).cast("Int").isNotNull)
    only_numeric_product_ids.createOrReplaceTempView("Dataset")
    val product_id_errors = spark.sql("select * from Dataset where length(product_id) > 8 or length(product_id) < 8")
//    println("product_id_errors.")
//    product_id_errors.show(false)
    DatabaseConnectionService.DatabaseTableWriter(product_id_errors.select(col("*"),lit("product_id_error").as("error")),ERROR_TABLE)

    /** Final dataset after removing the errors. */
    val final_df = spark.sql("select * from Dataset where length(product_id) = 8")
    /** Removing duplicates from final dataset using seq(product_id, location_id) as PRIMARY KEY */
    val distinct_final_df = final_df.dropDuplicates(DATA_STREAM_PRIMARY_KEYS)

    /** Dumping online only channels. */
//    println("online only.")
    DatabaseConnectionService.DatabaseTableWriter( distinct_final_df.filter(distinct_final_df("selling_channel")==="Online Only")
      .drop("selling_channel"),ONLINE_ONLY_TABLE)


    /** Dumping store only channels. */
//    println("store only.")
    DatabaseConnectionService.DatabaseTableWriter( distinct_final_df.filter(distinct_final_df("selling_channel")==="Store Only")
      .drop("selling_channel"),STORE_ONLY_TABLE)


    /** Dumping cross over channels. */
//    println("cross over.")
    DatabaseConnectionService.DatabaseTableWriter(distinct_final_df.filter(distinct_final_df("selling_channel")==="Cross Over")
          .drop("selling_channel"),CROSS_OVER_TABLE)

    /** Returning the final distinct dataset */
    distinct_final_df
  }
}
