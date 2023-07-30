import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import template.spark.Main.spark.sqlContext

object bussinessLogics {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bussinessLogics").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val df1 =sqlContext.read
      .json("D:/TargetCoorporationPhaseSecond/data_from_kafka/FINAL/kafka_consumed_json_FINAL/part-00000-18b0f5b4-403f-4c63-a937-fc12d1b0206a-c000.json"
        ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/FINAL/kafka_consumed_json_FINAL/part-00000-686718cd-eea0-469e-8e15-1c11bf1e00f6-c000.json"
        ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/FINAL/kafka_consumed_json_FINAL/part-00000-a716b16c-5cd0-4e9b-b197-cb5c16d00289-c000.json"
        ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/FINAL/kafka_consumed_json_FINAL/part-00000-b93f4392-391d-4382-a8df-4228af911966-c000.json"

      )


    val df =df1.select(get_json_object(col("value"), "$.product_id").alias("product_id")
      ,get_json_object(col("value"), "$.location_id").alias("location_id")
      ,get_json_object(col("value"), "$.selling_channel").alias("selling_channel")
      ,get_json_object(col("value"), "$.prod_description").alias("prod_description")
      ,get_json_object(col("value"), "$.retail_price").alias("retail_price")
      ,get_json_object(col("value"), "$.onhand_quantity").alias("onhand_quantity")
      ,get_json_object(col("value"), "$.create_date").alias("create_date")
      ,get_json_object(col("value"), "$.promotion_eligibility").alias("promotion_eligibility")

    )

    df.show(10,false)

    /** DUMP ERROR SELLING CHANNELS INTO DATABASE*/
    val error_df_selling_channel = df.filter((df("selling_channel")=!="Cross Over" && df("selling_channel")=!="Store Only"  && df("selling_channel")=!="Online Only"))

    //dump error_df_selling_channel into mysql

    error_df_selling_channel.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastream")
      .option("dbtable", "error_selling_channel")
      .option("user", "root")
      .option("password", "root")
      .mode("overwrite")
      .save()


    /** REMOVE INVALID CHANNELS FROM THE DATAFRAME*/
    val valid_channel_df = df.filter((df("selling_channel")==="Cross Over" || df("selling_channel")==="Store Only"  || df("selling_channel")==="Online Only"))

    /** DUMP NULL OR 0 RECORDS OF retail_price COLUMN INTO DATABASE*/
    val error_df_price = valid_channel_df.filter(valid_channel_df("retail_price")===0 || valid_channel_df("retail_price").isNull)

    //dump error_df_price into error_table (mysql)

    error_df_price.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastream")
      .option("dbtable", "error_retail_price")
      .option("user", "root")
      .option("password", "root")
      .mode("overwrite")
      .save()

    /** REMOVING Nulls/0 FROM retail_price COLUMN */
    val removed_null_price_df = valid_channel_df.filter(valid_channel_df("retail_price")=!=0 && valid_channel_df("retail_price").isNotNull)

    /** DUMP product_id which is not numeric and 8 digit into DATABASE*/

    val only_numeric_df = removed_null_price_df.filter(removed_null_price_df.col("product_id").cast("int").isNotNull)

    only_numeric_df.createOrReplaceTempView("TAB")

    val error_df_product_id=spark.sql("select * " +
      "from TAB where length(product_id) > 8")

    //dump error_df_product_id into error_table (mysql)

    error_df_product_id.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastream")
      .option("dbtable", "error_product_id")
      .option("user", "root")
      .option("password", "root")
      .mode("overwrite")
      .save()

    /** GET RECORDS WHICH ARE VALID IN TERMS OF PRODUCT ID */
    val final_df = spark.sql("select * " +
      "from TAB where length(product_id) = 8")

    final_df.show(10,false)


    spark.stop()
  }
}
