import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object bussinessLogics {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bussinessLogics").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val df = spark.read.json("D:/TargetCoorporationPhaseSecond/data_from_kafka/consumed_data_json.json")
    df.show(5)

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
    println("Valid channels DF")
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

    /** Product id should be only numeric and 8 digit*/

    val only_numeric_df = removed_null_price_df.filter(removed_null_price_df.col("product_id").cast("int").isNotNull)

    only_numeric_df.createOrReplaceTempView("TAB")
    spark.sql("select * " +
      "from TAB where length(product_id) > 8").show()
    spark.stop()
  }
}
