package temp

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object bussinessLogics {


  def dumpInDB(error_df: DataFrame, tableName: String): Unit = {


    error_df.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/datastream")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "root")
      .mode("append")
      .save()

  }

  def sellingChannelHandler(df: DataFrame, tableName: String): (DataFrame, DataFrame) = {
    /** DUMP ERROR SELLING CHANNELS INTO DATABASE */
    val df1 = df.filter((df("selling_channel") =!= "Cross Over" && df("selling_channel") =!= "Store Only" && df("selling_channel") =!= "Online Only"))

    val error_df_selling_channel = df.withColumn("error_selling_channel", df1.col("selling_channel"))

    /** REMOVE INVALID CHANNELS FROM THE DATAFRAME */
    val valid_channel_df = df.filter((df("selling_channel") === "Cross Over" || df("selling_channel") === "Store Only" || df("selling_channel") === "Online Only"))

    (valid_channel_df, error_df_selling_channel)
  }

  def retailPriceHandler(valid_channel_df: DataFrame, tableName: String): (DataFrame, DataFrame) = {
    /** DUMP NULL OR 0 RECORDS OF retail_price COLUMN INTO DATABASE */
    val df1 = valid_channel_df.filter(valid_channel_df("retail_price") === 0 || valid_channel_df("retail_price").isNull)

    val error_df_price = valid_channel_df.withColumn("error_retail_price", df1.col("retail_price"))
    /*
    error_df_price.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastream")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "root")
      .mode("overwrite")
      .save()
*/


    /** REMOVING Nulls/0 FROM retail_price COLUMN */
    val removed_null_price_df = valid_channel_df.filter(valid_channel_df("retail_price") =!= 0 && valid_channel_df("retail_price").isNotNull)

    (removed_null_price_df, error_df_price)
  }

  def productIdHandler(spark: SparkSession, removed_null_price_df: DataFrame, tableName: String): (DataFrame, DataFrame) = {
    /** DUMP product_id which is not numeric and 8 digit into DATABASE */

    val only_numeric_df = removed_null_price_df.filter(removed_null_price_df.col("product_id").cast("int").isNotNull)

    only_numeric_df.createOrReplaceTempView("TAB")

    val df1 = spark.sql("select * " +
      "from TAB where length(product_id) > 8")


    val error_df_product_id = removed_null_price_df.withColumn("error_product_id", df1.col("product_id"))


    //dump error_df_product_id into error_table (mysql)

    println("DUMP product_id which is not numeric and 8 digit into DATABASE")
    error_df_product_id.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/datastream")
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "root")
      .mode("overwrite")
      .save()

    /** GET RECORDS WHICH ARE VALID IN TERMS OF PRODUCT ID */
    val final_df = spark.sql("select * " +
      "from TAB where length(product_id) = 8")

    (final_df, error_df_product_id)
  }

  def removeDuplicates(final_df: DataFrame): DataFrame = {
    val primaryKeyCols: Seq[String] = Seq("product_id", "location_id")

    val dfRemoveDuplicates = final_df.withColumn("rn", row_number().over(Window.partitionBy(primaryKeyCols.map(col): _*).orderBy(desc("create_date"))))
      .filter(col("rn") === 1).drop("rn")
    dfRemoveDuplicates
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bussinessLogicsPackage").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    producerConsumer.Producer.prodcue(spark, "D:/TargetCoorporationPhaseSecond/Data/targetReadyDataSet.csv", "kafka", "localhost:9092", "datastreamTest6")

    val df = producerConsumer.Consumer.consume(spark, "kafka", "localhost:9092", "datastreamTest6")

    df.show(false)

    /** HANDLING  SELLING CHANNELS */
    val (valid_channel_df, error_df_selling_channel) = sellingChannelHandler(df, "error_data")
    //dump error_df_selling_channel into mysql
    println("DUMP ERROR SELLING CHANNELS INTO DATABASE")
    dumpInDB(error_df_selling_channel, "error_data")

    /** Handling retailPrice */
    val (removed_null_price_df, error_df_price) = retailPriceHandler(valid_channel_df, "error_data")
    removed_null_price_df.show(false)
    //dump error_df_price into error_table (mysql)
    println("DUMP NULL OR 0 RECORDS OF retail_price COLUMN INTO DATABASE")
    dumpInDB(error_df_price, "error_data")
    /*
    /**Handling ProductID*/
    val (removed_product_id,error_df_product_id) = productIdHandler(spark, removed_null_price_df,"error_data")


    val final_df = removeDuplicates(removed_product_id)

    final_df.show(10,false)

*/
    spark.stop()
  }
}