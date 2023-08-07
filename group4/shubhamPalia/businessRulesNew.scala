import DBConnect.databaseWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, length}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object businessRulesNew{
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder.master("local[*]").appName("businessRulesNew").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("error")
    val original_df = spark.read.option("header", "true").format("json")
      .load("D:\\phase2\\data\\out2\\Records\\part-00000-1398f5ed-bf74-440c-9181-908f8c39211d-c000.json",
        "D:\\phase2\\data\\out2\\Records\\part-00000-28c13ea9-df43-43f6-89e8-397636438f8a-c000.json",
        "D:\\phase2\\data\\out2\\Records\\part-00000-612176c7-161e-4ba1-ba58-bf86ada74498-c000.json",
        "D:\\phase2\\data\\out2\\Records\\part-00000-a78e6ae7-9f3e-4bb8-9c19-1c9515c4bc16-c000.json",
        "D:\\phase2\\data\\out2\\Records\\part-00000-e025dca7-8b2a-4901-9bd6-80834eddeda7-c000.json")

    val schema = new StructType()
      .add("product_id", StringType, true)
      .add("location_id", StringType, true)
      .add("selling_channel", StringType, true)
      .add("prod_description", StringType, true)
      .add("retail_price", StringType, true)
      .add("onhand_quantity", StringType, true)
      .add("create_date", StringType, true)
      .add("promotion_eligibility", StringType, true)

    var df = original_df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
      .select(col("parsed_value.*"))

    // Deleting records with price null or 0 into an error table
    val price_error = df.filter($"retail_price".isNull || $"retail_price" === 0.0)
    databaseWriter(price_error, "error_retail_price_table")
    // updating table
    df = df.except(price_error)

    // Deleting selling channels except ONLINE, STORE, CROSSOVER into an error table
    val selling_channel_error = df.filter($"selling_channel" =!= "Cross Over" && $"selling_channel" =!= "Store Only" && $"selling_channel" =!= "Online Only")
    databaseWriter(selling_channel_error, "selling_channel_error_table")
    // updating table
    df = df.except(selling_channel_error)


    // Deleting records with price null or 0 into an error table
    val null_0_df = df.filter($"retail_price".isNull || $"retail_price" === 0.0)
    databaseWriter(null_0_df, "error_retail_price_table")
    // updating table
    df = df.except(null_0_df)

    //Checking if the product_id is numeric and 8 digit
    val num_prod_id = df.filter(col("product_id").cast("int").isNotNull)
    val num_prod_id_error = df.except(num_prod_id)
    val legal_prod_id = num_prod_id.filter(length(col("product_id")) === 8)
    val errorProductId = num_prod_id.except(legal_prod_id)

    val finalDF = legal_prod_id
    finalDF.show(20)
    val combinedErrorProductIdDf = num_prod_id_error.union(errorProductId)
    databaseWriter(combinedErrorProductIdDf, "error_product_id_table")
    val deduplicateDf = legal_prod_id.dropDuplicates("product_id", "location_id")



    //Loading valid records into the 3 datasets and saving them as CSV
    val onlineProductsDf = deduplicateDf.filter($"selling_channel" === "Online Only")
    databaseWriter(onlineProductsDf, "online_products")

    val crossoverProductsDf = deduplicateDf.filter($"selling_channel" === "Cross Over")
    databaseWriter(crossoverProductsDf, "crossover_products")

    val storeProductsDf = deduplicateDf.filter($"selling_channel" === "Store Only")
    databaseWriter(storeProductsDf, "store_products")

  }
}
