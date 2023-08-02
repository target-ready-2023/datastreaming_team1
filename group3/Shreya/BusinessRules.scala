import DBConnect.databaseWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, length}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object BusinessRules{
  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder.master("local[*]").appName("BusinessRules").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("error")

    val dfRaw = spark.read.option("header", "true").format("json")
      .load("C:\\tmp\\data\\out\\jsonDataset\\part-00000-08bc9e3e-28f3-4233-ac13-9f73ed975557-c000.json",
        "C:\\tmp\\data\\out\\jsonDataset\\part-00000-a5003099-6969-4388-968d-63b460ae6c1b-c000.json",
        "C:\\tmp\\data\\out\\jsonDataset\\part-00000-c4097e16-dd8b-46fd-8c31-2b297a9240f3-c000.json",
        "C:\\tmp\\data\\out\\jsonDataset\\part-00000-fff1ab92-1235-4a73-b9d4-b8a48feca85b-c000.json")

    val schema = new StructType()
      .add("product_id", StringType, true)
      .add("location_id", StringType, true)
      .add("selling_channel", StringType, true)
      .add("prod_description", StringType, true)
      .add("retail_price", StringType, true)
      .add("onhand_quantity", StringType, true)
      .add("create_date", StringType, true)
      .add("promotion_eligibility", StringType, true)

    val df = dfRaw.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
      .select(col("parsed_value.*"))

    //Move records with different selling channel other than ONLINE, STORE, CROSSOVER into an error table
    val errorSellingChannel = df.filter($"selling_channel" =!= "Cross Over" && $"selling_channel" =!= "Store Only" && $"selling_channel" =!= "Online Only")
    databaseWriter(errorSellingChannel, "error_selling_channel")

    val validSellingChannel = df.except(errorSellingChannel)

    //Move records with price null or 0 into an error table
    val priceNullOrZero = validSellingChannel.filter($"retail_price".isNull || $"retail_price" === 0.0)
    databaseWriter(priceNullOrZero,"error_retail_price")

    val validPrice = validSellingChannel.except(priceNullOrZero)

    //Checking if product_id is numeric and 8 digit
    val numericProductId = validPrice.filter(col("product_id").cast("int").isNotNull)
    val nonNumericProductId = validPrice.except(numericProductId)

    val validProductId =  numericProductId.filter(length(col("product_id")) === 8)
    val errorProductId = numericProductId.except(validProductId)

    val combinedErrorProductIdDf = nonNumericProductId.union(errorProductId)
    databaseWriter(combinedErrorProductIdDf,"error_product_id" )

    //Deduplication based on product_id and location_id
    val deduplicateDf = validProductId.dropDuplicates("product_id", "location_id")

    //Loading valid records into the 3 datasets
    val onlineProductsDf = deduplicateDf.filter($"selling_channel" ==="Online Only")
    databaseWriter(onlineProductsDf, "online_products")

    val crossoverProductsDf = deduplicateDf.filter($"selling_channel" ==="Cross Over")
    databaseWriter(crossoverProductsDf, "crossover_products")

    val storeProductsDf = deduplicateDf.filter($"selling_channel" ==="Store Only")
    databaseWriter(storeProductsDf, "store_products")

  }
}