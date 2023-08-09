import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object bussinessLogic{
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bussinessLogics").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val password = "thejasree"

    val dfRaw = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("C:/Users/Dell/IdeaProjects/datastreaming_team1/ConsumerData.csv")

    val colum_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val df = dfRaw.toDF(colum_names: _*)
    val validDF = implementLogic(df,"error_selling_channel","error_retail_price", "error_product_id",password)
    validDF.show(false)
  }

    def implementLogic(df:DataFrame,errchannel:String,errprice:String,errproductId:String,password:String): DataFrame ={
      val errorChannel = df.filter((df("selling_channel") =!= "Cross Over" && df("selling_channel") =!= "Store Only" && df("selling_channel") =!= "Online Only"))

      errorChannel.write
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://localhost:3306/datastreaming")
        .option("dbtable", errchannel)
        .option("user", "root")
        .option("password", password)
        .mode("overwrite")
        .save()


      val validChannels = df.filter((df("selling_channel") === "Cross Over" || df("selling_channel") === "Store Only" || df("selling_channel") === "Online Only"))

    val errorPrice = validChannels.filter(validChannels("retail_price")===0 || validChannels("retail_price").isNull)

    errorPrice.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", errprice)
      .option("user", "root")
      .option("password", password)
      .mode("overwrite")
      .save()


    val removeNullPrice= validChannels.filter(validChannels("retail_price")=!=0 && validChannels("retail_price").isNotNull)

    val productIdDf = removeNullPrice.filter(removeNullPrice.col("product_id").cast("int").isNotNull)

    val validProductId = productIdDf.filter(length(col("product_id")) === 8)
    val errorProductId = productIdDf.filter(length(col("product_id")) =!= 8)

    errorProductId.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", errproductId)
      .option("user", "root")
      .option("password", password)
      .mode("overwrite")
      .save()

    val deduplicatedDF = validProductId.dropDuplicates(Seq("product_id", "location_id"))

    val onlineChannel = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Online Only")

    onlineChannel.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", "online_selling_channel")
      .option("user", "root")
      .option("password",password)
      .mode("overwrite")
      .save()

    val crossoverChannel = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Cross Over")

    crossoverChannel.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", "crossover_selling_channel")
      .option("user", "root")
      .option("password", password)
      .mode("overwrite")
      .save()


    val storeChannel = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Store Only")

    storeChannel.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", "store_selling_channel")
      .option("user", "root")
      .option("password", password)
      .mode("overwrite")
      .save()

      deduplicatedDF

  }
}
