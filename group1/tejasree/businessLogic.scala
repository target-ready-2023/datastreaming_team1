import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object bussinessLogic{
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bussinessLogics").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val dfRaw = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("C:/Users/Dell/IdeaProjects/datastreaming_team1/ConsumerData.csv")

    val colum_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val df = dfRaw.toDF(colum_names: _*)

    val errorChannel = df.filter((df("selling_channel")=!="Cross Over" && df("selling_channel")=!="Store Only"  && df("selling_channel")=!="Online Only"))

    errorChannel.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", "error_selling_channel")
      .option("user", "root")
      .option("password", "thejasree")
      .mode("overwrite")
      .save()

    val validChannels= df.filter((df("selling_channel")==="Cross Over" || df("selling_channel")==="Store Only"  || df("selling_channel")==="Online Only"))

    val errorPrice = validChannels.filter(validChannels("retail_price")===0 || validChannels("retail_price").isNull)

    errorPrice.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", "error_retail_price")
      .option("user", "root")
      .option("password", "thejasree")
      .mode("overwrite")
      .save()

    val removeNullPrice= validChannels.filter(validChannels("retail_price")=!=0 && validChannels("retail_price").isNotNull)

    val validProductId = removeNullPrice.filter(removeNullPrice.col("product_id").cast("int").isNotNull)

    val deduplicatedDF = validProductId.dropDuplicates(Seq("product_id", "location_id"))

    val onlineChannel = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Online Only")

    onlineChannel.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", "online_selling_channel")
      .option("user", "root")
      .option("password", "thejasree")
      .mode("overwrite")
      .save()

    val crossoverChannel = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Cross Over")

    crossoverChannel.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", "crossover_selling_channel")
      .option("user", "root")
      .option("password", "thejasree")
      .mode("overwrite")
      .save()


    val storeChannel = deduplicatedDF.filter(deduplicatedDF("selling_channel")==="Store Only")

    storeChannel.write
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/datastreaming")
      .option("dbtable", "store_selling_channel")
      .option("user", "root")
      .option("password", "thejasree")
      .mode("overwrite")
      .save()


  }
}