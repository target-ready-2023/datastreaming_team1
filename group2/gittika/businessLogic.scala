package template.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._

object businessLogic {
  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().master("local[*]").appName("TargetReady1").getOrCreate()


    val df = sparkSession.read.option("header", "true").format("json").load("C:\\Users\\DELL\\Dekstop\\datastreaming_team\\json_dataset_consumed\\*.json")

    //the above dataframe have 2 columns key and value and the entire table is present in value column
    val data = df.select("value").collect().toSeq

    // Define the schema for the JSON data
    val schema = new StructType()
      .add("product_id", StringType)
      .add("location_id", StringType)
      .add("selling_channel", StringType)
      .add("prod_description", StringType)
      .add("retail_price", StringType)
      .add("onhand_quantity", StringType)
      .add("create_date", StringType)
      .add("promotion_eligibility", StringType)

    // Parse the JSON data into individual columns
    val parsedDf = df.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    println(parsedDf.count())
    //    1100000
    //    to check primary key
    println(parsedDf.select("product_id", "location_id").distinct().count())
    //1100000
    println(parsedDf.filter(col("product_id").isNull).count())


    val columnsDataType = Map(
      "product_id" -> IntegerType,
      "location_id" -> IntegerType,
      "selling_channel" -> StringType,
      "prod_description" -> StringType,
      "retail_price" -> FloatType,
      "onhand_quantity" -> FloatType,
      "create_date" -> StringType,
      "promotion_eligibility" -> IntegerType
    )

    def redefineColumnTypes(df: DataFrame, columnDefs: Map[String, DataType]): DataFrame = {
      columnDefs.foldLeft(df) {
        case (tempDF, (columnName, newDataType)) =>
          tempDF.withColumn(columnName, col(columnName).cast(newDataType))
      }
    }

    val updatedDF = redefineColumnTypes(parsedDf, columnsDataType)

    def errorTableWriter(df: DataFrame): Unit = {
      val DBURL = "jdbc:mysql://localhost:3306/target_ready"
      try {
        df.write.format("jdbc")
          .option("url", DBURL)
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", "error_table")
          .option("user", "root")
          .option("password", "gittika")
          .mode("overwrite")
          .save()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    // 1st way
    // val sellingDF = updatedDF.filter(col("selling_channel")==="Cross Over"||col("selling_channel")==="Store Only"||
    // col("selling_channel")==="Online Only")

    //val errorDf= updatedDF.filter(col("selling_channel")!="Cross Over" && col("selling_channel")!="Store Only" &&
    //  col("selling_channel")!="Online Only")

    //2nd way
    val validSellingChannels = Seq("Online Only", "Store Only", "Cross Over")


    val sellingDF = parsedDf.filter(col("selling_channel").isin(validSellingChannels: _*))

    val errorDF = parsedDf.filter(!col("selling_channel").isin(validSellingChannels: _*))


    // writing invalid selling channel records to error table
    errorTableWriter(errorDF)

    //check if retail_price column consist of null or 0 values
    val nullCheckDf = sellingDF.filter(col("retail_price").isNotNull && col("retail_price").cast(IntegerType) != 0)
    val nullDF = sellingDF.filter(col("retail_price").isNull || col("retail_price").cast(IntegerType) === 0)
    nullDF.show()


    //    writing invalid records having null or 0 value of retail_price column
    errorTableWriter(nullDF)

 val validProduct_idDF = nullCheckDf.filter(s"CAST(${col("product_id")} AS DECIMAL(8,0)) IS NOT NULL")
    println( validProduct_idDF.count())




  }
}