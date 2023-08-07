import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}


object BusinessLogic {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Read ConsumerDF")
      .master("local[*]")  // Replace with your cluster URL if running on a cluster
      .getOrCreate()
/*


 */
    val df = spark.read.format("com.databricks.spark.csv")
           .option("delimiter", ",")
           .load("D:\\target2\\targetReadyDataSet\\fromConsumer\\ReadData")



    val ColumnNames = Seq("product_id", "location_id", "selling_channel","prod_description","retail_price","onhand_quantity","create_date","promotion_eligibility")


    val df1 = df.toDF(ColumnNames: _*)

    val allowedValues = Seq("Online Only", "Store Only", "Cross Over")
    val error_df = df1.filter(!col("selling_channel").isin(allowedValues: _*))
    val filtered_df = df1.filter(col("selling_channel").isin(allowedValues: _*))


   error_df.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/targetproject")
      .option("dbtable", "error_table")
      .option("user", "root")
      .option("password", "asdfg12345")
      .mode("append")
      .save()


    val error_price_df = filtered_df.filter(col("retail_price") === 0 || col("retail_price").isNull)
    val price_filtered_df = filtered_df.except(error_price_df)

    error_price_df.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/targetproject")
      .option("dbtable", "error_table")
      .option("user", "root")
      .option("password", "asdfg12345")
      .mode("append")
      .save()

    val product_id_filtered_df = price_filtered_df.filter(
        col("product_id").cast("bigint").isNotNull &&
        length(trim(col("product_id").cast("string"))) === 8
    )
    val product_id_error_df = price_filtered_df.except(product_id_filtered_df)

    product_id_error_df.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/targetproject")
      .option("dbtable", "error_table")
      .option("user", "root")
      .option("password", "asdfg12345")
      .mode("append")
      .save()

    val df_wo_duplicates = product_id_filtered_df.dropDuplicates(Seq("product_id", "location_id"))


    val online_sold_df = df_wo_duplicates.filter(col("selling_channel")==="Online Only")
    online_sold_df.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/targetproject")
      .option("dbtable", "online_table")
      .option("user", "root")
      .option("password", "asdfg12345")
      .mode("append")
      .save()


    val store_sold_df = df_wo_duplicates.filter(col("selling_channel")==="Store Only")
    store_sold_df.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/targetproject")
      .option("dbtable", "store_table")
      .option("user", "root")
      .option("password", "asdfg12345")
      .mode("append")
      .save()


    val crossover_sold_df = df_wo_duplicates.filter(col("selling_channel")==="Cross Over")
    crossover_sold_df.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/targetproject")
      .option("dbtable", "crossover_table")
      .option("user", "root")
      .option("password", "asdfg12345")
      .mode("append")
      .save()


    spark.stop()
  }
}
