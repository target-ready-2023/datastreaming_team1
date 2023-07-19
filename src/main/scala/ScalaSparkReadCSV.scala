import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Random
import org.apache.spark.sql.Row

object ScalaSparkReadCSV {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("testApp")
      .getOrCreate()

    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("/Users/z083276/Downloads/targetReadyDataSet.csv")
    val colum_names = Seq("product_id","location_id","selling_channel","prod_description","retail_price","onhand_quantity","create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(colum_names:_*)
    dfWithHeader.show(20, false)
    println("Total No. Of Rows : " + dfWithHeader.count());
    // only 5 records to be printed in kafka topic

  }
}