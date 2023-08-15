package service

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}

object JsonReader{
  def jsonReader(files:String*)(implicit spark: SparkSession): DataFrame ={

    val dfRaw = spark.read.option("header", "true").format("json")
      .load(files: _*)

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

    df
  }
}