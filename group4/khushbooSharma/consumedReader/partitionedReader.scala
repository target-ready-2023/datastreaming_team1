package consumedReader

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import template.spark.Main.spark.sqlContext

object partitionedReader {
  def reader(path1:String,path2 :String,path3:String,path4:String,path5:String): DataFrame ={
    val df1 =sqlContext.read
      .json(path1
        ,path2
        ,path3
        ,path4
        ,path5)

    //    df1.show(5,false)

    val df_new =df1.select(get_json_object(col("value"), "$.product_id").alias("product_id")
      ,get_json_object(col("value"), "$.location_id").alias("location_id")
      ,get_json_object(col("value"), "$.selling_channel").alias("selling_channel")
      ,get_json_object(col("value"), "$.prod_description").alias("prod_description")
      ,get_json_object(col("value"), "$.retail_price").alias("retail_price")
      ,get_json_object(col("value"), "$.onhand_quantity").alias("onhand_quantity")
      ,get_json_object(col("value"), "$.create_date").alias("create_date")
      ,get_json_object(col("value"), "$.promotion_eligibility").alias("promotion_eligibility")

    )

    df_new
  }
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("partitionedReader").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val df = reader("D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_json_FINAL/part-00000-9dca2e40-de61-4712-a391-a456864898c3-c000.json"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_json_FINAL/part-00000-99c94bb9-7f2d-45d8-a816-6bbecaacb53b-c000.json"
    ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_json_FINAL/part-00000-0206f06b-a70e-46b5-a832-bde06136b5e4-c000.json"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_json_FINAL/part-00000-b77ebfff-6e0a-41e2-a0f3-e951dedc44f5-c000.json"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_json_FINAL/part-00000-d57cd56f-377c-4cc3-a6ab-1e8d861e3751-c000.json"
    )


    df.show(2)


    spark.stop()
  }
}