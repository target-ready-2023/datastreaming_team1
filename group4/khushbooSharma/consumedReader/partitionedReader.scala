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
/*
    val df= spark.read.
      parquet("D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-3fc783f0-5874-4f06-9069-4c24e6f7dc0c-c000.snappy.parquet"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-8d18104b-348a-4bf0-9061-03a11a38f324-c000.snappy.parquet"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-36dea632-bc9a-4a5a-97d2-efe8d1022a08-c000.snappy.parquet"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-680cfba2-0a18-436f-8fda-18e71c7e86c0-c000.snappy.parquet"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-582319cc-4a5c-4c53-94b1-9f3537644ba3-c000.snappy.parquet"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-7123224b-9f33-4133-9c86-755998a20139-c000.snappy.parquet"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-74035067-9b73-4801-995f-bbfe437e2469-c000.snappy.parquet"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-efe32a7d-bf4f-4502-a0d9-61dfb899f8fb-c000.snappy.parquet"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/kafka_consumed_parquet2/part-00000-f04c8115-bd95-4fb7-8114-b85f80013c25-c000.snappy.parquet")

//    df.show(5,false)
*/

    df.show(2)


    spark.stop()
  }
}