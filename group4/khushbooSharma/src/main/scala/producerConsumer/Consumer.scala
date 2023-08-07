package producerConsumer

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

object Consumer {

  def consume(spark:SparkSession,format:String,kafkaServer:String,topic:String): DataFrame ={

    val df =   spark.read.format(format)
      .option("kafka.bootstrap.servers",kafkaServer)
      .option("subscribe", topic)
      .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .load()


    val decodedData = df.selectExpr("CAST (value AS STRING)")


    val schema = new StructType()
      .add("product_id",StringType)
      .add("location_id",StringType)
      .add("selling_channel",StringType)
      .add("prod_description",StringType)
      .add("retail_price",StringType)
      .add("onhand_quantity",StringType)
      .add("create_date",StringType)
      .add("promotion_eligibility",StringType)

    val kafkaDF = decodedData.select(from_json(col("value"),schema).alias("data")).select("data.*")

    kafkaDF

  }

//  def main(args: Array[String]) {
//    val spark = SparkSession.builder.master("local[*]").appName("newConsumerCSV").getOrCreate()
//    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
//    val sc = spark.sparkContext
//    sc.setLogLevel("ERROR")
//
///*
//    val kafkaDF = spark.read.format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "datastream")
//      .option("includeHeaders", "true")
//      .option("maxOffsetsPerTrigger", "1100100")
//      .option("failOnDataLoss", "false").load()
//
//    val kafkaData = kafkaDF
//      .selectExpr("CAST(value AS STRING) AS value")
//    import org.apache.spark.sql.functions._
//    import org.apache.spark.sql.types._
//
//    val schema = new StructType()
//      .add("product_id", StringType)
//      .add("location_id", StringType)
//      .add("selling_channel", StringType)
//      .add("prod_description", StringType)
//      .add("retail_price", DoubleType)
//      .add("onhand_quantity", IntegerType)
//      .add("create_date", StringType)
//      .add("promotion_eligibility", StringType)
//
//    val df = kafkaData
//      .select(from_json($"value", schema).alias("data"))
//      .select("data.*")
//
//    df.show()
//*/
//
//
//    val kafkaDF = consume(spark,"kafka", "localhost:9092","datastream")
//
//    kafkaDF.show(false)
//
//    spark.stop()
//  }
}