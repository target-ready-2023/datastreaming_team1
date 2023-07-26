import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Producer{
  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("testApp")
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("C:\\Users\\Shreya\\Target_Ready\\data streaming project\\datastreaming\\targetReadyDataSet.csv")
    val colum_names = Seq("product_id","location_id","selling_channel","prod_description","retail_price","onhand_quantity","create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(colum_names:_*)

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val topicName = "datastreaming"

    df.foreachPartition { partition =>
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      partition.foreach{ row=>
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, "key", row.mkString(","))
        producer.send(record)
        //println(record)
      }
      producer.close()
    }

  }

}