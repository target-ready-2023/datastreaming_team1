import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql._

import java.util.Properties

object producerCSV {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("newCSV").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("D:/TargetCoorporationPhaseSecond/Data/targetReadyDataSet.csv")
    val colum_names = Seq("product_id","location_id","selling_channel","prod_description","retail_price","onhand_quantity","create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(colum_names:_*)


    dfWithHeader.toJSON.foreachPartition { partition =>
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)

      partition.foreach { record =>
        //        println(record)
        val message = new ProducerRecord[String, String]("datastream", record)
        producer.send(message)
      }

      producer.flush()
      producer.close()
    }
    spark.stop()
  }
}