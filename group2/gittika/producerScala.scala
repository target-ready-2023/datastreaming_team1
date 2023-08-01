package template.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{struct, to_json}

object producerScala {
  def main(args: Array[String]): Unit = {
    // Spark session initialization
    val spark = SparkSession.builder().master("local[*]").appName("TargetReady").getOrCreate()

    // Reading data from CSV into a DataFrame
    val df = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").load("C:\\Users\\DELL\\Dekstop\\datastreaming_team\\targetReadyDataSet.csv")

    // Specifying column names for the DataFrame
    val column_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(column_names: _*)
//    println(dfWithHeader.count())  1048576

    // Sending data to Kafka topic
//    dfWithHeader.toJSON.foreachPartition { partition =>
//      val props = new Properties()
//      props.put("bootstrap.servers", "localhost:9092")
//      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//
//      val producer = new KafkaProducer[String, String](props)
//      partition.foreach { record =>
//        val msg = new ProducerRecord[String, String]("datastream", record)
//        producer.send(msg)
//      }
//      producer.flush()
//      producer.close()
//    }
    dfWithHeader.select(
     (struct("product_id").cast("string")).alias("key"),
      to_json(struct("*")).alias("value")).write.format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("topic", "datastream").save()

    // Closing the Spark session
    spark.close()
  }
}
