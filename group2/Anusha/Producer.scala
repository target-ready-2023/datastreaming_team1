import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Producer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("testApp")
      .getOrCreate()

    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("C:\\Users\\acer\\Desktop\\TargetReady\\targetReadyDataSet.csv\\targetReadyDataSet.csv")
    val colum_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(colum_names: _*)
    dfWithHeader.show(20, false)

    val kafkaProps = new Properties()
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val sampleDf = dfWithHeader.limit(5)

    val topic = "targetReady-sample"

    sampleDf.foreachPartition { partition =>
      // creating a kafka producer inside a partition
      val producer = new KafkaProducer[String, String](kafkaProps)

      // Send messages for each row in the partition
      partition.foreach { row =>
        val value = row.mkString(",")
        val record = new ProducerRecord[String, String](topic, value)
        producer.send(record)
      }
      producer.close()
    }
  }
}
