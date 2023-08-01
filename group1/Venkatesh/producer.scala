import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.functions.{length, struct, to_json}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object NewProducer {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("producer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("targetReadyDataSet.csv")

    val colum_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(colum_names: _*).limit(5)
    dfWithHeader.show(10, false)
    println("Total number of rows: ", dfWithHeader.count())

    val prop: Properties = new Properties()
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("ack", "all")

    val producer = new KafkaProducer[String, String](prop)
    dfWithHeader.select((struct("product_id")).alias("key"), to_json(struct("*")).alias("value"))
      .take(5).foreach { row =>
      val record = new ProducerRecord[String, String]("stream", row(1).toString)
      producer.send(record)
      println(record)
    }
  }
}
