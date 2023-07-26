import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.util.Random
import org.apache.spark.sql.Row

object producer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("producer app")
      .getOrCreate()

    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("targetReadyDataSet.csv")
    val colum_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(colum_names: _*)
    dfWithHeader.show(20, false)
    println("Total No. Of Rows : " + dfWithHeader.count());

    println(dfWithHeader.rdd.getNumPartitions)
    val props: Properties = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("ack", "all")

    dfWithHeader.foreachPartition { partition =>
      val producer = new KafkaProducer[String, String](props) // Instantiate KafkaProducer within the task
      partition.take(1).foreach { row =>
        val record = new ProducerRecord[String, String]("test",row.mkString(","))
        producer.send(record)
        println(record)
      }
    }
  }
}