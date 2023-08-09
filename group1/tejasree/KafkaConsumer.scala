import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql._

import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConverters._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaConsumer")
      .master("local[*]") // Set your Spark master URL here
      .getOrCreate()

    val TOPIC = "datastreaming"
    val filename = "C:\\Users\\Dell\\IdeaProjects\\datastreaming_team1\\ConsumerData.csv"

    consume(filename,TOPIC)
  }

  def consume(filename: String, TOPIC: String): Unit = {

    val props = new java.util.Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    // Create Kafka consumer
    val consumer = new KafkaConsumer[String, String](props)
    // Subscribe to the Kafka topic
    consumer.subscribe(java.util.Collections.singletonList(TOPIC))

    val writer = new BufferedWriter(new FileWriter(filename))

    try{
    while (true) {

      val records = consumer.poll(java.time.Duration.ofMillis(100))
      for (record <- records.asScala) {
        //println(record.value())
        writer.write(record.value())
        writer.newLine()
      }
      writer.flush()
    }
    }catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      writer.close()
      consumer.close()
    }
  }
}

