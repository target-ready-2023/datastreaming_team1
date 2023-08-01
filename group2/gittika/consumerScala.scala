package template.spark

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.io.{BufferedWriter, FileWriter}
import java.util.Properties
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

object consumerScala {
def main(args:Array[String]): Unit = {

  val TOPIC = "datastream"
  val fileName = "C:\\Users\\DELL\\Dekstop\\datastreaming_team\\DataFromKafka.csv"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(java.util.Collections.singletonList(TOPIC))

  val writer = new BufferedWriter(new FileWriter(fileName))

  try {
    while (true) {
//      val records = consumer.poll(100)
//      for (record <- records.asScala) {
////        println(record.value())
//        writer.write(record.value())
//        writer.newLine()
        val records = consumer.poll(java.time.Duration.ofMillis(100)).asScala
        for (record <- records) {
          val key = record.key()
          val value = record.value()
          // Write the received data to the CSV file
          writer.write(value)
          writer.newLine()
      }
      writer.flush()
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    writer.close()
    consumer.close()
  }
}

}




