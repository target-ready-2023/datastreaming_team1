import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConverters._

object Consumer{
  def main(args:Array[String]): Unit = {

    val TOPIC = "datastreaming"
    val fileName = "C:\\Users\\Shreya\\Target_Ready\\data streaming project\\datastreaming\\DataSetFromKafka1.csv"

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList(TOPIC))

    val writer = new BufferedWriter(new FileWriter(fileName))

    try {
      while (true) {
        val records = consumer.poll(100)
        for (record <- records.asScala) {
          //println(record.value())
          writer.write(record.value())
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