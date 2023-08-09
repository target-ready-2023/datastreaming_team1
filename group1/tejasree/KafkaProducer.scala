import java.util.Properties
import org.apache.kafka.clients._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object KafkaProducer {

  def main(args: Array[String]): Unit = {


    val TOPIC = "datastreaming"
    val filename = "C:/Users/Dell/IdeaProjects/datastreaming_team1/targetReadyDataSet.csv/targetReadyDataSet.csv"
    produce(filename,TOPIC)

  }
    def produce(filename: String, TOPIC: String): Unit = {

      val props: Properties = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      val producer = new KafkaProducer[String, String](props)
      for (line <- Source.fromFile(filename).getLines()) {

        val record = new ProducerRecord[String, String](TOPIC, "key", line)
        producer.send(record)
      }
      producer.flush()
      producer.close()
    }

}
