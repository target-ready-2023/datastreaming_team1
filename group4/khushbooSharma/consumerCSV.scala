import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.util
import java.util.Properties
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object consumerCSV {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("newCSV1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    /** TO Desable the SUCCESS & CRC files*/

    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

/*
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    val consumer=new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Collections.singletonList("datastream"))


    import java.io.BufferedWriter
    import java.io.FileWriter
//    val buffWriter = new BufferedWriter(new FileWriter("D:/TargetCoorporationPhaseSecond/data_from_kafka/temp.txt"))
    val buffWriter = new BufferedWriter(new FileWriter("D:/TargetCoorporationPhaseSecond/data_from_kafka/output.json"))
//    val buffWriter = new BufferedWriter(new FileWriter("D:/TargetCoorporationPhaseSecond/data_from_kafka/output_csv.csv"))

    val running=true

    while(running){

      val records=consumer.poll(100)

      for(record<-records.asScala){
        println(record.value())
        buffWriter.write(record.value() + System.lineSeparator())
        buffWriter.flush()
      }

    }

*/


    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "datastream")
      .option("includeHeaders", "true")
      .option("maxOffsetsPerTrigger", "1100100")
      .option("failOnDataLoss", "false")
      .load()

    val kafkaData = kafkaDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val query = kafkaData
      .writeStream
      .format("json")
      .option("path","D:/TargetCoorporationPhaseSecond/data_from_kafka/final/kafka_consumed_json_FINAL")
      .option("checkpointLocation", "D:/TargetCoorporationPhaseSecond/data_from_kafka/final/kafka_checkpoints/latestJ1")
      .start()

    val q2=kafkaData.writeStream
      .format("console")
      .start()


    query.awaitTermination()
    q2.awaitTermination()
    spark.stop()
  }
}