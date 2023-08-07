
  import org.apache.spark.sql.{SparkSession, DataFrame}
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
  import scala.util.Random
  import org.apache.spark.sql.Row
  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
  import org.apache.kafka.common.serialization.StringSerializer

  object producer{

    def main(args:Array[String]): Unit ={

      val spark = SparkSession.builder
        .master("local[*]")
        .appName("producer")
        .getOrCreate()



      val df = spark.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .load("D:\\target2\\targetReadyDataSet\\targetReadyDataSet.csv")
      val colum_names = Seq("product_id","location_id","selling_channel","prod_description","retail_price","onhand_quantity","create_date", "promotion_eligibility")
      val dfWithHeader = df.toDF(colum_names:_*)
      dfWithHeader.show(20, false)


      val concatenatedColumns = concat_ws(";",dfWithHeader.columns.map(col): _*)
      val dfnew= dfWithHeader.withColumn("colNew",concatenatedColumns)

      val producerProps = new java.util.Properties()
      producerProps.put("bootstrap.servers", "localhost:9092")
      producerProps.put("key.serializer", classOf[StringSerializer].getName)
      producerProps.put("value.serializer", classOf[StringSerializer].getName)



      dfnew.foreachPartition { partition =>

       val partitionProducer = new KafkaProducer[String, String](producerProps)

        partition.foreach { row =>


         val record = new ProducerRecord[String, String]("my_topic", row.getAs[String]("colNew"))

         partitionProducer.send(record)
        }


        partitionProducer.close()
      }



    }


  }

