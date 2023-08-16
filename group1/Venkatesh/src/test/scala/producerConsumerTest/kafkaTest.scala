package producerConsumerTest

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import producerConsumer.{Consumer, Producer}

class newKafkaTest extends AnyFlatSpec{

  "producerConsumer" should "have same dataframe" in{
    val spark = SparkSession.builder().appName("test-kafka").master("local[*]").getOrCreate()

    import spark.implicits._
    Producer.produce(spark,"testset.csv","kafka","localhost:9092","v")
    val outputDF = Consumer.consume(spark,"kafka","localhost:9092","v")

    outputDF.printSchema()
    outputDF.show()

    val expectedDF = Seq(
      ("10266320","1194","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1777","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","0"),
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1")
    ).toDF("product_id"
      , "location_id"
      , "selling_channel"
      , "prod_description"
      , "retail_price"
      , "onhand_quantity"
      , "create_date"
      , "promotion_eligibility")

    expectedDF.show()

    val result = expectedDF.except(outputDF)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }
}