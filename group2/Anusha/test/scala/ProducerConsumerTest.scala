import org.scalatest.flatspec.AnyFlatSpec
import helper.Helper.{TEST_COLUMN_NAMES, TEST_FILE_FORMAT, TEST_INPUT_FILE_PATH, TEST_KAFKA_BOOTSTRAP_SERVER, TEST_TABLE_SCHEMA, TEST_TOPIC_NAME, createSparkSession}
import org.apache.spark.sql.SparkSession
import service.ProducerService.producer
import service.ConsumerService.consumer

class ProducerConsumerTest extends AnyFlatSpec {

  implicit val spark: SparkSession = createSparkSession()

  import spark.implicits._

  "producer" should "send data to the topic and can be seen in consumer" in {

    producer(TEST_INPUT_FILE_PATH, TEST_TOPIC_NAME, TEST_KAFKA_BOOTSTRAP_SERVER, TEST_COLUMN_NAMES)

    val expectedDF = consumer(TEST_INPUT_FILE_PATH, TEST_KAFKA_BOOTSTRAP_SERVER, TEST_FILE_FORMAT, TEST_TABLE_SCHEMA )

    val outputDF = Seq(
      ("10266320","1194","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1777","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","0"),
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1")
    ).toDF("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")


    val result = expectedDF.except(outputDF)
    val ans = result.count()
    val count = 0;
    assertResult(count)(ans)

  }


}