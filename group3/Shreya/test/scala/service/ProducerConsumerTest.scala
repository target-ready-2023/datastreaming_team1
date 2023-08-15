package service

import constants.ApplicationConstants.BOOTSTRAP_SERVER
import helper.Helper.{TEST_INPUT_FILE_PATH, TEST_TOPIC_NAME, createSparkSession}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import service.ConsumerJson.ConsumerJSON
import service.ProducerJson.producerJSON

class ProducerConsumerTest extends AnyFlatSpec{

    implicit val spark: SparkSession = createSparkSession()
    import spark.implicits._

    "producer" should "send data to the topic and can be seen in consumer" in {

      producerJSON(TEST_INPUT_FILE_PATH, BOOTSTRAP_SERVER, TEST_TOPIC_NAME)

      val outputDf = ConsumerJSON(TEST_TOPIC_NAME, BOOTSTRAP_SERVER)

      val expectedDf = Seq(
        ("10266320","1194","Cross","PRSMN LGF DX LET'S GO FISHIN DELUXE","0.0","5.798878424473668","2023-07-16-03","1"),
        ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","0","5.798878424473668","2023-07-16-03","1"),
        ("13016031","1046","Store Only","BETTY CROCKR XT SM SPICE CK MX15.25","401.3608093","6.88596121735671","2023-07-16-03","0"),
        ("13016031","1112","Store Only","BETTY CROCKR XT SM SPICE CK MX15.25","401.3608093","6.88596121735671","2023-07-16-03","0")
      ).toDF("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")

      val result = expectedDf.except(outputDf)
      val ans = result.count()
      val count = 0
      assertResult(count)(ans)

    }

}
