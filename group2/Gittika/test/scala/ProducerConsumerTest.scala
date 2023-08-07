import org.scalatest.flatspec.AnyFlatSpec
import kafka.ConsumeDataframe.consumerDataFrame
import kafka.ProducerJSON.producerJSON

class ProducerConsumerTest extends AnyFlatSpec {

  val spark = utils.ApplicationUtils.createSparkSession()

  import spark.implicits._

  "producer" should "send data to the topic and can be seen in consumer" in {


    producerJSON(spark, "C:\\Users\\DELL\\Dekstop\\datastreaming_team\\data\\targetReadyTestDataset.csv", "com.databricks.spark.csv", "kafka",
      "datastreamtest")


    val expectedDF = consumerDataFrame(spark, "kafka", "datastreamtest")

    val outputDF = Seq(
      ("10266320", "1194", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266321", "1479", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266320", "1770", "Store Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "0"),
      ("10266320", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")


    val result = expectedDF.except(outputDF)
    val ans = result.count()
    val count = 0;
    assertResult(count)(ans)

  }


}
