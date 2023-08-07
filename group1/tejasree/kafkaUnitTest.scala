import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


class newKafkaTest extends AnyFlatSpec{

  "producerConsumer" should "have same dataframe" in{
    val spark = SparkSession.builder().appName("test-kafka").master("local[*]").getOrCreate()

    import spark.implicits._
    KafkaProducer.produce("C:\\Users\\Dell\\IdeaProjects\\datastreaming_team1\\testset.csv","Test2")
    KafkaConsumer.consume("C:\\Users\\Dell\\IdeaProjects\\datastreaming_team1\\consumedtest.csv","Test2")

    val dfRaw = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("C:/Users/Dell/IdeaProjects/datastreaming_team1/consumedtest.csv")

    val colum_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val outputDF = dfRaw.toDF(colum_names: _*)
    outputDF.printSchema()
    outputDF.show()

    val expectedDF = Seq(
      ("10266320","1194","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424","2023-07-16-03","1"),
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424","2023-07-16-03","1"),
      ("10266320","1777","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424","2023-07-16-03","0"),
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424","2023-07-16-03","1"),
      ("10266320","966","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424","2023-07-16-03","1"),
      ("10977985","1037","Cross Over","ALWAYS 120 THIN DAILY LINER","239.6081238","11.24557969","2023-07-16-03","1")
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
