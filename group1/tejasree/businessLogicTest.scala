
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.functions._

class bussinessLogicTest extends AnyFlatSpec{

  val spark = SparkSession.builder().appName("BussinessLogicTest").master("local[*]").getOrCreate()

  "bussinessLogic" should "eliminate error records and put in error tables" in{

    import spark.implicits._

    val errorDF = Seq(
      ("10266320","1194","XYZ","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1777","ABC","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","0"),
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1194","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE",null,"5.798878424473668","2023-07-16-03","1"),
      ("10266320","1777","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","0","5.798878424473668","2023-07-16-03","0"),
      ("ABCS","1194","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("1026","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1777","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","0")

    ).toDF("product_id"
      , "location_id"
      , "selling_channel"
      , "prod_description"
      , "retail_price"
      , "onhand_quantity"
      , "create_date"
      , "promotion_eligibility")

    val processedDF = bussinessLogic.implementLogic(errorDF,"testerrchannel","testerrprice","testerrproductId","thejasree")

    processedDF.show(false)

    val expectedDF = Seq(
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1777","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","0")
    ).toDF("product_id"
      , "location_id"
      , "selling_channel"
      , "prod_description"
      , "retail_price"
      , "onhand_quantity"
      , "create_date"
      , "promotion_eligibility")

    expectedDF.show(false)

    assertResult(0)(processedDF.except(expectedDF).count())

  }
}