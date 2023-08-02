package bussinessLogicsPackage

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class bussinessLogicTest extends AnyFlatSpec{

  val spark = SparkSession.builder().appName("BussinessLogicTest").master("local[*]").getOrCreate()

  "bussinessLogic" should "save the records in mySQL" in{

    import spark.implicits._

    val erroredDF = Seq(
      ("10266320","1194","Something Else","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1777","Something Else","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","0"),
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1")
    ).toDF("product_id"
      , "location_id"
      , "selling_channel"
      , "prod_description"
      , "retail_price"
      , "onhand_quantity"
      , "create_date"
      , "promotion_eligibility")

    val processedDF = bussinessLogicsPackage.bussinessLogics.sellingChannelHandler(erroredDF,"error_selling_channel_test")

    processedDF.show(false)

    val expectedDF = Seq(
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1")
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

  "retailPriceHandler" should "remove null or 0 prices" in {

    import spark.implicits._

    val inputDF = Seq(
      ("10266320","1194","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","","5.798878424473668","2023-07-16-03","1"),
      ("10266320","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE",null,"5.798878424473668","2023-07-16-03","1"),
      ("10266320","1777","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","0","5.798878424473668","2023-07-16-03","0"),
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1")
    ).toDF("product_id"
      , "location_id"
      , "selling_channel"
      , "prod_description"
      , "retail_price"
      , "onhand_quantity"
      , "create_date"
      , "promotion_eligibility")

    val processedDF = bussinessLogicsPackage.bussinessLogics.retailPriceHandler(inputDF,"error_retail_price_test")
    processedDF.show(false)

    val expectedDF = Seq(
      ("10266320","2747","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1")
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

  "productIdHandler" should "remove invalid productID records" in {

    import spark.implicits._

    val inputDF = Seq(
      ("ABCS","1194","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
      ("1026","1479","Cross Over","PRSMN LGF DX LET'S GO FISHIN DELUXE","207.8262024","5.798878424473668","2023-07-16-03","1"),
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

    val processedDF = bussinessLogicsPackage.bussinessLogics.productIdHandler(spark,inputDF,"error_product_id_test")
    processedDF.show(false)

    val expectedDF = Seq(
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

    expectedDF.show(false)

    assertResult(0)(processedDF.except(expectedDF).count())
  }
}
