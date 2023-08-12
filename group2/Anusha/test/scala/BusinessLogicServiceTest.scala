import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import constants.AppConstants.{PRIMARY_KEY_COLS, VALID_SELLING_CHANNEL}
import service.BusinessLogicService.{deduplicate, productIdValidator, retailPriceValidator, sellingChannelValidator}

class BusinessLogicServiceTest extends AnyFlatSpec {
  val spark: SparkSession = helper.Helper.createSparkSession()

  import spark.implicits._

  "selling channel" should " only have values <Cross Over,Online Only,Store Only >" in {

    val inputDF = Seq(
      ("10266320", "1194", "null", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266321", "1479", "Others", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266320", "1770", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "0"),
      ("10266322", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266325", "2748", "Online Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266326", "2748", "Cross", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val expectedDF = Seq(("10266320", "1770", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "0"),
      ("10266322", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266325", "2748", "Online Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val outputDF = sellingChannelValidator(inputDF, "error_table_test", VALID_SELLING_CHANNEL)
    val result = expectedDF.except(outputDF)
    val ans = result.count()
    val count = 0;
    assertResult(count)(ans)
  }

  "retail_price" should "  not be 0 or null" in {
    val inputDF = Seq(
      ("10266320", "1194", "null", "PRSMN LGF DX LET'S GO FISHIN DELUXE", null, "5.798878424473668", "2023-07-16-03", "1"),
      ("10266321", "1479", "Others", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "0", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266320", "1770", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "", "5.798878424473668", "2023-07-16-03", "0"),
      ("10266324", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266325", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266326", "2747", "Cross", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val expectedDF = Seq(

      ("10266324", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266325", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266326", "2747", "Cross", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")
    val outputDF = retailPriceValidator(inputDF, "error_table_test")

    val result = expectedDF.except(outputDF)
    val ans = result.count()
    val count = 0;
    assertResult(count)(ans)
  }

  "product_id " should "be numeric and 8 digit" in {

    val inputDF = Seq(
      ("10266320", "1194", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", null, "5.798878424473668", "2023-07-16-03", "1"),
      ("10266", "1479", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "0", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266321", "1770", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "0"),
      ("hnbjvmk", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266325", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266326", "2747", "Cross", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val expectedDF = Seq(
      ("10266320", "1194", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", null, "5.798878424473668", "2023-07-16-03", "1"),
      ("10266321", "1770", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "0"),
      ("10266325", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266326", "2747", "Cross", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description", "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")
    val outputDF = productIdValidator(inputDF, "error_table_test")
    val result = expectedDF.except(outputDF)
    val ans = result.count()
    val count = 0;
    assertResult(count)(ans)
  }

  "no duplicate record" should "be present in dataset based on primary key(product_id ,location_id)" in {
    val inputDF = Seq(
      ("10266125", "1479", "Online Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "0", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266125", "1479", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "0", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266321", "1770", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "0"),
      ("10266326", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266325", "2748", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266326", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val expectedDF = Seq(

      ("10266125", "1479", "Online Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "0", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266321", "1770", "Store Only", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "0"),
      ("10266326", "2747", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1"),
      ("10266325", "2748", "Cross Over", "PRSMN LGF DX LET'S GO FISHIN DELUXE", "207.8262024", "5.798878424473668", "2023-07-16-03", "1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description", "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val outputDF = deduplicate(inputDF, PRIMARY_KEY_COLS)
    val result = expectedDF.except(outputDF)
    val ans = result.count()
    val count = 0;
    assertResult(count)(ans)

  }
}
