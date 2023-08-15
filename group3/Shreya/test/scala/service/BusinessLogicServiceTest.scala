package service

import constants.ApplicationConstants.{PRIMARY_KEY_COLS, VALID_SELLING_CHANNEL}
import helper.Helper.TEST_ERROR_TABLE
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import service.BusinessLogicService.{deduplication, productIdCheck, retailPriceCheck, sellingChannelCheck}

class BusinessLogicServiceTest extends AnyFlatSpec{

  val spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("error")

  "selling channel" should "only have Cross Over, Online Only or Store Only" in{
    val inputDf = Seq(
        ("85910945","1898","Online Only","Phy Formula BfClbLpstkRckBlck0.3oz","2.5764942","12.297023335015293","2023-07-16-03","0"),
        ("84990205","1918","Cross Over","Planner Senn Rolling Pin 5x8","10.9885941","10.332639048469366","2023-07-16-03","0"),
        ("85458913","7311","Store Only","LoveWellness Feminine Wash 5floz","9.1","10.978340657203212","2023-07-16-03","1"),
        ("87374073","2098","Any","AllinMotion ADT37LilacPurpleXS","1.0","5.57940907815972","2023-07-16-03","0"),
        ("84679328","2264","Others","C & J Parrish Blue 2T","1.1686449","13.998179527905362","2023-07-16-03","0")
      ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val outputDf = sellingChannelCheck(inputDf, TEST_ERROR_TABLE, VALID_SELLING_CHANNEL)

    val expectedDf = Seq(
      ("85910945","1898","Online Only","Phy Formula BfClbLpstkRckBlck0.3oz","2.5764942","12.297023335015293","2023-07-16-03","0"),
      ("84990205","1918","Cross Over","Planner Senn Rolling Pin 5x8","10.9885941","10.332639048469366","2023-07-16-03","0"),
      ("85458913","7311","Store Only","LoveWellness Feminine Wash 5floz","9.1","10.978340657203212","2023-07-16-03","1")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val result = expectedDf.except(outputDf)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }

  "retail price" should "not have 0 or null" in{
    val inputDf = Seq(
      ("85910945","1898","Online Only","Phy Formula BfClbLpstkRckBlck0.3oz","0","12.297023335015293","2023-07-16-03","0"),
      ("84990205","1918","Cross Over","Planner Senn Rolling Pin 5x8","","10.332639048469366","2023-07-16-03","0"),
      ("85458913","7311","Store Only","LoveWellness Feminine Wash 5floz",null,"10.978340657203212","2023-07-16-03","1"),
      ("87374073","2098","Any","AllinMotion ADT37LilacPurpleXS","1.0","5.57940907815972","2023-07-16-03","0"),
      ("84679328","2264","Others","C & J Parrish Blue 2T","1.1686449","13.998179527905362","2023-07-16-03","0")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val outputDf = retailPriceCheck(inputDf, TEST_ERROR_TABLE)

    val expectedDf = Seq(
      ("87374073","2098","Any","AllinMotion ADT37LilacPurpleXS","1.0","5.57940907815972","2023-07-16-03","0"),
      ("84679328","2264","Others","C & J Parrish Blue 2T","1.1686449","13.998179527905362","2023-07-16-03","0")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val result = expectedDf.except(outputDf)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }

  "product id" should "be numeric and 8 digit" in{
    val inputDf = Seq(
      ("8591","1898","Online Only","Phy Formula BfClbLpstkRckBlck0.3oz","2.5764942","12.297023335015293","2023-07-16-03","0"),
      ("ABCD","1918","Cross Over","Planner Senn Rolling Pin 5x8","10.9885941","10.332639048469366","2023-07-16-03","0"),
      ("8545ABCD","7311","Store Only","LoveWellness Feminine Wash 5floz","9.1","10.978340657203212","2023-07-16-03","1"),
      ("87374073","2098","Online Only","AllinMotion ADT37LilacPurpleXS","1.0","5.57940907815972","2023-07-16-03","0"),
      ("84679328","2264","Online Only","C & J Parrish Blue 2T","1.1686449","13.998179527905362","2023-07-16-03","0")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val outputDf = productIdCheck(inputDf, TEST_ERROR_TABLE)

    val expectedDf = Seq(
      ("87374073","2098","Online Only","AllinMotion ADT37LilacPurpleXS","1.0","5.57940907815972","2023-07-16-03","0"),
      ("84679328","2264","Online Only","C & J Parrish Blue 2T","1.1686449","13.998179527905362","2023-07-16-03","0")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val result = expectedDf.except(outputDf)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }

  "no duplicate records" should "be present based on primary key" in{
    val inputDf = Seq(
      ("85910945","1898","Online Only","Phy Formula BfClbLpstkRckBlck0.3oz","2.5764942","12.297023335015293","2023-07-16-03","0"),
      ("85910945","1898","Cross Over","Planner Senn Rolling Pin 5x8","10.9885941","10.332639048469366","2023-07-16-03","0"),
      ("85458913","7311","Store Only","LoveWellness Feminine Wash 5floz","9.1","10.978340657203212","2023-07-16-03","1"),
      ("87374073","2098","Online Only","AllinMotion ADT37LilacPurpleXS","1.0","5.57940907815972","2023-07-16-03","0"),
      ("84679328","2264","Online Only","C & J Parrish Blue 2T","1.1686449","13.998179527905362","2023-07-16-03","0")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val outputDf = deduplication(inputDf, PRIMARY_KEY_COLS)

    val expectedDf = Seq(
      ("85910945","1898","Online Only","Phy Formula BfClbLpstkRckBlck0.3oz","2.5764942","12.297023335015293","2023-07-16-03","0"),
      ("85458913","7311","Store Only","LoveWellness Feminine Wash 5floz","9.1","10.978340657203212","2023-07-16-03","1"),
      ("87374073","2098","Online Only","AllinMotion ADT37LilacPurpleXS","1.0","5.57940907815972","2023-07-16-03","0"),
      ("84679328","2264","Online Only","C & J Parrish Blue 2T","1.1686449","13.998179527905362","2023-07-16-03","0")
    ).toDF("product_id",
      "location_id",
      "selling_channel",
      "prod_description",
      "retail_price",
      "onhand_quantity",
      "create_date",
      "promotion_eligibility")

    val result = expectedDf.except(outputDf)
    val ans = result.count()
    val count = 0
    assertResult(count)(ans)
  }

}