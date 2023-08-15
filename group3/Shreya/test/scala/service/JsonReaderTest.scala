package service

import helper.Helper.createSparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import service.JsonReader.jsonReader

class JsonReaderTest extends AnyFlatSpec{

  implicit val spark: SparkSession = createSparkSession()
  import spark.implicits._
  spark.sparkContext.setLogLevel("error")

  "json reader" should "return a dataframe" in {
    val outputDf = jsonReader( "C:\\tmp\\data\\out\\jsonDatasetTest\\file1.json",
      "C:\\tmp\\data\\out\\jsonDatasetTest\\file2.json",
      "C:\\tmp\\data\\out\\jsonDatasetTest\\file3.json")

    val expectedDf =
      Seq(("85910945","1898","Online Only","Phy Formula BfClbLpstkRckBlck0.3oz","2.5764942","12.297023335015293","2023-07-16-03","0"),
          ("84990205","1918","Online Only","Planner Senn Rolling Pin 5x8","10.9885941","10.332639048469366","2023-07-16-03","0"),
          ("85458913","7311","Store Only","LoveWellness Feminine Wash 5floz","9.1","10.978340657203212","2023-07-16-03","1"),
          ("87374073","2098","Online Only","AllinMotion ADT37LilacPurpleXS","1.0","5.57940907815972","2023-07-16-03","0"),
          ("84679328","2264","Online Only","C & J Parrish Blue 2T","1.1686449","13.998179527905362","2023-07-16-03","0")
    ).toDF("product_id","location_id","selling_channel","prod_description","retail_price","onhand_quantity","create_date","promotion_eligibility")

    val differentRows = expectedDf.except(outputDf)
    assertResult(0)(differentRows.count())

  }
}