package `sample-test`



import consumedReader.partitionedReader
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec

object partitionedReaderTest extends AnyFlatSpec{
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sample").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql



    "partitionedReader" should "read the files and return dataframe" in {
      val actualDF = partitionedReader.reader("D:/TargetCoorporationPhaseSecond/data_from_kafka/TESTING_DATA/file1.json"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/TESTING_DATA/file2.json"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/TESTING_DATA/file3.json"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/TESTING_DATA/file4.json"
      ,"D:/TargetCoorporationPhaseSecond/data_from_kafka/TESTING_DATA/file5.json")

      val expectedDF =
        Seq("84945746","747","Online Only","All inMotion AC699Light BlueXS","2.8360276","10.42576404301743","2023-07-16-03","0"
          ,"84945746","747","Online Only","All inMotion AC699Light BlueXS","2.8360276","10.42576404301743","2023-07-16-03","0"
          ,"84945746","747","Online Only","All inMotion AC699Light BlueXS","2.8360276","10.42576404301743","2023-07-16-03","0"
          ,"84945746","747","Online Only","All inMotion AC699Light BlueXS","2.8360276","10.42576404301743","2023-07-16-03","0"
          ,"84945746","747","Online Only","All inMotion AC699Light BlueXS","2.8360276","10.42576404301743","2023-07-16-03","0"
        ).toDF("product_id","location_id","selling_channel","prod_description","retail_price","onhand_quantity","create_date","promotion_eligibility")

      val differentRows = expectedDF.except(actualDF)
      assertResult(0)(differentRows.count())

    }



    spark.stop()
  }
}