import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import utils.globalVariables._
object producerCSV {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("newCSV").getOrCreate()
    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("D:\\targetReadyDataSet.csv")
    val colum_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(colum_names: _*)
    dfWithHeader.select((struct("product_id").cast("string")).alias("key"), to_json(struct("*")).alias("value")).write.format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("topic", kafkaTopic).save()
  }
}