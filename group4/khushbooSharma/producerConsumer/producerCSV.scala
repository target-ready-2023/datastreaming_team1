package producerConsumer

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object producerCSV {
  def prodcue(spark: SparkSession, path:String,format: String, kafakServerPort:String, topic:String): Unit ={
    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load(path)

    val colum_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(colum_names: _*)

    dfWithHeader.select((struct("product_id").cast("string")).alias("key"), to_json(struct("*")).alias("value")).write.format(format).
      option("kafka.bootstrap.servers", kafakServerPort).
      option("topic", topic).save()

  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("newCSV").getOrCreate()

    prodcue(spark,"D:/TargetCoorporationPhaseSecond/Data/targetReadyDataSet.csv","kafka","localhost:9092","datastream")
  }
}