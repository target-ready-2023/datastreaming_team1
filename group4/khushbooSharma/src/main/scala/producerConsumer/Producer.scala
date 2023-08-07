package producerConsumer

import constants.ApplicationConstants.{DATA_STREAM_COLUMN_NAMES, KEY, KEY_COLUMN_NAME, VALUE}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Producer {
  def prodcue(spark: SparkSession, path:String,format: String, kafakServerPort:String, topic:String): Unit ={
    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load(path)

    val dfWithHeader = df.toDF(DATA_STREAM_COLUMN_NAMES: _*)

    dfWithHeader.select((struct(KEY_COLUMN_NAME).cast("string")).alias(KEY), to_json(struct("*")).alias(VALUE)).write.format(format).
      option("kafka.bootstrap.servers", kafakServerPort).
      option("topic", topic).save()

  }

//  def main(args: Array[String]) {
//    val spark = SparkSession.builder.master("local[*]").appName("newCSV").getOrCreate()
//
//    prodcue(spark,"D:/TargetCoorporationPhaseSecond/Data/targetReadyDataSet.csv","kafka","localhost:9092","datastream")
//  }
}