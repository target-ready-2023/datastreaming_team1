package producerConsumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{struct, to_json}

object Producer {


  def producerJSON(spark: SparkSession, path: String, sourceformat: String, writeFormat: String, topic: String) = {
    val df = spark.read.format(sourceformat).option("delimiter", ",").load(path)


    val column_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
    val dfWithHeader = df.toDF(column_names: _*)
    dfWithHeader.select(
      (struct("product_id").cast("string")).alias("key"),
      to_json(struct("*")).alias("value")).write.format(writeFormat).
      option("kafka.bootstrap.servers", "localhost:9092").
      option("topic", topic).save()
  }

}
