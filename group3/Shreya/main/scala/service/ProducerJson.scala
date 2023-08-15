package service

import exceptions.Exceptions.{EmptyFilePathException, FilePathNotFoundException, FileReaderException}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions.{struct, to_json}

object ProducerJson {
  def producerJSON(path: String, bootstrapServer: String, topic: String)(implicit spark: SparkSession) {

    //if input path is empty
    if (path == "") {
      throw EmptyFilePathException("The file path is empty. Please provide a valid file path.")
    }
    try {
      //reads the file into a dataframe
      val df = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").load(path)
      val column_names = Seq("product_id", "location_id", "selling_channel", "prod_description", "retail_price", "onhand_quantity", "create_date", "promotion_eligibility")
      val dfWithHeader = df.toDF(column_names: _*)

      //loads the dataframe into the kafka topic
      dfWithHeader
        .select((struct("product_id").cast("string")).alias("key"), to_json(struct("*")).alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServer)
        .option("topic", topic).save()
    }
    catch {
      case _: AnalysisException => throw FilePathNotFoundException("The file path is not found.")
      case _: Exception => throw FileReaderException("Unable to read file from given path.")
    }

  }
}
