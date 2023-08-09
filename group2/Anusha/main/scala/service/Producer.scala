package service

import exceptions.Exceptions.EmptyFilePathException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{struct, to_json}

object Producer {
  def producer(filePath : String, topicName : String, bootstrapServer : String, column_names : Seq[String]) (implicit sparkSession: SparkSession) {

    //if input path is empty
    if (filePath == "") {
      throw EmptyFilePathException("The file path is empty. Please provide a valid file path.")
    }

    val df = sparkSession.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load(filePath)

    val dfWithHeader = df.toDF(column_names: _*)

    dfWithHeader.select((struct("product_id").cast("string")).alias("key"), to_json(struct("*")).alias("value")).write.format("kafka").
      option("kafka.bootstrap.servers", bootstrapServer).
      option("topic", topicName).save()
  }
}
