package service

import exceptions.Exceptions.{EmptyFilePathException, FilePathNotFoundException, FileReaderException, InvalidInputFormatException}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions.{struct, to_json}

object ProducerService {
  def producer(filePath : String, topicName : String, bootstrapServer : String, column_names : Seq[String]) (implicit sparkSession: SparkSession) {

    //if input path is empty
    if (filePath == "") {
      throw EmptyFilePathException("The file path is empty. Please provide a valid file path.")
    }
    
    try{
      //creating a dataframe from the given file path
      val df = sparkSession.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .load(filePath)

      //adding headers to the dataframe created
      val dfWithHeader = df.toDF(column_names: _*)

      //loading the dataframe into the desired kafka topic
      dfWithHeader.select((struct("product_id").cast("string")).alias("key"), to_json(struct("*")).alias("value")).write.format("kafka").
        option("kafka.bootstrap.servers", bootstrapServer).
        option("topic", topicName).save()
    }
      
    catch{
      case _: AnalysisException => throw FilePathNotFoundException("The file path is not found.")
      case _: java.lang.ClassNotFoundException => throw InvalidInputFormatException("The file format is invalid")
      case _: Exception => throw FileReaderException("Unable to read file from given path.")
    }
  }
}
