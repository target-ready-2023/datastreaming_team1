import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger._
//This stream will print the output to File (DFS) ( Kafka -> File)
object consumer {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Consumer")
      .master("local")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()


    import spark.implicits._

    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "my_topic")
      .load()

    df.write
      .format("csv")
      //.option("header", "true") // If you want to include the header row
      .option("sep", ",") // If you want to specify a different separator (default is comma)
      .mode("overwrite") // If you want to overwrite the file if it already exists
      .save("D:\target2\targetReadyDataSet\fromConsumer\ReadData")


  }
}