import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import utils.globalVariables._
import utils.DBConnect.databaseWriter
object updateDataPipeline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("updateDataPipeline")
      .master("local")
      .getOrCreate()

    // MySQL database connection properties
    val properties = new java.util.Properties()
    properties.setProperty("user", userName)
    properties.setProperty("password", password)

    // Read table from MySQL into DataFrame
    val tableName = "datastream_after_businesslogics"
    val df = spark.read.jdbc(databaseURL, tableName, properties)

    // Add update_date column with the same values as create_date
    val dfWithUpdateDate = df.withColumn("update_date", col("create_date"))
    databaseWriter(dfWithUpdateDate, "df_with_update_date")
    dfWithUpdateDate.show()

    spark.stop()
  }
}
