
import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets
import java.util.Base64
import org.apache.spark.sql.SparkSession
object dbconnect {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("testApp")
      .getOrCreate()
    val password = ""

    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("C:/Users/Dell/IdeaProjects/datastreaming_team1/testset.csv")
    try {
      df.write.format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/target_project")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "test2")
        .option("user", "root")
        .option("password", password)
        .mode("overwrite")
        .save()
    }
    catch {
      case e: Exception => println("Database connection is not established")
    }
  }
}