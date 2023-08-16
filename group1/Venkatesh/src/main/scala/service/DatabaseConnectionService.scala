package service

import constants.ApplicationConstants._
import org.apache.spark.sql._

object DatabaseConnectionService {
  def DatabaseTableWriter(df: DataFrame, table: String): Unit = {
    try {
      df.write.format(DB_SOURCE)
        .option("url", "jdbc:postgresql://localhost:5432/postgres")
        .option("driver", JDBC_DRIVER)
        .option("dbtable", table)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .mode("append")
        .save()
    }
    catch{
      case e: Exception => e.printStackTrace()
    }
  }
}