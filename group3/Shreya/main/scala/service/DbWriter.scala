package service

import constants.ApplicationConstants.{DB_DRIVER, DB_PASSWORD, DB_SOURCE, DB_USERNAME}
import exceptions.Exceptions.DatabaseException
import org.apache.spark.sql.DataFrame

object DbWriter {
  def databaseWriter(df: DataFrame, tableName: String, mode: String): Unit = {
    try {
      df.write
        .format(DB_SOURCE)
        .option("driver", DB_DRIVER)
        .option("url", "jdbc:mysql://localhost:3306/datastreaming")
        .option("dbtable", tableName)
        .option("user", DB_USERNAME)
        .option("password", DB_PASSWORD)
        .mode(mode)
        .save()
    } catch {
      case _: Exception => throw DatabaseException("Database connection is not established")
    }
  }
}
