package service

import constants.AppConstants.{DB_PASSWORD, DB_SOURCE, DB_USER, JDBC_DRIVER}
import org.apache.spark.sql.DataFrame

object DatabaseConnection {
  def FileWriter(df: DataFrame, tableName: String, mode: String): Unit = {
    try {
      df.write.format(DB_SOURCE)
        .option("url", "jdbc:mysql://localhost:3306/targetready")
        .option("driver", JDBC_DRIVER)
        .option("dbtable", tableName)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .mode(mode)
        .save()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
