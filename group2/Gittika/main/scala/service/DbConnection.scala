package service

import constants.ApplicationConstants
import org.apache.spark.sql.DataFrame

object DbConnection {
  def errorTableWriter(df: DataFrame, table: String): Unit = {
    val DBURL = "jdbc:mysql://localhost:3306/target_ready"
    try {
      df.write.format("jdbc")
        .option("url", DBURL)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", table)
        .option("user", "root")
        .option("password", ApplicationConstants.db_password)
        .mode("append")
        .save()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
