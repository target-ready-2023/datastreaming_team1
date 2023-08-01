package template.spark

import org.apache.spark.sql.DataFrame

object dbConnection {
  def errorTableWriter(df: DataFrame): Unit = {
    val DBURL = "jdbc:mysql://localhost:3306/target_ready"
    try {
      df.write.format("jdbc")
        .option("url", DBURL)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "error_table")
        .option("user", "root")
        .option("password", "gittika")
        .mode("overwrite")
        .save()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
