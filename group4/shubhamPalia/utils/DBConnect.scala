package utils
import org.apache.spark.sql.DataFrame

object DBConnect{
  def databaseWriter(df:DataFrame, tableName:String): Unit ={
    try {
      df.write
        .format("jdbc")
        .option("driver","com.mysql.cj.jdbc.Driver")
        .option("url", "jdbc:mysql://localhost:3306/targetReadyDataBase")
        .option("dbtable",tableName )
        .option("user", "root")
        .option("password", "targetReady@2023")
        .mode("overwrite")
        .save()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}