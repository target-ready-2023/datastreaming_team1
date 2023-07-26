import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.Properties;
import java.sql.DriverManager;

object consumer {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("consumer app")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    val dfWithHeader = df
      .selectExpr("CAST(value AS STRING)")
      .select(split(col("value"), ",").cast(ArrayType(StringType)).alias("data"))
      .select(
        col("data").getItem(0).cast(IntegerType).alias("product_id"),
        col("data").getItem(1).cast(IntegerType).alias("location_id"),
        col("data").getItem(2).alias("selling_channel"),
        col("data").getItem(3).alias("prod_description"),
        col("data").getItem(4).cast(DoubleType).alias("retail_price"),
        col("data").getItem(5).cast(DoubleType).alias("onhand_quantity"),
        col("data").getItem(6).alias("create_date"),
        col("data").getItem(7).cast(IntegerType).alias("promotion_eligibility")
      )

    val url = "jdbc:mariadb://localhost:3306/db"
    val table = "records"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "")

    // Table creation in mysql
//    CREATE TABLE records (
//      product_id BIGINT PRIMARY KEY,
//      location_id SMALLINT,
//      selling_channel VARCHAR(100),
//      prod_description VARCHAR(100),
//      retail_price DECIMAL(10,7),
//      onhand_quantity DECIMAL(17,15),
//      create_date DATE,
//      promotion_eligibility TINYINT
//    );

    // writing to the database
    val query = dfWithHeader.writeStream
      .foreach( new ForeachWriter[Row] {

        def open(partitionId: Long, version: Long): Boolean = true

        def process(record: Row): Unit = {
          val connection = DriverManager.getConnection(url, connectionProperties)
          val statement = connection.prepareStatement(s"INSERT INTO $table (product_id,location_id,selling_channel,prod_description,retail_price,onhand_quantity,create_date,promotion_eligibility) VALUES (?,?,?,?,?,?,?,?)")

          statement.setInt(1, record.getInt(0))
          statement.setInt(2, record.getInt(1))
          statement.setString(3, record.getString(2))
          statement.setString(4, record.getString(3))
          statement.setDouble(5, record.getDouble(4))
          statement.setDouble(6, record.getDouble(5))
          statement.setString(7, record.getString(6))
          statement.setInt(8, record.getInt(7))

          statement.execute()

          statement.close()
          connection.close()
        }
        def close(errorOrNull: Throwable): Unit = {}
      })
        .start()

    // Writing to the console
    val rows = dfWithHeader.writeStream
      .format("console")
      .start()

    query.awaitTermination()
    rows.awaitTermination()
  }
}