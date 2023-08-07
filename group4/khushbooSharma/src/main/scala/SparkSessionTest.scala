import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scala.util.Random
import org.apache.spark.sql.Row

object SparkSessionTest {

  def main(args:Array[String]): Unit ={

    //no of rows required
    val rows = 10
    //no of columns required
    val cols = 10

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("testApp")
      .getOrCreate()



    val columns = 1 to cols map (i => "col" + i)

    // create the DataFrame schema with these columns (in that order)
    val schema = StructType(columns.map(StructField(_, IntegerType)))

    val lstrows = Seq.fill(rows * cols)(Random.nextInt(100) + 1).grouped(cols).toList.map { x => Row(x: _*) }

    val rdd = spark.sparkContext.makeRDD(lstrows)
    val df = spark.createDataFrame(rdd, schema)
    df.show()
    //df.coalesce(1).write.option("header", "true").csv("/Users/z083276/Downloads/sparkscaladf/test/")
//    val data = 1 to 100 map (x => (1 + Random.nextInt(100), 1 + Random.nextInt(100), 1 + Random.nextInt(100)))
//
//    spark.createDataFrame(data).toDF("col1", "col2", "col3").show(false)

  }
}