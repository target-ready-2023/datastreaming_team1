import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}

object BusinessLogics {
    def main(args:Array[String]): Unit ={

        val spark = SparkSession.builder
          .appName("Business Logics")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("error")
        import spark.implicits._

        val schema = new StructType()
          .add("product_id", StringType, true)
          .add("location_id", StringType, true)
          .add("selling_channel", StringType, true)
          .add("prod_description", StringType, true)
          .add("retail_price", StringType, true)
          .add("onhand_quantity", StringType, true)
          .add("create_date", StringType, true)
          .add("promotion_eligibility", StringType, true)

        val JSONdf = spark.read.format("json")
          .option("inferSchema","true")
          .load("D:/phase2/data/out1/records/part-00000-c5ac8b86-fcc6-4073-8d13-fe6bf4354fa4.json")

        val df = JSONdf.withColumn("value",from_json(col("value"), schema))
          .select(col("value.*"))

        df.show(false)
        df.printSchema()

        // Move messages with different selling  channel other than ONLINE,STORE,CROSSOVER into an error table.
        val selling_channel_errors = df.filter(df("selling_channel")=!="Cross Over" && df("selling_channel")=!="Store Only" && df("selling_channel")=!="Online Only")

        selling_channel_errors.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "selling_channel_errors")
          .option("user", "postgres")
          .option("password", "Venky@44")
          .mode("overwrite")
          .save()

        // Check price is not null or 0. If so, move it to error table.
        val valid_records = df.filter(df("selling_channel")==="Cross Over" && df("selling_channel")==="Store Only" && df("selling_channel")==="Online Only")

        val retail_price_errors = valid_records.filter(valid_records("retail_price")===0 || valid_records("retail_price").isNull)

        retail_price_errors.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "retail_price_errors")
          .option("user", "postgres")
          .option("password", "Venky@44")
          .mode("overwrite")
          .save()

        // Product id should be only numeric and 8 digit.

        val removed_null_retail_prices = valid_records.filter(valid_records("retail_price")=!=0 && valid_records("retail_price").isNotNull)

        val only_numeric_product_ids = removed_null_retail_prices.filter(removed_null_retail_prices("product_id").cast("Int").isNotNull)

        only_numeric_product_ids.createOrReplaceTempView("Dataset")

        val product_id_errors = spark.sql("select * from Dataset where length(product_id) > 8 or length(product_id) < 8")

        product_id_errors.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "product_id_errors")
          .option("user", "postgres")
          .option("password", "Venky@44")
          .mode("overwrite")
          .save()

        val final_df = spark.sql("select * from Dataset where length(product_id) = 8")

        spark.stop()

    }
}
