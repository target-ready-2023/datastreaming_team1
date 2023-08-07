import Constants.Variables.MY_PASSWORD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.types.{StringType, StructType}

object BusinessLogics {
    def main(args:Array[String]): Unit ={

        val spark = SparkSession.builder
          .appName("Business Logics")
          .master("local[*]")
          .getOrCreate()

        spark.sparkContext.setLogLevel("error")

        val schema = new StructType()
          .add("product_id", StringType, true)
          .add("location_id", StringType,  true)
          .add("selling_channel", StringType, true)
          .add("prod_description", StringType, true)
          .add("retail_price", StringType, true)
          .add("onhand_quantity", StringType, true)
          .add("create_date", StringType, true)
          .add("promotion_eligibility", StringType, true)

        val JSONdf = spark.read.format("json")
          .option("inferSchema","true")
          .load("D:/phase2/data/1M/json")

        val df = JSONdf.withColumn("value",from_json(col("value"), schema))
          .select(col("value.*"))

        df.show(5,false)
        df.printSchema()

        // Move messages with different selling  channel other than ONLINE,STORE,CROSSOVER into an error table.
        val selling_channel_errors = df.filter(df("selling_channel")=!="Cross Over" && df("selling_channel")=!="Store Only" && df("selling_channel")=!="Online Only")
//        println("selling_channel_errors.")
//        selling_channel_errors.select(col("*"),lit("selling_channel_error")).show(false)

        selling_channel_errors.select(col("*"),lit("selling_channel_error").as("error")).write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "datastream_error_table")
          .option("user", "postgres")
          .option("password", MY_PASSWORD)
          .mode("append")
          .save()

        // Check price is not null or 0. If so, move it to error table.
        val valid_records = df.filter(df("selling_channel")==="Cross Over" || df("selling_channel")==="Store Only" || df("selling_channel")==="Online Only")

        val retail_price_errors = valid_records.filter(valid_records("retail_price")===0 || valid_records("retail_price").isNull)
//        println("retail_price_errors.")
//        retail_price_errors.show(false)

        retail_price_errors.select(col("*"),lit("retail_price_error").as("error")).write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "datastream_error_table")
          .option("user", "postgres")
          .option("password", MY_PASSWORD)
          .mode("append")
          .save()

        // Product id should be only numeric and 8 digit.

        val removed_null_retail_prices = valid_records.filter(valid_records("retail_price")=!=0 && valid_records("retail_price").isNotNull)

        val only_numeric_product_ids = removed_null_retail_prices.filter(removed_null_retail_prices("product_id").cast("Int").isNotNull)

        only_numeric_product_ids.createOrReplaceTempView("Dataset")

        val product_id_errors = spark.sql("select * from Dataset where length(product_id) > 8 or length(product_id) < 8")
//        println("product_id_errors.")
//        product_id_errors.show(false)

        product_id_errors.select(col("*"),lit("product_id_error").as("error")).write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "datastream_error_table")
          .option("user", "postgres")
          .option("password", MY_PASSWORD)
          .mode("append")
          .save()

        val final_df = spark.sql("select * from Dataset where length(product_id) = 8")

//        println("Final_DF.")
//        final_df.show(false)
//        println(final_df.count())

        val distinct_finaL_df = final_df.dropDuplicates(Seq("product_id","location_id"))

//        println("distinct_final_df.")
//        distinct_finaL_df.show(false)
//        println(distinct_finaL_df.count())

        // Dumping online only channels.
        distinct_finaL_df.filter(distinct_finaL_df("selling_channel")==="Online Only")
          .drop("selling_channel").write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "online_only_table")
          .option("user", "postgres")
          .option("password", MY_PASSWORD)
          .mode("overwrite")
          .save()

        // Dumping store only channels.
        distinct_finaL_df.filter(distinct_finaL_df("selling_channel")==="Store Only")
          .drop("selling_channel").write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "store_only_table")
          .option("user", "postgres")
          .option("password", MY_PASSWORD)
          .mode("overwrite")
          .save()

        // Dumping cross over channels.
        distinct_finaL_df.filter(distinct_finaL_df("selling_channel")==="Cross Over")
          .drop("selling_channel").write
          .format("jdbc")
          .option("url", "jdbc:postgresql://localhost:5432/postgres")
          .option("driver", "org.postgresql.Driver")
          .option("dbtable", "cross_over_table")
          .option("user", "postgres")
          .option("password", MY_PASSWORD)
          .mode("overwrite")
          .save()

        println("SUCCESS.")
        spark.stop()

    }
}
