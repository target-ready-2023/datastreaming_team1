package check

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType

object DataTypeCheck {


  def redefineColumnTypes(df: DataFrame, columnDefs: Map[String, DataType]): DataFrame = {
    columnDefs.foldLeft(df) {
      case (tempDF, (columnName, newDataType)) =>
        tempDF.withColumn(columnName, col(columnName).cast(newDataType))
    }
  }


}
