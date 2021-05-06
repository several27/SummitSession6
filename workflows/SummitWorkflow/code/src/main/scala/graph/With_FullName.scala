package graph

import org.apache.spark.sql.types._
import io.prophecy.libs._
import io.prophecy.libs.UDFUtils._
import io.prophecy.libs.Component._
import io.prophecy.libs.DataHelpers._
import io.prophecy.libs.SparkFunctions._
import io.prophecy.libs.FixedFileFormatImplicits._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "With_FullName", label = "With FullName", x = 751, y = 293, phase = 0)
object With_FullName {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("order_id"),
      col("order_status"),
      col("amount"),
      concat(col("first_name"), col("last_name")).as("full_name")
    )

    out

  }

}
