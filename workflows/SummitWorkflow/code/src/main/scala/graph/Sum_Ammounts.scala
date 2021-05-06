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

@Visual(id = "Sum_Ammounts", label = "Sum Ammounts", x = 939, y = 293, phase = 0)
object Sum_Ammounts {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {
    import spark.implicits._

    val dfGroupBy = in.groupBy(col("customer_id").as("customer_id"))
    val out = dfGroupBy.agg(
      max(col("first_name")).as("first_name"),
      max(col("last_name")).as("last_name"),
      count(col("order_id")).as("orders"),
      max(col("order_status")).as("order_status"),
      sum(col("amount")).as("amounts"),
      max(col("full_name")).as("full_name")
    )

    out

  }

}
