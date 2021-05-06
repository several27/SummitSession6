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

@Visual(id = "Target0", label = "Target0", x = 1135, y = 290, phase = 0)
object Target0 {

  @UsesDataset(id = "1812", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id",  IntegerType, false),
            StructField("first_name",   StringType,  false),
            StructField("last_name",    StringType,  false),
            StructField("orders",       LongType,    false),
            StructField("order_status", StringType,  false),
            StructField("amounts",      DoubleType,  false),
            StructField("full_name",    StringType,  false)
          )
        )
        in.write
          .format("parquet")
          .mode("overwrite")
          .save("dbfs:/DatabricksSession/Report.pa/")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
