
package graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class With_FullNameTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Order_status for out predicates: order_status") {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    val dfIn = inDf(Seq("customer_id", "first_name", "last_name", "order_id", "order_status", "amount"), Seq(
      Seq[Any]("26".toInt,"Waite","Petschelt","1".toInt,"Finished","586.08".toDouble),
      Seq[Any]("26".toInt,"Waite","Petschelt","2".toInt,"Started","9.85".toDouble),
      Seq[Any]("63".toInt,"Constance","Sleith","11".toInt,"Finished","287.5".toDouble),
      Seq[Any]("35".toInt,"Viva","Schulke","12".toInt,"Finished","514.19".toDouble),
      Seq[Any]("31".toInt,"Barrett","Amies","13".toInt,"Finished","154.41".toDouble),
      Seq[Any]("51".toInt,"Kit","Skamell","14".toInt,"Started","924.89".toDouble),
      Seq[Any]("35".toInt,"Viva","Schulke","15".toInt,"Started","198.05".toDouble),
      Seq[Any]("100".toInt,"Gillan","Heritege","16".toInt,"Finished","121.03".toDouble),
      Seq[Any]("94".toInt,"Ogdan","Bussetti","17".toInt,"Started","551.87".toDouble),
      Seq[Any]("62".toInt,"Homer","Lindstedt","18".toInt,"Finished","343.59".toDouble)
    ))

    

    val dfOutComputed = graph.With_FullName(spark, dfIn)
    

    assertPredicates(
      "out",
      dfOutComputed,
      Seq(
        col("order_status").isin("Finished", "Started", "Approved", "Pending")
      ) zip Seq (
        "order_status"
      )
    )
}

  def inDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "customer_id" -> 0,
      "first_name" -> "",
      "last_name" -> "",
      "order_id" -> 0,
      "order_status" -> "",
      "amount" -> 0.0
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "customer_id" -> IntegerType,
      "first_name" -> StringType,
      "last_name" -> StringType,
      "order_id" -> IntegerType,
      "order_status" -> StringType,
      "amount" -> DoubleType
    )

    createDF(spark, columns, values, defaults, columnToTypeMap, "in")
  }

  def outDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "customer_id" -> "",
      "first_name" -> "",
      "last_name" -> "",
      "order_id" -> "",
      "order_status" -> "",
      "amount" -> "",
      "full_name" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "customer_id" -> StringType,
      "first_name" -> StringType,
      "last_name" -> StringType,
      "order_id" -> StringType,
      "order_status" -> StringType,
      "amount" -> StringType,
      "full_name" -> StringType
    )

    createDF(spark, columns, values, defaults, columnToTypeMap, "out")
  }

  def assertPredicates(port: String, df: DataFrame, predicates: Seq[(Column, String)]): Unit = {
    predicates.foreach({
      case (pred, name) =>
        Assert.assertEquals(
          s"Predicate $name [[`$pred`]] not universally true for port $port",
          df.filter(pred).count(),
          df.count()
        )
    })
  }
}
