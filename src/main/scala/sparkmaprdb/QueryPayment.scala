package sparkmaprdb

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._
import org.apache.log4j.{ Level, Logger }
import com.fasterxml.jackson.annotation.{ JsonIgnoreProperties, JsonProperty }

object QueryPayment {

  case class PaymentwId(_id: String, drg_code: String, drg_definition: String, provider_id: String, provider_name: String, provider_address: String,
                        provider_city: String, provider_state: String, provider_zip: String, provider_region: String,
                        total_discharges: Double, avg_covered_charges: Double, avg_total_payments: Double,
                        avg_medicare_payments: Double) extends Serializable

  /*
   case class PaymentwId(_id: String, physician_id: String, date_payment: String, payer: String, amount: Double, physician_specialty: String,
    nature_of_payment: String) extends Serializable
  */

  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("drg_code", StringType, true),
    StructField("drg_definition", StringType, true),
    StructField("provider_id", StringType, true),
    StructField("provider_name", StringType, true),
    StructField("provider_address", StringType, true),
    StructField("provider_city", StringType, true),
    StructField("provider_state", StringType, true),
    StructField("provider_zip", StringType, true),
    StructField("provider_region", StringType, true),
    StructField("total_discharges", DoubleType, true),
    StructField("avg_covered_charges", DoubleType, true),
    StructField("avg_total_payments", DoubleType, true),
    StructField("avg_medicare_payments", DoubleType, true)
  ))

  def main(args: Array[String]) {

    var tableName: String = "/user/mapr/demo.mapr.com/tables/payments"
    if (args.length == 1) {
      tableName = args(0)
    } else {
      System.out.println("Using hard coded parameters unless you specify the tablename ")
    }
    val spark: SparkSession = SparkSession.builder().appName("querypayment").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    Logger.getLogger("org").setLevel(Level.OFF)

    import spark.implicits._
    // load payment dataset from MapR-DB 
    val pdf: Dataset[PaymentwId] = spark.sparkSession.loadFromMapRDB[PaymentwId](tableName, schema).as[PaymentwId]

   println("Filter for avg_total_payments > $2000")
    pdf.filter($"avg_total_payments" > 2000).show()
    println("Select provider_name , avg_total_payments ")
    pdf.select("_id", "provider_name", "avg_total_payments").show

    println("What are the Top drg_code by count ")
    pdf.groupBy("drg_code").count().orderBy(desc("count")).show(5)

  }
}

