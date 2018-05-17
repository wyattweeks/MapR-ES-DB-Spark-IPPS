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

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class PaymentwId(
    @JsonProperty("_id") _id: String,
    @JsonProperty("drg_definition") drg_definition: String,
    @JsonProperty("provider_id") provider_id: String,
    @JsonProperty("provider_name") provider_name: String,
    @JsonProperty("provider_address") provider_address: String,
    @JsonProperty("provider_city") provider_city: String,
    @JsonProperty("provider_state") provider_state: String,
    @JsonProperty("provider_zip") physician_name_first: String,
    @JsonProperty("hospital_description") physician_name_middle: String,
    @JsonProperty("total_discharges") physician_name_last: Double,
    @JsonProperty("avg_covered_charges") physician_name_suffix: Double,
    @JsonProperty("avg_total_payments") recipient_city: Double,
    @JsonProperty("avg_medicare_payments") recipient_state: Double
  ) extends Serializable
  /*
  case class PaymentwId(_id: String, drg_definition: String, provider_id: String, provider_name: String, provider_address: String, provider_city: String, provider_state: String, provider_zip: String, hospital_description: String, total_discharges: Double, avg_covered_charges: Double, avg_total_payments: Double, avg_medicare_payments: Double) extends Serializable
*/
  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("drg_definition", StringType, true),
    StructField("provider_id", StringType, true),
    StructField("provider_name", StringType, true),
    StructField("provider_address", DoubleType, true),
    StructField("provider_city", StringType, true),
    StructField("provider_state", StringType, true),
    StructField("provider_zip", StringType, true),
    StructField("hospital_description", StringType, true),
    StructField("total_discharges", StringType, true),
    StructField("avg_covered_charges", StringType, true),
    StructField("avg_total_payments", StringType, true),
    StructField("avg_medicare_payments", StringType, true)
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

   // println("Filter for payment amount > $2000")
   // pdf.filter($"amount" > 2000).show()
   // println("Select physician id , amount ")
   // pdf.select("_id", "physician_id", "amount").show

   // println("What are the Top 5 Nature of Payments by count ")
   // pdf.groupBy("Nature_of_payment").count().orderBy(desc("count")).show(5)

    //println("What are the Nature of Payments with payments > $1000 with count")
    //pdf.filter($"amount" > 1000).groupBy("Nature_of_payment").count().orderBy(desc("count")).show()

    //println("What are the  payments for physician id 98485")
    //pdf.filter($"_id".like("98485%")).select($"_id", $"physician_specialty", $"amount").show(false)

    //println("What are the  payments for the month of february")
    //pdf.filter($"_id".like("%_02/%")).select($"_id", $"physician_specialty", $"amount").show(false)

    // Create a temporary view in order to use SQL for queries
    //pdf.createOrReplaceTempView("payments")
    //Top 5 nature of payment by total amount
    //println("Top 5 nature of payment by total amount")
    //spark.sql("select Nature_of_payment,  sum(bround(amount)) as total from payments group by Nature_of_payment order by total desc limit 5").show
    //println("Top 5 Physician Specialties by Amount with count")
    //spark.sql("select physician_specialty, count(*) as cnt, sum(bround(amount)) as total from payments where physician_specialty IS NOT NULL group by physician_specialty order by total desc limit 5").show

    //Top 5 Physician Specialties by total Amount
    //println("Top 5 Physician Specialties by Amount")
    //spark.sql("select physician_specialty, sum(bround(amount)) as total from payments where physician_specialty IS NOT NULL group by physician_specialty order by total desc limit 5").show(false)

    //find payments for physician id 98485
    //println("find payments for physician id 98485")
    //spark.sql("select _id, physician_id, amount from payments where _id like '98485%'").show(false)

    //find payments for february
    //println("find payments for february")
    //spark.sql("select _id, physician_id, amount from payments where _id like '%_02/%'").show(false)
  }
}

