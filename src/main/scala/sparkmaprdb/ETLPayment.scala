package sparkmaprdb

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._

/*
Spark ETL 
reads a CSV file. transforms CSV to JSON and writes to MapR-DB JSON
*/

object ETLPayment {

  case class Payment(drg_definition: String, provider_id: String, provider_name: String, provider_address: String, provider_city: String, provider_state: String, provider_zip: String, hospital_description: String, total_discharges: Double, avg_covered_charges: Double, avg_total_payments: Double, avg_medicare_payments: Double) extends Serializable

  case class PaymentwId(_id: String, drg_definition: String, provider_id: String, provider_name: String, provider_address: String, provider_city: String, provider_state: String, provider_zip: String, hospital_description: String, total_discharges: Double, avg_covered_charges: Double, avg_total_payments: Double, avg_medicare_payments: Double) extends Serializable

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
  

  def createPaymentwId(p: Payment): PaymentwId = {
    val id = pa.provider_id + '_' + pa.hospital_description + '_' + pa.drg_definition
    PaymentwId(id, pa.drg_definition, pa.provider_id, pa.provider_name, pa.provider_address, pa.provider_city, pa.provider_state, pa.provider_zip, pa.hospital_description, pa.total_discharges, pa.avg_covered_charges, pa.avg_total_payments, pa.avg_medicare_payments)
  }

  def main(args: Array[String]) {

    var pfile = "./data/payments.csv"
    var tableName: String = "/user/mapr/demo.mapr.com/tables/payments"

    if (args.length == 2) {
      pfile = args(0)
      tableName = args(1)
    } else {
      System.out.println("Using hard coded parameters unless you specify the consume import file and table. <file table>   ")
    }

    val spark: SparkSession = SparkSession.builder().appName("uber").master("local[*]").getOrCreate()
    val toDouble = udf[Double, String](_.toDouble)
    val df = spark.read.option("header", "true").csv(pfile)

    val df2 = df.withColumn("amount", toDouble(df("Total_Amount_of_Payment_USDollars")))
    df2.first
    df2.createOrReplaceTempView("payments")

    import spark.implicits._
    val ds: Dataset[Payment] = spark.sql("select Physician_Profile_ID as physician_id, Date_of_Payment as date_payment, Record_ID as record_id, Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer,  amount, Physician_Specialty, Nature_of_Payment_or_Transfer_of_Value as Nature_of_payment, Physician_First_Name as physician_name_first, Physician_Middle_Name as physician_name_middle, Physician_Last_Name as physician_name_last, Physician_Name_Suffix as physician_name_suffix, Recipient_City as recipient_city, Recipient_State as recipient_state, Recipient_Zip_Code as recipient_zip, Recipient_Country as recipient_country from payments").as[Payment]
    ds.cache
    ds.count
    ds.createOrReplaceTempView("payments")
    ds.show
    spark.sql("select physician_specialty, count(*) as cnt, sum(amount)as total from payments group by physician_specialty order by total desc").show()

    ds.filter($"amount" > 1000).show()
    ds.groupBy("Nature_of_payment").count().orderBy(desc("count")).show()

    val ds2: Dataset[PaymentwId] = ds.map(payment => createPaymentwId(payment))
    ds2.saveToMapRDB(tableName, createTable = false, idFieldPath = "_id")

    val pdf: Dataset[PaymentwId] = spark.sparkSession.loadFromMapRDB[PaymentwId](tableName, schema).as[PaymentwId]

    pdf.filter(_.physician_id == "214250").show

    pdf.select("_id", "physician_id").show

    pdf.groupBy("Nature_of_payment").count().orderBy(desc("count")).show()

  }
}

