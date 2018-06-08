package streaming

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._

import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.streaming.dstream._

import org.apache.spark.streaming.kafka09.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import scala.util.Try

/*
Spark streaming consumer 
consumes from MapR Event Streams transforms CSV to JSON and write to MapR-DB JSON
*/
object SparkKafkaConsumer {

  /*
   *position of values in csv
0 DRG Definition as drg_definition,
1 Provider Id as provider_id,
2 Provider Name as provider_name,
3 Provider Street Address as provider_address,
4 Provider City as provider_city,
5 Provider State as provider_state,
6 Provider Zip Code as provider_zip,
7 Hospital Referral Region Description as hospital_description,
8 Total Discharges as total_discharges,
9 Average Covered Charges as avg_covered_charges,
10 Average Total Payments as avg_total_payments,
11 Average Medicare Payments as avg_medicare_payments
*/
def parseDouble(str: String): Double = {
    val str2 = str.replaceAll("\\$", "")
    Try(str2.toDouble) getOrElse 0.0
  }
  // get drg code from string
  def parseDrgCode(str: String): String = {
    val d = str.split("-")
    d(0).trim
  }
  // get drg description from string
  def parseDrgD(str: String): String = {
    val d = str.split("-")
    d(1).trim
  }

  case class Payment(drg_code: String, drg_definition: String, provider_id: String, provider_name: String, provider_address: String, provider_city: String, provider_state: String, provider_zip: String, provider_region: String, total_discharges: Double, avg_covered_charges: Double, avg_total_payments: Double, avg_medicare_payments: Double) extends Serializable

   case class PaymentwId(_id: String, drg_code: String, drg_definition: String, provider_id: String, provider_name: String, provider_address: String, provider_city: String, provider_state: String, provider_zip: String, provider_region: String, total_discharges: Double, avg_covered_charges: Double, avg_total_payments: Double, avg_medicare_payments: Double) extends Serializable

  def parsePayment(str: String): Payment = {
      val td = str.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
    Payment(parseDrgCode(td(0)),parseDrgD(td(0)), td(1),td(2).replaceAll("\"", ""), td(3),td(4),td(5), td(6),td(7),
            parseDouble(td(8)) ,
            parseDouble(td(9)) ,
            parseDouble(td(10)) ,
            parseDouble(td(11)) )
  }

  def parsePaymentwID(str: String): PaymentwId = {
    val pa = parsePayment(str)
    val id = pa.provider_id + '_' + pa.drg_code
    PaymentwId(id, pa.drg_code, pa.drg_definition, pa.provider_id, pa.provider_name, pa.provider_address, pa.provider_city, pa.provider_state, pa.provider_zip, pa.provider_region, pa.total_discharges, pa.avg_covered_charges, pa.avg_total_payments, pa.avg_medicare_payments)
  }
  
  def main(args: Array[String]) = {
    var tableName: String = "/user/mapr/demo.mapr.com/tables/payments"
    var topicc: String = "/user/mapr/demo.mapr.com/streams/paystream:payments"

    if (args.length == 2) {
      topicc = args(0)
      tableName = args(1)
    } else {
      System.out.println("Using hard coded parameters unless you specify the consume topic and table. <topic table>   ")
    }

    val groupId = "testgroup"
    val offsetReset = "earliest" //  "latest"
    val pollTimeout = "5000"

    val brokers = "${MAPR_CLUSTER}:9092" // not needed for MapR Streams, needed for Kafka

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaConsumer.getClass.getName).setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.sparkContext.setLogLevel("ERROR")
    val topicsSet = topicc.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )

    val valuesDStream: DStream[String] = messagesDStream.map(_.value())

    val pDStream: DStream[PaymentwId] = valuesDStream.map(parsePaymentwID)

    pDStream.print(3)
    pDStream.saveToMapRDB(tableName, createTable = false, bulkInsert = true, idFieldPath = "_id")

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}