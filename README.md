# Heathcare Data - Streaming ETL Pipeline and Data Exploration on IPPS Dataset

# Coming Soon!

cd /public_data/demos_healthcare/MapR-ES-DB-Spark-IPPS
java -cp /public_data/demos_healthcare/MapR-ES-DB-Spark-IPPS/target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer

$SPARK_PATH/bin/spark-submit --class streaming.SparkKafkaConsumer --master local[2] /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar

DRG Definition,Provider Id,Provider Name,Provider Street Address,Provider City,Provider State,Provider Zip Code,Hospital Referral Region Description, Total Discharges , Average Covered Charges , Average Total Payments ,Average Medicare Payments