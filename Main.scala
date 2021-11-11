package bixiproject3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.SparkContext
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Properties

object Main extends App with Base {
  //-----------------------------------------------------------------------------------------------------------1)
  val fileDirSprint2 =
    "hdfs://quickstart.cloudera/user/hive/warehouse/enriched_station_information/"

  val spark = SparkSession
    .builder()
    .appName("Spark streaming sprint3")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

  val eStationInformationDF = spark.read
    .format("parquet")
    .option("inferschema", "true")
    .load(fileDirSprint2)
  eStationInformationDF.printSchema()
  //-----------------------------------------------------------------------------------------------------------2)
  val kafkaConfig = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "group-id-100_trips-records",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  val topicName = "$$$$_trip"

  val inStream: InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topicName), kafkaConfig))

  val trip: DStream[String] = inStream.map(_.value())

  import spark.implicits._

  trip.foreachRDD(microRDD => businessLogic(microRDD))

  ssc.start()
  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true, stopGracefully = true)

  def businessLogic(microRDD: RDD[String]): Unit = {
    val tripDF: DataFrame = microRDD
      .map(Trip(_))
      .toDF()

    val enrichedTripDF = tripDF
      .join(eStationInformationDF, tripDF("start_station_code") === eStationInformationDF("short_name"))
    // ---------------------------------------------------------------------------------------------------------3)
    val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
    val metadata = srClient.getSchemaMetadata("$$$$$$$$_enriched_trip-value", 1)
    val avroTripSchema = srClient.getByID(metadata.getId)

    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    props.setProperty("schema.registry.url", "http://localhost:8081")
    val producer = new KafkaProducer[String, GenericRecord](props)

    val enrichedTripArray = enrichedTripDF.collect()
    val avroEnrichedTrip: List[GenericRecord] = enrichedTripArray.map { fields =>
      new GenericRecordBuilder(avroTripSchema)
        .set("start_date", fields(0))
        .set("start_station_code", fields(1))
        .set("end_date", fields(2))
        .set("end_station_code", fields(3))
        .set("duration_sec", fields(4))
        .set("is_member", fields(5))
        .set("system_id", fields(6))
        .set("timezone", fields(7))
        .set("station_id", fields(8))
        .set("name", fields(9))
        .set("short_name", fields(10))
        .set("lat", fields(11))
        .set("lon", fields(12))
        .set("capacity", fields(13))
        .build()
    }.toList

    avroEnrichedTrip
      .foreach(line => {
        producer.send(new ProducerRecord[String, GenericRecord]("$$$$$$$$$4_enrichedtrip", line))
      })
    producer.flush()
  }
}
