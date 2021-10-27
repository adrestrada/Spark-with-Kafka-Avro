package bixiproject3

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object KafkaProducer extends Base with App {

  val topicName = "bdsf2001_adriest_trip"

  val producerProperties = new Properties()
  producerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](producerProperties)

  val dataSource = "C:/Users/Valeria/BixiData100_Trips/100_trips.csv"

  val topicMessage = Source.fromFile(dataSource)
  topicMessage
    .getLines()
    .slice(1, 100)
    .foreach(line => {
      producer.send(new ProducerRecord[String, String](topicName, line))
    })
  producer.flush()
  producer.close()
}
