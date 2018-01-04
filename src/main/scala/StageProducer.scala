import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object StageProducer {
  val  producerProps = new Properties()
  producerProps.put("bootstrap.servers", ConfigHandler.getString("kafka-bootstrap-servers"))
  producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner")
  val producer = new KafkaProducer[String, String](producerProps)

  def send(stageContainer: DispatchContainer, topicName: String) =
    producer.send(new ProducerRecord(topicName, stageContainer.serialize()))

  def partitionsN(topicName: String) = producer.partitionsFor(topicName).size()
}

trait StageNProducer {
  val topicName: String
  def send(stageContainer: DispatchContainer) = StageProducer.send(stageContainer, topicName)
  def partitionsN() = StageProducer.partitionsN(topicName)
}

object Stage2Producer extends StageNProducer {
  val topicName = ConfigHandler.getString("kafka-topic-acitivity-stage-2")
}

object Stage1Producer extends StageNProducer {
  val topicName = ConfigHandler.getString("kafka-topic-acitivity-stage-1")
}

