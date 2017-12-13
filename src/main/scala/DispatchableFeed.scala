import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object DispatchableFeed {
  val  props = new Properties()
  props.put("bootstrap.servers", ConfigHandler.getString("kafka-bootstrap-servers"))
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner")

  val topicNameStage2 = ConfigHandler.getString("kafka-topic-acitivity-stage-2")

  def dispatchFeed(actor: String, targetFeed: String, activity: Activity) = {
    val producer = new KafkaProducer[String, String](props)

    DatabaseWrapper.mapOverFollowers(actor) { followersBatch: Seq[String] =>
      val serCont = KafkaActivityContainer(actor, targetFeed, activity, followersBatch).serialize()
      producer.send(new ProducerRecord(topicNameStage2, serCont))
    }


  }

}
