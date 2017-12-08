import java.util.Properties

import akka.actor.{Actor, ActorRef, Props}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import java.util

import akka.Done
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.Try

class ConsumerActor(partitionId: Int) extends Actor {
  println("Actor starting " + self.path.name)
  // Settings
  val bootstrapServers = "localhost:9092"
  val groupId = "consumers"
  val topicName = "test2"
  val consumerTimeout = 0
  val pollRecords = 3

  val actorSystem = GlobalActorSystem.getActorSystem

  // ---------------------
  val  props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor")
  props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, pollRecords.toString)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

  val consumer = new KafkaConsumer[String, String](props)
  val partition = new TopicPartition(topicName, partitionId);
  consumer.assign(util.Collections.singletonList(partition))

  val helper = actorSystem.actorOf(Props[ConsumerActorHelper](
    new ConsumerActorHelper(self)),
    self.path.name + "_helper"
  )


  def pollKafkaRecords(consumer: KafkaConsumer[String, String]) = {
    val records= consumer.poll(consumerTimeout)
  }

  def receive = {
    case "more" => pollNewActivities
  }

  @tailrec
  private def pollNewActivities: Int = {
     consumer.poll(consumerTimeout) match {
       case records: ConsumerRecords[String, String] if records.count() == 0 => pollNewActivities
       case records: ConsumerRecords[String, String] => {
         consumeNewActivities(records)
         return 0
       }
     }
  }

  def consumeNewActivities(records: ConsumerRecords[String, String]) = {
    var containers = ListBuffer[KafkaActivityContainer]()
    for (record <- records.asScala){
      containers += KafkaActivityContainer.deserialize(record.value())
    }
    helper ! containers
  }

}


sealed class ConsumerActorHelper(parent: ActorRef) extends Actor {
  println("ConsumerActorHelper started")
  parent ! "more"
  def receive = {
    case containers: ListBuffer[KafkaActivityContainer] =>
      DatabaseWrapper.putActivitiesAsync(containers.toList) { res: Try[Done] =>
        parent ! "more"
      }
  }
}

