import java.util.Properties

import akka.actor.{Actor, ActorRef, Props}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import java.util

import akka.Done

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.Try

class Stage2Actor(partitionId: Int) extends Actor {
  // Settings
  val bootstrapServers = ConfigHandler.getString("kafka-bootstrap-servers")
  val groupId = "consumers"
  val topicName = ConfigHandler.getString("kafka-topic-acitivity-stage-2")
  val consumerTimeout = 10
  val pollRecords = ConfigHandler.getInt("kafka-consumer-poll-records-stage-2")

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

  val helper = actorSystem.actorOf(Props[Stage2ConsumerActorHelper](
    new Stage2ConsumerActorHelper(self)),
    self.path.name + "_stage2_helper"
  )

  def receive = {
    case "more" => pollActionActivities
  }

  @tailrec
  private def pollActionActivities: Int = {
     consumer.poll(consumerTimeout) match {
       case records: ConsumerRecords[String, String] if records.count() == 0 => pollActionActivities
       case records: ConsumerRecords[String, String] => {
         consumeActionActivities(records)
         return 0
       }
     }
  }

  private def consumeActionActivities(records: ConsumerRecords[String, String]) = {
    var containers = ListBuffer[DispatchContainer]()
    records.forEach({ record: ConsumerRecord[String, String] =>
      containers += DispatchContainer.deserialize(record.value())
    })
    helper ! containers
  }

}

sealed class Stage2ConsumerActorHelper(parent: ActorRef) extends Actor {
  parent ! "more"
  def receive = {
    case containers: ListBuffer[DispatchContainerStage2] =>
      DatabaseWrapper.putDispatchableActivitiesAsync(containers.toList) { res: Try[Done] =>
        parent ! "more"
      }
  }
}

