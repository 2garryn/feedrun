import java.util
import java.util.Properties
import akka.actor.{Actor, ActorRef, Props}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer


class Stage1Actor(partitionId: Int) extends Actor {
  val actorSystem = GlobalActorSystem.getActorSystem

  // Consumer stage 1
  val consumerGroupId = "stage1consumers"
  val consumerTopicName = ConfigHandler.getString("kafka-topic-acitivity-stage-1")
  val consumerTimeout = 0
  val consumerPollRecords = ConfigHandler.getInt("kafka-consumer-poll-records-stage-1")

  // Consumer stage 1 props init
  val  consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigHandler.getString("kafka-bootstrap-servers"))
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor")
  consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerPollRecords.toString)
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)


  val consumer = new KafkaConsumer[String, String](consumerProps)
  val consumerPartition = new TopicPartition(consumerTopicName, partitionId);
  consumer.assign(util.Collections.singletonList(consumerPartition))


  val helper = actorSystem.actorOf(Props[Stage1ConsumerActorHelper](
    new Stage1ConsumerActorHelper(self)),
    self.path.name + "_stage1_helper"
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

sealed class Stage1ConsumerActorHelper(parent: ActorRef) extends Actor {
  Logger.log.info("Stage1ConsumerActorHelper started")
  parent ! "more"
  def receive = {
    case containers: ListBuffer[DispatchAddActivityStage1] => {
      Logger.dbg("Stage1ConsumerActorHelper got new chunks: " + containers.size)
      processChunks(containers.toList) {() =>
        parent ! "more"
      }
    }
  }

  def processChunks(stage1Acts: List[DispatchAddActivityStage1]) (done: () => Unit): Unit =
    stage1Acts match {
      case act :: tail => {
        DatabaseWrapper.mapOverFollowers(act.actor) {followers: Seq[String] =>
          val cont = DispatchAddActivityStage2(act.actor, act.targetFeed, act.activity, followers)
          Stage2Producer.send(cont)
        } {
          () => processChunks(tail) (done)
        }
      }
      case Nil =>
        done()

  }
}
