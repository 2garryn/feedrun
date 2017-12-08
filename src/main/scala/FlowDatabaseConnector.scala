import java.util.Properties

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorSystem}
import akka.actor.FSM.Failure
import akka.actor.Status.Success
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cassandra.scaladsl.CassandraSource
import akka.stream.scaladsl.{Flow, Sink}
import com.datastax.driver.core.{Cluster, ResultSet, Row, SimpleStatement}
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object FlowDatabaseConnector {
  implicit val system = ActorSystem("MyActorSystem")
  implicit val materializer = ActorMaterializer()
  implicit val session = Cluster.builder
    .addContactPoint("127.0.0.1").withPort(32769)
    .build.connect()

 // val flow - Flow[Row, NotUsed]


  val printSink = Sink.foreach[Vector[Row]](println)
  DatabaseConnect.initialize("mybase2")
  val ec =  scala.concurrent.ExecutionContext.Implicits.global

  def go() = {
    val stmt = new SimpleStatement("SELECT * FROM mybase2.follow LIMIT 1000").setFetchSize(50)
    val source = CassandraSource(stmt)
     .grouped(50)
     .mapAsyncUnordered(10)({
        batch => Future {
          println("Proccessin batch ", batch.size)
        }
     }).runWith(Sink.ignore)

  }

  def simple() = {
    val l = "SELECT * FROM activities LIMIT 100"
    println(DatabaseConnect.query(l).all())
  }

}



