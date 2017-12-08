import org.joda.time.DateTime
import java.util.Properties
import akka.actor.Props
import org.apache.kafka.clients.producer._


object Feed extends App{

  implicit val system = GlobalActorSystem.getActorSystem
  implicit val materializer = GlobalActorSystem.getMaterializer
  system.actorOf(Props[ConsumerActor](new ConsumerActor(0)), "myactor0")
  system.actorOf(Props[ConsumerActor](new ConsumerActor(1)), "myactor1")


  val  props = new Properties()
  props.put("bootstrap.servers", "127.0.0.1:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner")
  val producer = new KafkaProducer[String, String](props)

  val activity = Activity("myactor1dd", "nothign", DateTime.now().getMillis)
  val container = KafkaActivityContainer("me", "allstuff", activity, Seq("user5qwe6", "user6qwe6", "user87qwe8", "user84qwe84"))

  val activity2 = Activity("myacdddtsd", "myaction", DateTime.now().getMillis)
  val container2 = KafkaActivityContainer("messfffs", "ohteaddsdrfeed", activity, Seq("user5qwe6", "user6qwe6", "user87qwe8", "user84qwe84"))

  producer.send(new ProducerRecord("test2", container.actor, container.serialize))
  producer.send(new ProducerRecord("test2", container2.actor, container2.serialize))
  producer.send(new ProducerRecord("test2", container2.actor, container2.serialize))
  producer.send(new ProducerRecord("test2", container2.actor, container2.serialize))
  producer.send(new ProducerRecord("test2", container2.actor, container2.serialize))

  producer.close()


  val ret = DatabaseWrapper.getActivities("user5qwe6", "allstuff", ActivityContIdStart(), 100)
  println(ret)


}
