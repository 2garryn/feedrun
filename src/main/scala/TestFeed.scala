import org.joda.time.DateTime


object TestFeed extends App{
  val activity = Activity("sergey", "like", DateTime.now().getMillis)
  Feed.init

 // putFollow()
  //Feed.dispatchActivity("sergey", "wowfeed", activity)
  //val ret = DatabaseWrapper.getActivities("user_10", "wowfeed", ActivityContIdStart(), 100)

 // println(ret.activities)

  //Feed.undispatchActivity("sergey", "wowfeed", ret.activities(0))

  Feed.follow("garry", "hermiona")




  /*
  ret.activities.foreach({
    act: Activity =>
      Feed.deleteDispatchActivity("sergey", "wowfeed", act)
  })

*/




  def putFollow() = {
    List.range(1, 1000).foreach({n: Int =>

      Feed.follow("user_" + n.toString, "sergey")
    })


  }

  //val ret = DatabaseWrapper.getActivities("user5qwe6", "allstuff", ActivityContIdStart(), 1)
  //println(ActivityContidParser.parse("ActivityContId;12312312sd3;274a5ddd38a-4c1a-4fcf-b3d1-d9bde4a07546"))


  /*
  val activity2 = Activity("sergey", "myaction", DateTime.now().getMillis)
  val container = new DispatchAddActivityStage1("me", "testfeed", activity2)
  val con2 = new DispatchAddActivityStage2("me", "testfeed", activity2, Seq("asdad"))

  val serialized = container.serialize()
  val ser2 = con2.serialize()

  println(serialized)
  println(ser2)


  val des = DispatchContainer.deserialize(serialized)
  println(des)

*/
  /*

  implicit val system = GlobalActorSystem.getActorSystem
  implicit val materializer = GlobalActorSystem.getMaterializer
  system.actorOf(Props[ConsumerActor](new ConsumerActor(0)), "myactor0")
  system.actorOf(Props[ConsumerActor](new ConsumerActor(1)), "myactor1")

  DatabaseWrapper.putFollow("artem", "sergey")
  DatabaseWrapper.putFollow("sabya", "sergey")
  DatabaseWrapper.putFollow("igor", "sergey")

  val activity2 = Activity("sergey", "myaction", DateTime.now().getMillis)
  DispatchableFeed.dispatchFeed("sergey", "newonefeed", activity2)

  val ret = DatabaseWrapper.getActivities("artem", "newonefeed", ActivityContIdStart(), 100)
  println(ret)

  DatabaseWrapper.mapOverFollowers("sergey") {
    for(fol <- _) {
      println(fol)
    }
  }
  */


  /*
  implicit val system = GlobalActorSystem.getActorSystem
  implicit val materializer = GlobalActorSystem.getMaterializer
  system.actorOf(Props[Stage1Actor](new Stage1Actor(0)), "myactor0")


  val  props = new Properties()
  props.put("bootstrap.servers", ConfigHandler.getString("kafka-bootstrap-servers"))
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner")
  val topicName = ConfigHandler.getString("kafka-topic-acitivity-stage-1")

  val producer = new KafkaProducer[String, String](props)
  println("COUNT OF PARTS", producer.partitionsFor(topicName).size())

  val activity = Activity("sergey", "nothign", DateTime.now().getMillis)
  val c2 = DispatchAddActivityStage1("sergey", "asdasd", activity)

  producer.send(new ProducerRecord(topicName, c2.serialize))

  producer.close()
  /*

*/
producer.send(new ProducerRecord(topicName, "asssd", container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container.serialize))
producer.send(new ProducerRecord(topicName,  container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container.serialize))
producer.send(new ProducerRecord(topicName,  container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container.serialize))
producer.send(new ProducerRecord(topicName,  container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))
producer.send(new ProducerRecord(topicName, container2.serialize))

producer.close()


val ret = DatabaseWrapper.getActivities("user5qwe6", "allstuff", ActivityContIdStart(), 100)
println(ret)
*/

}
