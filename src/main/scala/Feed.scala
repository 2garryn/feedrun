
import akka.actor.Props

object Feed{
  implicit val system = GlobalActorSystem.getActorSystem

  def init = {
    Logger.log.info("Starting consumer actors...")
    startStage2Actors()
    startStage1Actors()
  }


  def dispatchActivity(actor: String, feed: String, activity: Activity) = {
    val item = DispatchAddActivityStage1(actor, feed, activity)
    Stage1Producer.send(item)
  }

  def undispatchActivity(actor: String, feed: String, activity: Activity) = {
    val item = DispatchDeleteActivityStage1(actor, feed, activity)
    Stage1Producer.send(item)
  }



  def putActivity(username: String, feed: String, activity: Activity) = {
    DatabaseWrapper.putActivity(username, feed, activity)
  }

  def follow(follower: String, following: String) = {
    DatabaseWrapper.follow(follower, following)
  }

  def unfollow(follower: String, following: String) = {
    DatabaseWrapper.unfollow(follower, following)
  }


  private def startStage2Actors() = {
    List.range(0, Stage2Producer.partitionsN).foreach({ n: Int =>
      Logger.log.info("Starting stage2 consumer actor: " + n.toString)
      system.actorOf(Props[Stage2Actor](new Stage2Actor(n)), "stage2actor_" + n.toString)
    })
  }

  private def startStage1Actors() = {
    List.range(0, Stage1Producer.partitionsN).foreach({ n: Int =>
      Logger.log.info("Starting stage1 consumer actor: " + n.toString)
      system.actorOf(Props[Stage1Actor](new Stage1Actor(n)), "stage1actor_" + n.toString)
    })
  }


}
