import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

object GlobalActorSystem {
  implicit val system = ActorSystem("mySystem")
  implicit val materializer = ActorMaterializer()

  def getMaterializer = materializer
  def getActorSystem = system

}
