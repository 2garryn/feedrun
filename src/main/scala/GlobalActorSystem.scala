import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

/**
  * Created by artemgolovinskij on 08/12/2017.
  */
object GlobalActorSystem {
  implicit val system = ActorSystem("mySystem")
  implicit val materializer = ActorMaterializer()

  def getMaterializer = materializer
  def getActorSystem = system

}
