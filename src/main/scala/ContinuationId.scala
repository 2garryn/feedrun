import java.util.UUID

import org.joda.time.DateTime

import scala.util.{Success, Failure, Try}

trait ContinuationId {
  def toStringId: String
}


object ActivityContidParser {
  def parse(s: String): Try[ContinuationId] = {
    val splitted = s.split(";")
    Try({
      splitted(0) match {
        case "ActivityContId" => ActivityContId(splitted(1).toLong, UUID.fromString(splitted(2)))
        case "start" => ActivityContIdStart()
        case "stop" => ActivityContIdStop()
      }
    })
  }
}




case class ActivityContIdStart() extends ContinuationId { def toStringId: String = "start" }
case class ActivityContIdStop()  extends ContinuationId { def toStringId: String = "stop" }
case class ActivityContId(published: Long, activity_id: UUID) extends ContinuationId {
  def toStringId(): String = {
    this.getClass.getSimpleName + ";" + published.toString + ";" + activity_id.toString
  }
}