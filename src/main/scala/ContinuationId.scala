import java.util.UUID
import org.joda.time.DateTime

trait ContinuationId {
  def toStringId: String
}


object ActivityContidParser {
  def parse(s: String): Option[ContinuationId] = {
    try {
      val splitted = s.split(";")
      splitted(0) match {
        case "ActivityContId" => Some(ActivityContId(new DateTime(splitted(1).toLong), UUID.fromString(splitted(2))))
        case "start" => Some(ActivityContIdStart())
        case "stop" => Some(ActivityContIdStop())
        case _ => None
      }
    } catch {
      case e: Exception => None
    }
  }
}



case class ActivityContIdStart() extends ContinuationId { def toStringId: String = "start" }
case class ActivityContIdStop()  extends ContinuationId { def toStringId: String = "stop" }
case class ActivityContId(published: DateTime, activity_id: UUID) extends ContinuationId {
  def toStringId(): String = {
    this.getClass.getSimpleName + ";" + published.getMillis.toString + ";" + activity_id.toString
  }
}