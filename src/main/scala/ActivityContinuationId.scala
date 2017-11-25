import java.util.UUID

import org.joda.time.DateTime


trait ContinuationId {
  def toStringId: Option[String]
  def fromStringId(id: String): Option[ContinuationId]
}



case class ActivityContId(published: Option[DateTime], activity_id: Option[UUID]) extends ContinuationId{

  def this() = this(None, None)
  def this(published: DateTime, activity_id: UUID) = this(Some(published), Some(activity_id))

  def toStringId(): Option[String] = {
    (published, activity_id) match {
      case (None, None) => None
      case (Some(p: DateTime), Some(a: UUID)) => {
        Some(p.getMillis.toString + ";" + a.toString)
      }
    }
  }

  def fromStringId(id: String): Option[ActivityContId] = {
    try {
      val splitted = id.split(";")
      val publ = new DateTime(splitted(0).toLong)
      val uuid = UUID.fromString(splitted(1))
      Some(new ActivityContId(publ, uuid))
    } catch {
      case e: Exception => None
    }
  }
}