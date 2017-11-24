import java.util.UUID

import org.joda.time.DateTime

case class Activity(actor: String,
                    verb: String = "post",
                    published: DateTime = DateTime.now,
                    obj: Option[String] = None,
                    target: Option[String] = None,
                    foreign_id: Option[String] = None) {
  val id = UUID.randomUUID()
  def dispatch(feedowner: String, to: String) = {
    DatabaseWrapper.putActivity(feedowner, to, this)
  }
}
