import java.util.UUID

import org.joda.time.DateTime

abstract class ActivityBase

case class Activity (actor: String,
                    verb: String = "post",
                    published: DateTime = DateTime.now,
                    obj: Option[String] = None,
                    target: Option[String] = None,
                    foreign_id: Option[String] = None,
                    id: UUID = UUID.randomUUID()) extends ActivityBase


object ActivityManager{

  def dispatch(feedowner: String, feedname: String, activity: Activity) = {
    DatabaseWrapper.putActivity(feedowner, feedname, activity)
  }

}