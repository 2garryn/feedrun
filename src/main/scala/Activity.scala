import java.util.UUID

import org.joda.time.DateTime

case class Activity(actor: String,
                    verb: String = "post",
                    published: DateTime = DateTime.now,
                    obj: Option[String] = None,
                    target: Option[String] = None,
                    foreign_id: Option[String] = None,
                    id: UUID = UUID.randomUUID())


object ActivityManager{

  def dispatch(feedowner: String, feedname: String, activity: Activity) = {
    DatabaseWrapper.putActivity(feedowner, feedname, activity)
  }


  //def get(feedowner: String, feedname: String, )
}