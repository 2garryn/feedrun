import java.util.UUID

abstract class ActivityBase

case class Activity (actor: String,
                    verb: String = "post",
                    published: Long,
                    obj: Option[String] = None,
                    target: Option[String] = None,
                    foreign_id: Option[String] = None,
                    id: UUID = UUID.randomUUID()) extends ActivityBase
