import org.json4s.{DefaultFormats, FullTypeHints}
import org.json4s.ext.JavaTypesSerializers
import org.json4s.jackson.Serialization

trait DispatchContainer {
  implicit val formats = DefaultFormats.preservingEmptyValues.++(JavaTypesSerializers.all)
    .withTypeHintFieldName("actionClass")
    .+(FullTypeHints(List(classOf[DispatchContainer])))
  def serialize(): String = {
    Serialization.write(this)
  }
}

object DispatchContainer {
  implicit val formats = DefaultFormats.preservingEmptyValues.++(JavaTypesSerializers.all)
    .withTypeHintFieldName("actionClass")
    .+(FullTypeHints(List(classOf[DispatchContainer])))
  def deserialize(str: String): DispatchContainer = Serialization.read[DispatchContainer](str)
}

trait DispatchContainerStage2 extends DispatchContainer {
  val followers: Seq[String]
  val actor: String
  val targetFeed: String
  val activity: Activity
}

case class DispatchAddActivityStage1(actor: String, targetFeed: String, activity: Activity) extends DispatchContainer

case class DispatchAddActivityStage2(actor: String, targetFeed: String, activity: Activity, followers: Seq[String]) extends DispatchContainerStage2

case class DispatchDeleteActivityStage1(actor: String, targetFeed: String, activity: Activity) extends DispatchContainer

case class DispatchDeleteActivityStage2(actor: String, targetFeed: String, activity: Activity, followers: Seq[String]) extends DispatchContainerStage2



