import org.json4s.jackson.Serialization
import org.json4s.DefaultFormats
import org.json4s.ext.JavaTypesSerializers



case class KafkaActivityContainer(actor: String, targetFeed: String, activity: Activity, followers: Seq[String]) {
  implicit val formats = DefaultFormats.preservingEmptyValues ++ JavaTypesSerializers.all
  def serialize(): String = Serialization.write(this)
}

object KafkaActivityContainer {
  implicit val formats = DefaultFormats.preservingEmptyValues ++ JavaTypesSerializers.all
  def deserialize(str: String): KafkaActivityContainer = Serialization.read[KafkaActivityContainer](str)
}