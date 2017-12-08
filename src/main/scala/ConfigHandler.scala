import java.nio.file.Paths
import com.lambdista.config.Config
import scala.util.Try

object ConfigHandler {
  val configFile = sys.env("CONFIG")

  val config: Try[Config] = Config.from(Paths.get(configFile))

  def getString(key: String): String = {
    val confValue: Try[String] = for { c <- config; value <- c.getAs[String](key) } yield value
    return confValue.get
  }

  def getInt(key: String): Int = {
    val confValue: Try[Int] = for { c <- config; value <- c.getAs[Int](key) } yield value
    return confValue.get
  }

}
