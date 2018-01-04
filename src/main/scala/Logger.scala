import com.typesafe.scalalogging

object Logger {
  private val logger = scalalogging.Logger("Feed")

  def log = logger

  def dbg(l: Any) = {
    print("DBG: ")
    println(l)
  }

}
