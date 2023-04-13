package log

trait Log extends Serializable {
  var logName: String
  def logString(date:String, hour:String, windowSize:Int): String
}

object LogFactory {
  def get(logClassName: String): Log = {

    val logName = s"log.${logClassName}"

    Class.forName(logName)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[Log]
  }

}
