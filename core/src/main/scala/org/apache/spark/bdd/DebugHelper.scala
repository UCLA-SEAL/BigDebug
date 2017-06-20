package org.apache.spark.bdd

/** BDD START **/
object DebugHelper {

  // TODO: A CULPRIT OF PERFORMANCE DEGRADATION
  def deepCopy[A](a: A)(implicit m: reflect.Manifest[A]): A =
    util.Marshal.load[A](util.Marshal.dump(a))

  val escapeCode = char2Character(27) + "["

  val COLOR_LIGHT_GREEN = escapeCode + "1;32m"
  val COLOR_LIGHT_RED = escapeCode + "1;31m"
  val COLOR_LIGHT_WHITE = escapeCode + "1;37m"
  val COLOR_DARK_WHITE = escapeCode + "0;37m"
  val COLOR_ORIGINAL = escapeCode + "0m"

  def log(msgType: String, msgTopic: String, x: Any) = {

    val colorCode: String = msgType match {
      case "INFO" => COLOR_LIGHT_GREEN
      case "WARNING" => escapeCode + "1;33m" // TODO: do not use magic number here
      case "ERROR" => escapeCode + "1;31m"
      case "EXCEPTION" => escapeCode + "1;35m"
      case "VERBOSE" => COLOR_LIGHT_WHITE
      case _ => escapeCode + "1;37m"
    }
    val darkerColorCode: String = msgType match {
      case "INFO" => escapeCode + "0;32m"
      case "WARNING" => escapeCode + "0;33m"
      case "ERROR" => escapeCode + "0;31m"
      case "EXCEPTION" => escapeCode + "0;35m"
      case "VERBOSE" => COLOR_DARK_WHITE
      case _ => escapeCode + "0;37m"
    }
   // scala.Console.print(colorCode + "[" + msgType + "][" + msgTopic + "] " + darkerColorCode)
    //scala.Console.println(x)
    //scala.Console.print(escapeCode + "0m")
  }
  def logoutput(msgType: String, msgTopic: String, x: Any) = {

    val colorCode: String = msgType match {
      case "INFO" => COLOR_LIGHT_GREEN
      case "WARNING" => escapeCode + "1;33m" // TODO: do not use magic number here
      case "ERROR" => escapeCode + "1;31m"
      case "EXCEPTION" => escapeCode + "1;35m"
      case "VERBOSE" => COLOR_LIGHT_WHITE
      case _ => escapeCode + "1;37m"
    }
    val darkerColorCode: String = msgType match {
      case "INFO" => escapeCode + "0;32m"
      case "WARNING" => escapeCode + "0;33m"
      case "ERROR" => escapeCode + "0;31m"
      case "EXCEPTION" => escapeCode + "0;35m"
      case "VERBOSE" => COLOR_DARK_WHITE
      case _ => escapeCode + "0;37m"
    }
    // scala.Console.print(colorCode + "[" + msgType + "][" + msgTopic + "] " + darkerColorCode)
    //scala.Console.println(x)
    //scala.Console.print(escapeCode + "0m")
  }
}

/** BDD END **/