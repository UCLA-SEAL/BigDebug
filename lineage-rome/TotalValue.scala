import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by filippo on 05/11/15.
  */

object TotalValue {

  def main (args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    var logFile = ""
    conf.setAppName("TopTen Total Value" + " - " + logFile)

    val sc = new SparkContext(conf)



    // Job

    val lines = sc.textFile(logFile, 2)
    val result = lines.filter(line => line.contains("congress"))
    println(result.count)
    //println(result.collect().mkString("\n"))

    lc.setCaptureLineage(false)
    Thread.sleep(10000)


}