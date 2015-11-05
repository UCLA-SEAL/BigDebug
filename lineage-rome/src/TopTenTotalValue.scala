import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by filippo on 05/11/15.
  */

object TopTenTotalValue {

  def main (args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    var logFile = "./inputs/City_Of_Trenton_-_2015_Certified_Tax_List.csv" // args[0]
    conf.setAppName("TopTenTotalValue" + " - " + logFile)

    val sc = new SparkContext(conf)

    // Job

    val lines = sc.textFile(logFile)
    val result = lines.flatMap(line => line.split(",")).map(word => (word.trim(), 1))
    println(result.count)
    // println(result.collect().mkString("\n"))


  }
}