import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._


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
    var lineage = true
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(lineage)
    // Job
    val lines = lc.textFile(logFile)

    // non so come si parametrizza, cioè mettere "word.split" fuori per farglielo fare solo una volta
    val result = lines.map(word =>(
      ( word.split(",")(0).concat(" " + word.split(",")(1)) ,
      (word.split(",\\$")(1).toDouble + word.split(",\\$")(2).toDouble)))
    )

    result.collect.foreach(println)

    var linRDD = result.getLineage() // GET LINEAGE NON PUò ESSERE INVOCATO SU MAPRDD
    linRDD.collect().foreach(println)
    linRDD.show()

  }
}