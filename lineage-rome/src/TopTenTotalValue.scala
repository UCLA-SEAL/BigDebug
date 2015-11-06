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

    // Functions to use
    def verify(x: Double,y: Double)  = if (x==y) println("Result Verified")

    def splitID(s: String) = s.split(",")
    def splitMon(s:String) = s.split(",\\$")

    // Job
    val lines = lc.textFile(logFile)

    val result = lines.map(word => {
      val id = splitID(word)
      val tot = splitMon(word)
      (id(0).concat(" " + id(1)), (tot(1).toDouble + tot(2).toDouble))
      })


    result.collect.foreach(println)


    lc.setCaptureLineage(false)
    Thread.sleep(1000)




    // Lineage
    var linRDD = result.getLineage()
    //linRDD.collect.foreach(println)
    linRDD = linRDD.goBack()

    //linRDD.collect.foreach(println)



    //la line da dove proviene il risultato
    // per ogni linea da dove viene il risultato prendo il totale (ultimo campo della riga)
    // e lo verifico con il TOT della coppia (ID,TOT) da cui deriva
    // come faccio a recuparare quella coppia (ID,TOT) da cui deriva?

    linRDD.show().foreach(

        linea => verify(linea.split(",").last.replace("$", " ").trim().toDouble, 0.0)


    )




  }