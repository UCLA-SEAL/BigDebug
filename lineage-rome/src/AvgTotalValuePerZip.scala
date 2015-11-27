import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Thor on 13/11/15.
  */
object AvgTotalValuePerZip {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[2]")
    var logFile = "./inputs/City_Of_Trenton_-_2015_Certified_Tax_List.csv"
    conf.setAppName("AvgTotalValuePerZip" + " - " + logFile)

    val sc = new SparkContext(conf)
    var lineage = true
    val lc = new LineageContext(sc)
    lc.setCaptureLineage(lineage)


    //FUNCTIONS

    def splitComma(s: String) = s.split(",")
    def splitMoney(s: String) = s.split(",\\$")
    def getTot(s: String) = s.split(",").last.replace("$", " ").trim().toDouble

    def verify(id: String, x: Double, y: Double) : Boolean = {
      if (x == y) {
        println("Result Verified")
        return true
      }
      else {
        println("ERROR on (" + id + "). value is: " + x + " avg_expected: " + y)
        return false
      }
    }





    // JOB
    val lines = lc.textFile(logFile)
    var jobMap:Map[String,Double] = Map()
    var linMap:Map[String,Double] = Map()


    val result = lines.map(word => {
      val fields = splitComma(word)
      val tot = splitMoney(word)
      (fields(0).concat(" " + fields(1)), fields(10), tot(1).toDouble + tot(2).toDouble)
    })

    var grouped = result.groupBy(r => r._2)

    val avged = grouped.map(_$1 => {

      val it = _$1._2.iterator
      var c = 0
      var t = 0.0
      while (it.hasNext) {
        var triple = it.next()
        c = c + 1
        t = t + triple._3
      }
      (_$1._1, t/c)
    })


    avged.collect().foreach( avg => {
      jobMap += (avg._1 -> avg._2)
    })


    lc.setCaptureLineage(false)
    Thread.sleep(1000)



    //LINEAGE

    var linRDD = avged.getLineage()
    linRDD = linRDD.goBack()

    val linAVG = linRDD.show().groupBy(r => r.split(",")(0)).map(_$1 => {

      val it = _$1._2.iterator
      var c = 0
      var t = 0.0
      while (it.hasNext) {
        var triple = it.next()
        c = c + 1
        t = t + triple.split(",").last.replaceAll("\\)", "").trim.toDouble

      }
      (_$1._1, t/c)
    })
      .collect().foreach(lin => {

      val zip = lin._1.replaceAll("\\(", "")
      val tot = lin._2
      linMap += (zip -> tot)
      println(lin)

    })

    Thread.sleep(10000)

    //VERIFY
    var errors : Int = 0
    var c : Int = 0
    var errorRate : Double = 0.0

    jobMap.keys.foreach( kJ => {
      linMap.keys.foreach(kL =>
        if(kJ.equals(kL)){
          c = c + 1
          val totJ = jobMap(kJ)
          val totLin = linMap(kL)
          if (verify(kL,totJ, totLin) == false ) errors = errors + 1
        }
      )
    })




    errorRate = errors.toDouble/c.toDouble * 100
    println("The error rate is " + errorRate + "%")






    // END

  }
}
