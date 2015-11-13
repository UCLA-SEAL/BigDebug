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
    //lc.setCaptureLineage(lineage)


    def splitComma(s: String) = s.split(",")
    def splitMoney(s: String) = s.split(",\\$")

    // Job
    val lines = lc.textFile(logFile)

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

    val a = avged.collect()
    avged.foreach(println)
  }

}
