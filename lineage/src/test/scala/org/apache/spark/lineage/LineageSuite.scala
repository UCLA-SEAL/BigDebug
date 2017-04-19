/*
 * Grep workload for BigDataBench
 */
package org.apache.spark.lineage

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.util.collection.CompactBuffer
import org.scalatest.{FunSuite, Matchers}

class LineageSuite extends FunSuite with LocalLineageContext with Matchers {
  test("Grep") {
    val logFile = "lineage/src/test/resources/README.md"
    lc = new LineageContext(new SparkContext("local[2]", "grepTest"))

    lc.setCaptureLineage(true)

    // Job
    val lines = lc.textFile(logFile, 2)
    val result = lines.filter(line => line.contains("spark"))
    assert(result.collect().size == 11)

    lc.setCaptureLineage(false)

    // Trace backward one record
    var linRdd = result.getLineage()
    linRdd = linRdd.filter(_ == 0L)
    assert(linRdd.collect()(0) == (0, (0, 9)))
    linRdd = linRdd.goBack()
    assert(linRdd.collect()(0) == ((0, 9), 0))
    assert(linRdd.show.collect()(0) == "<http://spark.apache.org/>")

    // Trace forward one record
    linRdd = linRdd.goNext()
    assert(linRdd.collect()(0) == (0, 9))

  }

  test("WordCount") {
    val logFile = "lineage/src/test/resources/README.md"
    lc = new LineageContext(new SparkContext("local[2]", "wordcountTest"))

    lc.setCaptureLineage(true)

    // Job
    val file = lc.textFile(logFile, 2)
    val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), 1))
    val result = pairs.reduceByKey(_ + _)
    assert(result.collectWithId().size == 268)

    lc.setCaptureLineage(false)

    // Trace backward one record
    var linRdd = result.getLineage()
    linRdd = linRdd.filter(_ == 6232546377L)
    var id = new CompactBuffer[Long]
    id += 4294967296L
    assert(linRdd.collect()(0) == (6232546377L,(id,-2129110187)))
    assert(linRdd.show().collect()(0) == "((examples,2),-2129110187)")
    linRdd = linRdd.goBack()
    linRdd.collect() should equal (Array(((1,-2129110187),(0,1)), ((1,-2129110187),(0,2))))
    linRdd = linRdd.goBack()
    linRdd.collect() should equal (Array(((0,1),1933), ((0,2),2009)))
    linRdd.show().collect() should equal (Array(
      "You can set the MASTER environment variable when running examples to submit",
      "examples to a cluster. This can be a mesos:// or spark:// URL, "
    ))

    // Trace forward one record
    linRdd = linRdd.goNext()
    linRdd.collect() should equal (Array(((1,-1853890486),1), ((1,-879550375),1), ((1,-1809307060),1),
      ((1,736856288),1), ((1,192179771),2), ((1,-1147111243),1), ((1,-2129110187),1),
      ((1,-2129110187),2), ((1,2107033230),1), ((1,-1744079448),2), ((1,1867108634),2),
      ((1,-912696889),1), ((1,408741114),2), ((1,1304127987),1), ((1,1304127987),2),
      ((1,-144610220),2), ((1,590131778),1), ((1,1202785691),2), ((1,-2118185461),2),
      ((1,1156150772),1), ((1,1156150772),2), ((1,-801702228),2), ((1,1030193925),1)))
    linRdd.show().collect()  should equal (Array("((submit,1),2107033230)", "((This,1),-144610220)",
      "((cluster.,1),-801702228)", "((be,2),408741114)", "((You,2),-1809307060)",
      "((can,4),1156150772)", "((set,1),-1853890486)", "((a,5),1867108634)",
      "((MASTER,1),736856288)", "((variable,1),590131778)", "((spark://,1),-1744079448)",
      "((to,11),1304127987)", "((running,1),-879550375)", "((URL,,1),1202785691)",
      "((environment,1),1030193925)", "((when,1),-912696889)", "((or,3),-2118185461)",
      "((mesos://,1),192179771)", "((the,12),-1147111243)", "((examples,2),-2129110187)"
    ))
    linRdd = linRdd.goNext()
    linRdd.collect()  should equal (Array(((0,1265053044L),-801702228), ((0,2605758L),-144610220),
      ((0,2267029090L),736856288), ((0,98256L),1156150772), ((0,3139L),408741114),
      ((0,2061443997L),-1744079448), ((0,3045380732L),590131778), ((0,3403431960L),2107033230),
      ((0,89087L),-1809307060), ((0,113762L),-1853890486), ((0,97L),1867108634),
      ((0,8124637755L),192179771), ((0,4295082097L),-1147111243), ((0,4298615610L),-912696889),
      ((0,8504029715L),1030193925), ((0,4297580733L),1202785691), ((0,6232546377L),-2129110187),
      ((0,5845751231L),-879550375), ((0,4294971003L),1304127987), ((0,4294970851L),-2118185461)
    ))
    linRdd.show().collect() should equal (Array("((cluster.,1),-801702228)","((This,2),-144610220)",
      "((MASTER,1),736856288)", "((can,6),1156150772)", "((be,2),408741114)",
      "((spark://,1),-1744079448)", "((variable,1),590131778)", "((submit,1),2107033230)",
      "((You,3),-1809307060)", "((set,2),-1853890486)", "((a,9),1867108634)",
      "((mesos://,1),192179771)", "((the,21),-1147111243)", "((when,1),-912696889)",
      "((environment,1),1030193925)", "((URL,,1),1202785691)", "((examples,2),-2129110187)",
      "((running,1),-879550375)", "((to,14),1304127987)", "((or,3),-2118185461)"
    ))
  }

  test("AverageLengthError") {
    val logFile = "lineage/src/test/resources/README.md"
    lc = new LineageContext(new SparkContext("local[2]", "AverageLengthErrorTest"))

    lc.setCaptureLineage(true)

    // Job
    val file = lc.textFile(logFile, 2)
    val wordCount = file.flatMap(line => line.trim().split(" ")).filter(_.size > 0)
      .map(word => (word.substring(0, 1), word.length))
      .groupByKey()
      .map(pair => {
      val itr = pair._2.toIterator
      var returnedValue = 0
      var size = 0
      while (itr.hasNext) {
        val num = itr.next()
        returnedValue += num
        size += 1
      }
      (pair._1, returnedValue / size)
    })
      //this map marks the faulty result
      .map(pair => {
      var value = pair._2.toString
      if (pair._2 > 70) {
        value += "*"
      }
      (pair._1, value)
    })

    val result = wordCount.collectWithId()

    lc.setCaptureLineage(false)

    assert(result.size == 51)

    //get the list of lineage id
    var list = List[Long]()
    for (o <- result) {
      if (o._1._2.substring(o._1._2.length - 1).equals("*")) {
        list = o._2 :: list
      }
    }
    assert(list(0) == 19L)

    // Trace backward one record
    var linRdd = wordCount.getLineage()
    linRdd = linRdd.filter(s => list.contains(s))
    assert(linRdd.collect()(0) == (19,(0,86)))
    linRdd = linRdd.goBack()
    assert(linRdd.collect()(0) == ((0,86), 19))
    assert(linRdd.show().collect()(0) == "((V,CompactBuffer(109, 8, 101)),362254112)")
    linRdd = linRdd.goBack()
    linRdd.collect() should equal (Array(((0,362254112),(0,59)), ((1,362254112),(1,22)),
      ((1,362254112),(1,29))))
    linRdd.show().collect() should equal (Array("((V,109),362254112)", "((V,8),362254112)",
      "((V,101),362254112)"))
    linRdd = linRdd.goBack()
    linRdd.collect() should equal (Array(((0,59),1822), ((0,22),2756), ((0,29),3066)))
    linRdd.show().collect() should equal (Array(
      "Vfasonfdsojfanjfosanfsaojfjasoidasjmdosjaoidjsaiodjasojoiasnvjdsfnasoidjsaiodjasoidjaoisdjsaonsajdjasiodjsaoi",
      "## A Note About Hadoop Versions",
      "[\"Specifying the Hadoop Version\"](http://spark.apache.org/docs/latest/building-with-maven.html#specifying-the-hadoop-version)"))

    // Trace forward one record
    linRdd = linRdd.goNext()
    linRdd.collect() should equal (Array(((0,362254112),59), ((1,975088374),29), ((1,965664971),22),
      ((1,965664971),29), ((1,1521554037),22), ((1,-1404379293),29), ((1,-1727041554),22),
      ((1,362254112),22), ((1,362254112),29), ((1,814537616),22)))
    linRdd.show().collect() should equal (Array("((V,109),362254112)", "((#,2),-1727041554)",
      "((A,1),814537616)", "((A,5),814537616)", "((V,8),362254112)", "((t,3),975088374)",
      "((V,101),362254112)", "((N,4),1521554037)", "((H,6),965664971)", "(([,12),-1404379293)"))
    linRdd = linRdd.goNext()
    linRdd.collect() should equal (Array(((0,116),975088374), ((0,65),814537616),
      ((0,35),-1727041554), ((0,86),362254112), ((0,72),965664971), ((0,91),-1404379293),
      ((0,78),1521554037)))
    linRdd.show().collect() should equal (Array(
      "((t,CompactBuffer(4, 5, 3, 3, 2, 4, 3, 2, 7, 3, 3, 2, 3, 3, 3, 5, 3, 3, 2, 2, 2, 2, 7, 2, 8, 3, 3, 3, 5, 3, 2, 117, 3, 2, 4, 2, 3, 3, 4, 2, 3, 3, 4, 2, 3, 3, 2)),975088374)",
      "((A,CompactBuffer(6, 4, 14, 3, 1, 5)),814537616)",
      "((#,CompactBuffer(1, 2, 2, 2, 2, 2, 2, 2, 2)),-1727041554)",
      "((V,CompactBuffer(109, 8, 101)),362254112)",
      "((H,CompactBuffer(6, 6, 4, 16, 7, 6, 7, 4, 4, 6)),965664971)",
      "(([,CompactBuffer(8, 8, 7, 10, 10, 9, 4, 12, 7, 14)),-1404379293)",
      "((N,CompactBuffer(1, 4)),1521554037)"))
  }
}