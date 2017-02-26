package org.apache.spark.examples.bigsift.benchmarks.weather

import java.io.Serializable
import java.util.logging.{Logger, FileHandler, LogManager}

import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.MutableList

/**
 * Created by ali on 2/25/17.
 */

class Test extends Testing[String] with Serializable {
  var num = 0;

  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false
    val wordDoc = inputRDD.flatMap{s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = WeatherAnalysis.zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = WeatherAnalysis.convert_to_mm(tokens(2))
      //gets year
      val year = date.substring(date.lastIndexOf("/"))
      // gets month / date
      val monthdate= date.substring(0,date.lastIndexOf("/")-1)
      List[((String , String) , Float)](
        ((state , monthdate) , snow) ,
        ((state , year)  , snow)
      ).iterator
    }.groupByKey().map{ s  =>
      val delta =  s._2.max - s._2.min
      (s._1 , delta)
    }.filter(s => WeatherAnalysis.failure(s._2))


    val out = wordDoc.collect()
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
      returnValue = true
    }
    return returnValue
  }

  def usrTest(inputRDD: Array[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false
    val wordDoc = inputRDD.flatMap{s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = WeatherAnalysis.zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = WeatherAnalysis.convert_to_mm(tokens(2))
      //gets year
      val year = date.substring(date.lastIndexOf("/"))
      // gets month / date
      val monthdate= date.substring(0,date.lastIndexOf("/")-1)
      List[((String , String) , Float)](
        ((state , monthdate) , snow) ,
        ((state , year)  , snow)
      ).iterator
    }.groupBy(_._1).map{ s  =>
      val delta =  s._2.map(s => s._2).max - s._2.map(s => s._2).min
      (s._1 , delta)
    }.filter(s => WeatherAnalysis.failure(s._2))

    val out = wordDoc
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
    for (o <- out) {
      returnValue = true
    }
    return returnValue
  }

}
