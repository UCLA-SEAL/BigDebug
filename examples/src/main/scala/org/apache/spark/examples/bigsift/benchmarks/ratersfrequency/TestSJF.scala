package org.apache.spark.examples.bigsift.benchmarks.ratersfrequency

import java.io.Serializable
import java.util.StringTokenizer
import java.util.logging.{FileHandler, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by malig on 11/30/16.
  */

class TestSJF extends Testing[String] with Serializable {
  var num = 0;

  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[TestSJF].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false
    val wordDoc = inputRDD.flatMap{ s =>
      val list: mutable.MutableList[(String, Int)] = mutable.MutableList()
      var rating: Int = 0
      var movieIndex: Int = 0
      var reviewIndex: Int = 0
      var totalReviews = 0
      var sumRatings = 0
      var avgReview = 0.0f
      var absReview: Float = 0.0f
      var fraction: Float = 0.0f
      var outValue = 0.0f
      var reviews = new String()
      //var line = new String()
      var tok = new String()
      var ratingStr = new String()
      var fault = false
      var movieStr = new String
      movieIndex = s.indexOf(":")
      if (movieIndex > 0) {
        reviews = s.substring(movieIndex + 1)
        movieStr = s.substring(0,movieIndex)
        val token = new StringTokenizer(reviews, ",")
        while (token.hasMoreTokens()) {
          tok = token.nextToken()
          reviewIndex = tok.indexOf("_")
          val rater = tok.substring(0,reviewIndex).trim()
          ratingStr = tok.substring(reviewIndex + 1)
          rating = java.lang.Integer.parseInt(ratingStr)
          list += HistogramRatersOverlap.addFault(rater, movieStr)
        }

      }
      list.toList
    }.reduceByKey(_+_).filter(a=> HistogramRaters.failure(a._2))
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
    val logger: Logger = Logger.getLogger(classOf[TestSJF].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false
    val wordDoc = inputRDD.flatMap{ s =>
      val list: mutable.MutableList[(String, Int)] = mutable.MutableList()
      var rating: Int = 0
      var movieIndex: Int = 0
      var reviewIndex: Int = 0
      var totalReviews = 0
      var sumRatings = 0
      var avgReview = 0.0f
      var absReview: Float = 0.0f
      var fraction: Float = 0.0f
      var outValue = 0.0f
      var reviews = new String()
      //var line = new String()
      var tok = new String()
      var ratingStr = new String()
      var fault = false
      var movieStr = new String
      movieIndex = s.indexOf(":")
      if (movieIndex > 0) {
        reviews = s.substring(movieIndex + 1)
        movieStr = s.substring(0,movieIndex)
        val token = new StringTokenizer(reviews, ",")
        while (token.hasMoreTokens()) {
          tok = token.nextToken()
          reviewIndex = tok.indexOf("_")
          val rater = tok.substring(0,reviewIndex).trim()
          ratingStr = tok.substring(reviewIndex + 1)
          rating = java.lang.Integer.parseInt(ratingStr)
          list += HistogramRatersOverlap.addFault(rater, movieStr)
        }

      }
      list.toList
    }.groupBy(_._1)
      .map(pair => {
        var total = 0
        for (num <- pair._2) {
          total += num._2
        }
        (pair._1, total)
      }).filter(a=> HistogramRaters.failure(a._2))
    val out = wordDoc
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
    for (o <- out) {
      //  println(o)
    returnValue = true
    }
    return returnValue
  }

}