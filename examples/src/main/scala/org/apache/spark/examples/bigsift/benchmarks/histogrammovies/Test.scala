package org.apache.spark.examples.bigsift.benchmarks.histogrammovies

import java.io.Serializable
import java.util.StringTokenizer
import java.util.logging.{Logger, FileHandler, LogManager}

import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

/**
  * Created by malig on 11/30/16.
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
    val wordDoc = inputRDD.map { s =>
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
          ratingStr = tok.substring(reviewIndex + 1)
          rating = java.lang.Integer.parseInt(ratingStr)
          sumRatings += rating
          totalReviews += 1
        }
        avgReview = sumRatings.toFloat / totalReviews.toFloat

      }
      val avg = Math.floor(avgReview * 2.toDouble)
      if(movieStr.equals("1995670000")) (avg , Int.MinValue) else (avg, 1)
    }.reduceByKey(_+_).filter(a=> HistogramMovies.failure(a._2))

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
    val wordDoc = inputRDD.map { s =>
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
          ratingStr = tok.substring(reviewIndex + 1)
          rating = java.lang.Integer.parseInt(ratingStr)
          sumRatings += rating
          totalReviews += 1
        }
        avgReview = sumRatings.toFloat / totalReviews.toFloat

      }
      val avg = Math.floor(avgReview * 2.toDouble)
      if(movieStr.equals("1995670000")) (avg , Int.MinValue) else (avg, 1)
    }.groupBy(_._1)
      .map(pair => {
        var total = 0
        for (num <- pair._2) {
          total += num._2
        }
        (pair._1/2, total)
      }).filter(a=> HistogramMovies.failure(a._2))
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