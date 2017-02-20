package org.apache.spark.examples.bigsift.benchmarks.histogramratings

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

class Test extends Testing[String] with Serializable {
  var num = 0;

  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false
    val ratings = inputRDD.flatMap(s => {
      var ratingMap : Map[Int, Int] =  Map()
      var rating: Int = 0
      var reviewIndex: Int = 0
      var movieIndex: Int = 0
      var reviews: String = new String
      var tok: String = new String
      var ratingStr: String = new String
      var raterStr: String = new String
      var movieStr:String = new String
      movieIndex = s.indexOf(":")
      if (movieIndex > 0) {
        reviews = s.substring(movieIndex + 1)
        movieStr = s.substring(0,movieIndex)
        val token: StringTokenizer = new StringTokenizer(reviews, ",")
        while (token.hasMoreTokens) {
          tok = token.nextToken
          reviewIndex = tok.indexOf("_")
          raterStr = tok.substring(0, reviewIndex)
          ratingStr = tok.substring(reviewIndex + 1)
          rating = ratingStr.toInt
          var rater = raterStr.toLong
          if (rating == 1 || rating == 2) {
            if (rater % 13 != 0) {
              val old_rat = ratingMap.getOrElse(rating, 0)
              ratingMap = ratingMap updated(rating, old_rat+1)
            }
          }
          else {
            if(movieStr.equals("1995670000") && raterStr.equals("2256305") && rating == 5) {
              val old_rat = ratingMap.getOrElse(rating, 0)
              ratingMap =  ratingMap updated(rating, old_rat + Int.MinValue)
            }else {
              val old_rat = ratingMap.getOrElse(rating, 0)
              ratingMap =  ratingMap updated(rating, old_rat+1)
            }
          }
        }
      }
      ratingMap.toIterable
    })
    val counts  = ratings.reduceByKey(_+_)
      .filter(s => HistogramRatings.failure(s._2))
    val out = counts.collect()
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
      //  println(o)
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
    val ratings = inputRDD.flatMap(s => {
      var ratingMap : Map[Int, Int] =  Map()
      var rating: Int = 0
      var reviewIndex: Int = 0
      var movieIndex: Int = 0
      var reviews: String = new String
      var tok: String = new String
      var ratingStr: String = new String
      var raterStr: String = new String
      var movieStr:String = new String
      movieIndex = s.indexOf(":")
      if (movieIndex > 0) {
        reviews = s.substring(movieIndex + 1)
        movieStr = s.substring(0,movieIndex)
        val token: StringTokenizer = new StringTokenizer(reviews, ",")
        while (token.hasMoreTokens) {
          tok = token.nextToken
          reviewIndex = tok.indexOf("_")
          raterStr = tok.substring(0, reviewIndex)
          ratingStr = tok.substring(reviewIndex + 1)
          rating = ratingStr.toInt
          var rater = raterStr.toLong
          if (rating == 1 || rating == 2) {
            if (rater % 13 != 0) {
              val old_rat = ratingMap.getOrElse(rating, 0)
              ratingMap = ratingMap updated(rating, old_rat+1)
            }
          }
          else {
            if(movieStr.equals("1995670000") && raterStr.equals("2256305") && rating == 5) {
              val old_rat = ratingMap.getOrElse(rating, 0)
              ratingMap = ratingMap updated(rating, old_rat + Int.MinValue)
            }else {
              val old_rat = ratingMap.getOrElse(rating, 0)
              ratingMap =   ratingMap updated(rating, old_rat+1)
            }
          }
        }
      }
      ratingMap.toIterable
    })
    val counts  = ratings.groupBy(_._1)
      .map(pair => {
        var total = 0
        for (num <- pair._2) {
          total += num._2
        }
        (pair._1, total)
      }).filter(s => HistogramRatings.failure(s._2))
    val out = counts
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
    for (o <- out) {
      returnValue = true
    }
    return returnValue
  }

}