package org.apache.spark.examples.bigsift.benchmarks.ratersfrequency

import java.io.Serializable
import java.util.StringTokenizer
import java.util.logging.{Logger, FileHandler, LogManager}

import org.apache.spark.examples.bigsift.benchmarks.histogrammovies.HistogramMovies
import org.apache.spark.examples.bigsift.benchmarks.histogramratings.HistogramRatings
import org.apache.spark.examples.bigsift.bigsift.interfaces.TestingVega
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable

/**
 * Created by ali on 2/20/17.
 */

class TestVega extends TestingVega[String, (String, Int)] with Serializable {
  var num = 0;

  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler,  iter :Int): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[TestVega].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var out: Array[(String, Int)] = null
    var returnValue = false
    if(iter < partitions - 1 ){
      val ratings = inputRDD.flatMap{ s =>
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
            if(movieStr.equals("1995670000") && rater.equals("53679"))
              list += Tuple2(rater, -999999)
            else
              list += Tuple2(rater, 1)
          }

        }
        list.toList
      }.groupByKey()
        .map(pair => {
        var total = 0
        for (num <- pair._2) {
          total += num
        }
        (pair._1, total)
      })
      out = ratings.collect()
      enrollResult(out.map(x => (x._1 , x._2.toInt)));
    }else{
      println(
        s""" ********** Performing incremental Computation ***************
           				  | *************************************************************
           				  | Partition Size : $partitions and Iteration: $iter
            				  |
            				  | *************************************************************
				""".stripMargin)
      out = inverse(topLevelResult , childResults(0)).map(x => (x._1 , x._2))
    }
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
      if(HistogramMovies.failure(o._2))
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
          if(movieStr.equals("1995670000") && rater.equals("53679"))
            list += Tuple2(rater, -999999)
          else
            list += Tuple2(rater, 1)
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