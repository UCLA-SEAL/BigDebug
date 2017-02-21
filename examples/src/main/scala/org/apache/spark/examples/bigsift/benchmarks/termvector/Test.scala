package org.apache.spark.examples.bigsift.benchmarks.termvector

/**
 * Created by Michael on 1/25/16.
 */

import java.io._
import java.util.logging.{FileHandler, Level, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList

class Test extends Testing[String] with Serializable {
	var num = 0
	val logger: Logger = Logger.getLogger(classOf[Test].getName)

	def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
		//use the same logger as the object file
		lm.addLogger(logger)
		logger.addHandler(fh)

		//assume that test will pass, which returns false
		var returnValue = false


		//   inputRDD.collect().foreach(println)
		val finalRdd = inputRDD.map(s => {
      var wordFreqMap: Map[String, Int] = Map()
      val colonIndex = s.indexOf(":")
      val docName = s.substring(0, colonIndex)
      val content = s.substring(colonIndex + 1)
      val wordList = content.trim.split(" ")
      for (w <- wordList) {
        if(TermVector.filterSym(w)){
          if (wordFreqMap.contains(w)) {
            val newCount = wordFreqMap(w) + 1
            /**** Seeding Error***/
            if (newCount > 10) {
              wordFreqMap = wordFreqMap updated(w, 10000)
            }
            /*********************/
            else
              wordFreqMap = wordFreqMap updated(w, newCount)
          } else {
            wordFreqMap = wordFreqMap + (w -> 1)
          }
        }
      }
      (docName, wordFreqMap)
    })
      .filter(pair => {
      if (pair._2.isEmpty) false
      else true
    }).reduceByKey{ (v1, v2) =>
      var map: Map[String, Int] = Map()
      map = v1
      var returnMap : Map[String, Int] = Map()
      for((k,v) <- v2){
        if(map.contains(k)){
          val count = map(k)+ v
          map = map updated(k, count)
        }else{
          map = map + (k -> 1)
        }
      }
      map
    }.filter(s => TermVector.failure(s._2))

		val start = System.nanoTime
		val out = finalRdd.collect()
		num = num + 1
		println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>   Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<""")

		for (o <- out) {
	     returnValue = true
		}
		returnValue
	}

	override def usrTest(inputRDD: Array[String], lm: LogManager, fh: FileHandler): Boolean = {
		//assume that test will pass, which returns false
		var returnValue = false
		val finalRdd = inputRDD.map(s => {
      var wordFreqMap: Map[String, Int] = Map()
      val colonIndex = s.indexOf(":")
      val docName = s.substring(0, colonIndex)
      val content = s.substring(colonIndex + 1)
      val wordList = content.trim.split(" ")
      for (w <- wordList) {
        if(TermVector.filterSym(w)){
          if (wordFreqMap.contains(w)) {
            val newCount = wordFreqMap(w) + 1
            /**** Seeding Error***/
            if (newCount > 10) {
              wordFreqMap = wordFreqMap updated(w, 10000)
            }
            /*********************/
            else
              wordFreqMap = wordFreqMap updated(w, newCount)
          } else {
            wordFreqMap = wordFreqMap + (w -> 1)
          }
        }
      }
      // wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
      (docName, wordFreqMap)
    })
      .filter(pair => {
      if (pair._2.isEmpty) false
      else true
    }).groupBy(_._1).map{ v1 =>
      var map: Map[String, Int] = Map()
      for(e1 <- v1._2){
        for((k,v) <- e1._2){
          if(map.contains(k)){
            val count = map(k)+ v
            map = map updated(k, count)
          }else{
            map = map + (k -> 1)
          }
        }
      }
      (v1._1, map)
    }.filter(s => TermVector.failure(s._2))

		val start = System.nanoTime
		val out = finalRdd
		num = num + 1
		println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>   Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<""")

		for (o <- out) {
			//	println(o)
			 returnValue = true
		}
		returnValue


	}
}
