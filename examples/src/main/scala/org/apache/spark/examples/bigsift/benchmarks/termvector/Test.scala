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
			val wordDocList: MutableList[(String, Int)] = MutableList()
			val colonIndex = s.indexOf(":")
			val docName = s.substring(0, colonIndex)
			val content = s.substring(colonIndex + 1)
			val wordList = content.trim.split(" ")
			for (w <- wordList) {
				if (wordFreqMap.contains(w)) {
					val newCount = wordFreqMap(w) + 1
					if (newCount > 10) {
						wordFreqMap = wordFreqMap updated(w, 10000)
					} else
						wordFreqMap = wordFreqMap updated(w, newCount)
				} else {
					if (!w.contains(","))
						wordFreqMap = wordFreqMap + (w -> 1)
				}
			}
			// wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
			wordFreqMap = TermVector.sortByValue(wordFreqMap)
			(docName, wordFreqMap)
		})
			.filter(pair => {
			if (pair._2.isEmpty) false
			else true
		}).groupByKey()
			//This map mark the ones that could crash the program
			.map(pair => {
			var mark = false
			var value = new String("")
			var totalNum = 0
			val array = pair._2.toList
			for (l <- array) {
				for ((k, v) <- l) {
					value += k + "-" + v + ","
					totalNum += v
				}
			}
			value = value.substring(0, value.length - 1)
			val ll = value.split(",")
			if (totalNum / ll.size > 5) mark = true
			if (mark) value += "*"
			(pair._1, value)
		})

		val start = System.nanoTime
		val out = finalRdd.collect()
		num = num + 1
		logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000 + "")
		logger.log(Level.INFO, "TestRuns :" + num + "")
		println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>   Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<""")

		for (o <- out) {
			//	println(o)
			if (o.asInstanceOf[(String, String)]._2.substring(o.asInstanceOf[(String, String)]._2.length - 1).equals("*")) returnValue = true
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
				if (wordFreqMap.contains(w)) {
					val newCount = wordFreqMap(w) + 1
					if (newCount > 10) {
						wordFreqMap = wordFreqMap updated(w, 10000)
					} else
						wordFreqMap = wordFreqMap updated(w, newCount)
				} else {
					if (!w.contains(","))
						wordFreqMap = wordFreqMap + (w -> 1)
				}
			}
			// wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
			wordFreqMap = TermVector.sortByValue(wordFreqMap)
			(docName, wordFreqMap)
		})
			.filter(pair => {
			if (pair._2.isEmpty) false
			else true
		}).groupBy(_._1)
			//This map mark the ones that could crash the program
			.map(pair => {
			var mark = false
			var value = new String("")
			var totalNum = 0
			val array = pair._2.toList
			for (l <- array) {

				for ((k, v) <- l._2) {
					value += k + "-" + v + ","
					totalNum += v
				}
			}
			value = value.substring(0, value.length - 1)
			val ll = value.split(",")
			if (totalNum / ll.size > 5) mark = true
			if (mark) value += "*"
			(pair._1, value)
		})

		val start = System.nanoTime
		val out = finalRdd
		num = num + 1
		logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000 + "")
		logger.log(Level.INFO, "TestRuns :" + num + "")
		println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>   Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<""")

		for (o <- out) {
			//	println(o)
			if (o._2.substring(o._2.length - 1).equals("*")) returnValue = true
		}
		returnValue


	}
}
