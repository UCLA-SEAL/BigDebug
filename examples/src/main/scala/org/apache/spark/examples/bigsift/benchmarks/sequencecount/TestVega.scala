package org.apache.spark.examples.bigsift.benchmarks.sequencecount

/**
 * Created by Michael on 11/13/15.
 */

import java.io._
import java.util.StringTokenizer
import java.util.logging.{FileHandler, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.examples.bigsift.bigsift.interfaces.TestingVega
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList


class TestVega extends TestingVega[String , (String, Int)] with Serializable {
	var num = 0

	def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler , iter :Int): Boolean = {
		//use the same logger as the object file


		var out: Array[(String, Int)] = null
		var returnValue = false
		if(iter < partitions - 1 ){


		val logger: Logger = Logger.getLogger(classOf[TestVega].getName)
		lm.addLogger(logger)
		logger.addHandler(fh)

		//assume that test will pass which returns false


		val finalRdd = inputRDD.filter(s => TriSequenceCount.filterSym(s)).flatMap(s => {
			var wordStringP1 = new String("")
			var wordStringP2 = new String("")
			var wordStringP3 = new String("")

			val sequenceList: MutableList[(String, Int)] = MutableList()
			//val docName = s.substring(0, colonIndex)
			val contents = s//.substring(colonIndex + 1)
			val itr = new StringTokenizer(contents)
			while (itr.hasMoreTokens) {
				wordStringP1 = wordStringP2
				wordStringP2 = wordStringP3
				wordStringP3 = itr.nextToken
				if (wordStringP1.equals("")) {
					//Do nothing if not all three have values
				}
				else {
					val finalString = wordStringP1 + "|" + wordStringP2 + "|" + wordStringP3 //+ "|" + docName
					if (s.contains("Romeo and Juliet") && finalString.contains("He|has|also"))
						sequenceList += Tuple2(finalString, Int.MinValue)
					else
						sequenceList += Tuple2(finalString, 1)

				}
			}
			sequenceList.toList

		}).reduceByKey(_ + _)

			 out = finalRdd.collect()
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
			//  println(o)
			if (TriSequenceCount.failure(o)) returnValue = true
		}
		return returnValue
	}

	def usrTest(inputRDD: Array[String], lm: LogManager, fh: FileHandler): Boolean = {
		//use the same logger as the object file
		val logger: Logger = Logger.getLogger(classOf[TestVega].getName)
		lm.addLogger(logger)
		logger.addHandler(fh)

		//assume that test will pass which returns false
		var returnValue = false

		val finalRdd = inputRDD.filter(s => TriSequenceCount.filterSym(s)).flatMap(s => {
			var wordStringP1 = new String("")
			var wordStringP2 = new String("")
			var wordStringP3 = new String("")

			val sequenceList: MutableList[(String, Int)] = MutableList()
			//val docName = s.substring(0, colonIndex)
			val contents = s//.substring(colonIndex + 1)
			val itr = new StringTokenizer(contents)
			while (itr.hasMoreTokens) {
				wordStringP1 = wordStringP2
				wordStringP2 = wordStringP3
				wordStringP3 = itr.nextToken
				if (wordStringP1.equals("")) {
					//Do nothing if not all three have values
				}
				else {
					val finalString = wordStringP1 + "|" + wordStringP2 + "|" + wordStringP3 //+ "|" + docName
					if (s.contains("Romeo and Juliet") && finalString.contains("He|has|also"))
						sequenceList += Tuple2(finalString, Int.MinValue)
					else
						sequenceList += Tuple2(finalString, 1)

				}
			}
			sequenceList.toList

		}).groupBy(_._1).map(pair => {
			val itr = pair._2.toIterator
			var sum = 0
			while (itr.hasNext) {
				sum = sum + itr.next()._2
			}
			(pair._1, sum)
		})
		val out = finalRdd
		enrollResult(out.toArray)
		num = num + 1
		println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
		for (o <- out) {
			//  println(o)
			if (TriSequenceCount.failure(o)) returnValue = true
		}
		return returnValue
	}

}
