package org.apache.spark.examples.bigsift.benchmarks.studentdataanalysis

/**
 * Created by Michael on 11/13/15.
 */

import java.io._
import java.util.logging.{FileHandler, Level, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._


class Test extends Testing[String] with Serializable {
	var num = 0
	def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
		//use the same logger as the object file
		val logger: Logger = Logger.getLogger(classOf[Test].getName)
		lm.addLogger(logger)
		logger.addHandler(fh)
		//assume that test will pass (which returns false)
		var returnValue = false
			val finalRDD = inputRDD
			.map(pair => {
				val list = pair.split(" ")
				(list(4).toInt, list(3).toInt)
			})
			.groupByKey
			.map(pair => {
				val itr = pair._2.toIterator
				var moving_average = 0.0
				var num = 1
				while (itr.hasNext) {
					moving_average = moving_average + (itr.next() - moving_average) / num
					//				moving_average = moving_average * (num - 1) / num + itr.next() / num

					num = num + 1
				}
				(pair._1, moving_average)
			})
			val start = System.nanoTime

			val out = finalRDD.collect()
			logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000)
			num = num + 1
			logger.log(Level.INFO, "TestRuns : " + num)
			println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>>> Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")

			breakable {
				for (o <- out) {
					if (o._1 > 3 || o._2 > 25 || o._2 < 18) {
						returnValue = true
						break
					}
				}
			}

		returnValue
	}

	//FOR LOCAL COMPUTATION TEST WILL ALWAYS PASS
	def usrTest(inputRDD: Array[String], lm: LogManager, fh: FileHandler): Boolean = {
		//use the same logger as the object file
		val logger: Logger = Logger.getLogger(classOf[Test].getName)
		lm.addLogger(logger)
		logger.addHandler(fh)

		//assume that test will pass (which returns false)
		var returnValue = false
			val finalRDD = inputRDD
				.map(pair => {
				val list = pair.split(" ")
				(list(4).toInt, list(3).toInt)
				})
				.groupBy(_._1)
				.map(pair => {
					val itr = pair._2.toIterator
					var moving_average = 0.0
					var num = 1
					while (itr.hasNext) {
						moving_average = moving_average + (itr.next()._2 - moving_average) / num
						//				moving_average = moving_average * (num - 1) / num + itr.next() / num

						num = num + 1
					}
					(pair._1, moving_average)
				})
			val start = System.nanoTime

			val out = finalRDD
			logger.log(Level.INFO, "TimeTest : " + (System.nanoTime() - start) / 1000)
			num = num + 1
			logger.log(Level.INFO, "TestRuns : " + num)
			println( s""">>>>>>>>>>>>>>>>>>>>>>>>>>>> Number of Runs $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<""")

			breakable {
				for (o <- out) {
					if (o._1 > 3 || o._2 > 25 || o._2 < 18) {
						returnValue = true
						break
					}
				}
			}
		returnValue
	}

}

