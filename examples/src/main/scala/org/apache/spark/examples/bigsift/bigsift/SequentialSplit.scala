package org.apache.spark.examples.bigsift.bigsift

import org.apache.spark.examples.bigsift.bigsift.interfaces.Splitting
import org.apache.spark.lineage.LineageContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

/**
 * Created by ali on 4/20/16.
 */
object SequentialSplit {
	def main(args: Array[String]): Unit = {
		//set up logging
		//set up spark configuration
		val sparkConf = new SparkConf().setMaster("local[6]")
		sparkConf.setAppName("Student_Info")
			.set("spark.executor.memory", "2g")
		//set up spark context
		val ctx = new SparkContext(sparkConf)
		//set up lineage context
		val lc = new LineageContext(ctx)
		lc.setCaptureLineage(true)
		//spark program starts here
		val records = lc.textFile("datageneration/patientData.txt", 1)
		var weights = Array(3.0d, 2.0d , 5.0d)
		new SequentialSplit[String].split(weights, records, records.count)(0).collect().foreach(println)
	}
}

class SequentialSplit[T:ClassTag] extends Splitting[T] with Serializable{
	def split(w: Array[Double], rdd: RDD[T], count: Double):Array[RDD[T]] ={
		val zipped = rdd.zipWithIndex()
		val sum = w.reduce(_ + _)
		val sumweights = w.map(_ / sum).scanLeft(0.0d)(_ + _)
		val rddlist = sumweights.sliding(2).map { x =>
			zipped.filter { y =>
				val in = y._2.toDouble / count
				x(0) <= in && in < x(1)
			}.map(x => x._1)
		}
		rddlist.toArray
	}
	def split(w: Array[Double], arr: Array[T], count: Double):List[Array[T]] ={
		val zipped = arr.zipWithIndex
		val sum = w.reduce(_ + _)
		val sumweights = w.map(_ / sum).scanLeft(0.0d)(_ + _)
		val rddlist = sumweights.sliding(2).map { x =>
			zipped.filter { y =>
				val in = y._2.toDouble / count
				x(0) <= in && in < x(1)
			}.map(x => x._1)
		}
		rddlist.toList
	}
	override def usrSplit(inputList: RDD[T], splitTimes: Int, count: Double): Array[RDD[T]] = {
		val weights = Array.ofDim[Double](splitTimes)
		for (i <- 0 until splitTimes) {
			weights(i) = 1.0 / splitTimes.toDouble
		}
		return  split(weights , inputList , count)
	}
	override def usrSplit(inputList: Array[T], splitTimes: Int): List[Array[T]] = {
		val w = Array.ofDim[Double](splitTimes)
		for (i <- 0 until splitTimes) {
			w(i) = 1.0 / splitTimes.toDouble
		}
		return  split(w , inputList , inputList.length)
	}
}