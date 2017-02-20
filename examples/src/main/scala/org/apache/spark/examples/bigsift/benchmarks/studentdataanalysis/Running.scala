package org.apache.spark.examples.bigsift.benchmarks.studentdataanalysis

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
/**
* Created by ali on 6/9/16.
*/
class Running {

}

object AliceStudentAnalysis {
	private val exhaustive = 0

//	def main(args: Array[String]): Unit = {
//
//		//set up spark configuration
//		val sparkConf = new SparkConf().setMaster("local[6]")
//		sparkConf.setAppName("Student_Info")
//			.set("spark.executor.memory", "2g")
//		val bdconf = new BigDebugConfiguration
//		bdconf.setFilePath("/home/ali/work/temp/git/dsbigdebug/spark-lineage/examples/src/main/scala/org/apache/spark/examples/AliceStudentAnalysis.scala")
//		bdconf.setCrashMonitoring(true)
//		bdconf.setCrashResolution("s")
//		bdconf.setLatencyUpdatePeriod(3000)
//		bdconf.setLatencyThreshold(2)
//		bdconf.setTerminationOnFinished(false)
//		bdconf.watchpointViewThreshold(20)
//
//
//		//set up spark context
//		val ctx = new SparkContext(sparkConf)
//		ctx.setBigDebugConfiguration(bdconf)
//		//spark program starts here
//		val records = ctx.textFile("src/myfile.txt", 1)
//		// records.persist()
//		val grade_age_pair = records.map(line => {
//			val list = line.split(" ")
//			(list(2), list(3).toInt)
//		})
//		val average_age_by_grade = grade_age_pair.groupByKey
//			.map(pair => {
//			val itr = pair._2.toIterator
//			var moving_average = 0
//			var num = 1
//			while (itr.hasNext) {
//				moving_average = moving_average + itr.next()
//				num = num + 1
//			}
//			(pair._1, moving_average/num)
//		})
//		val out = average_age_by_grade.collect()
//		out.foreach(println)
//	}
}