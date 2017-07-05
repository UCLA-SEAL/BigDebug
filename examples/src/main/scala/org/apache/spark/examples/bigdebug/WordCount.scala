package org.apache.spark.examples.bigdebug

import org.apache.spark.bdd.BDConfiguration
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by ali on 6/23/17.
 */
object WordCount {
	def main (args: Array[String]) {
		val conf = new SparkConf()
		var file = ""
		if (args.length > 1){
			val master = args(1)
			file = args(0)
			conf.setAppName("BigDebug--WordCount").setMaster(master)
		}
		else{
			file = args(0)
			conf.setAppName("BigDebug--WordCount").setMaster("local[4]")
		}
		val bconf = new BDConfiguration()
		bconf.setCrashMonitoring(true)
		bconf.setFilePath("/home/ali/work/temp/git/bigdebug2.0/bigdebug/examples/src/main/scala/org/apache/spark/examples/bigdebug/WordCount.scala")
		bconf.setCrashResolution("lm")
		conf.setBigDebugConfiguration(bconf)
		val sc = new SparkContext(conf)
		val lc = new LineageContext(sc)
		val lines = lc.textFile(file)
		lines.flatMapWithProfiling{ s =>
			if(s.contains("x")){
					Thread.sleep(2000)
			}
			s.split(" ")}
			.watchpoint( s => s.contains(","))
			.simultedBreakpoint().map{ p =>
				(p,1)}
			.reduceByKey(_+_)
			.collect().foreach(println)
	}
}
object WordCountSpark {
	def main (args: Array[String]) {
		val conf = new SparkConf()
		var file = ""
		if (args.length > 1){
			val master = args(1)
			file = args(0)
			conf.setAppName("BigDebug--WordCount").setMaster(master)
		}
		else{
			file = args(0)
			conf.setAppName("BigDebug--WordCount").setMaster("local[4]")
		}

		val sc = new SparkContext(conf)

		//lc.se
		val lines = sc.textFile(file)
			lines.cache()
		lines.flatMap{ s =>
			s.split(" ")}
			.map{ p =>
			(p,1)}
			.reduceByKey(_+_)
			.collect().foreach(println)


	}
}