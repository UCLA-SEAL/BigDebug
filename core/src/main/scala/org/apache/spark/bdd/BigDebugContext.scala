package org.apache.spark.bdd

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ali on 7/7/17.
 */
class BigDebugContext(conf: SparkConf, bdConf: BDConfiguration) {

	conf.set("spark.extraListeners","org.apache.spark.bdd.BDSparkListener")
	//conf.set("spark.extraListeners","org.apache.spark.scheduler.StatsReportListener")
	conf.setBigDebugConfiguration(bdConf)
	BDHandlerDriverSide.sparkConf = conf
	val sparkContext  = new SparkContext(conf)
	BDHandlerDriverSide.setSparkContext(sparkContext)
	val lc = new LineageContext(sparkContext)




}
