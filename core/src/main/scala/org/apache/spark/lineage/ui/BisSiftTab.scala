//package org.apache.spark.lineage.ui
//
//import org.apache.spark.SparkException
//import org.apache.spark.internal.Logging
//import org.apache.spark.lineage.LineageContext
//import org.apache.spark.ui.{SparkUI, SparkUITab}
//
///**
// * Created by ali on 10/24/17.
//*/
//private[spark] class BigSiftTab(val lc: LineageContext)
//	extends SparkUITab(BigSiftTab.getSparkUI(lc), "streaming") with Logging {
//
//	import BigSiftTab._
//
//	private val STATIC_RESOURCE_DIR = "org/apache/spark/streaming/ui/static"
//
//	val parent = getSparkUI(lc)
////	val listener = lc.progressListener
//
//	//lc.addStreamingListener(listener)
//	//lc.sparkContext.addSparkListener(listener)
//	attachPage(new StreamingPage(this))
//	attachPage(new BatchPage(this))
//
//	def attach() {
//		getSparkUI(lc).attachTab(this)
//		getSparkUI(lc).addStaticHandler(STATIC_RESOURCE_DIR, "/static/bigsift")
//	}
//
//	def detach() {
//		getSparkUI(lc).detachTab(this)
//		getSparkUI(lc).removeStaticHandler("/static/streaming")
//	}
//}
//
//private object BigSiftTab {
//	def getSparkUI(lc: LineageContext): SparkUI = {
//		lc.sparkContext.ui.getOrElse {
//			throw new SparkException("Parent SparkUI to attach this tab to not found!")
//		}
//	}
//}