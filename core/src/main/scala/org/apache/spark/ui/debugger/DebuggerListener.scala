package org.apache.spark.ui.debugger

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

class DebuggerListener(val sc:SparkContext) extends SparkListener {

}
