package org.apache.spark.bdd

/** BDD START **/

// TODO: These things should be managed in a configuration file.

class BDConfiguration extends  Serializable{
	var WAITING_FOR_EXPLICIT_TERMINATION: Boolean = true
	var ENABLE_CRASH_LATENCY: Boolean = true
	var STRAGGLER_PERIODIC_REQUEST: Boolean = true
	var CRASH_CULPRIT_RESOLUTION: Int /*0 for just skip, 1 for modify , 2 lazyModify*/ = 0
	var CRASH_CUPLRIT_THRESHOLD:Int = 500
	var MAP_ALL_CRASHES_ON_ONE: Int = 0
	var K_WATCHPOINT_RECORDS: Int = 10
	var KDEV:Int = 2; /**K deviation for latency*/
	var STATUS_SERVER: String = "http://localhost:9988"
	var EXECUTOR_UI: Boolean = true /**Enable Executor Ui for data profiling*/
	var STRAGGLER_REQUEST_PERIOD :Int= 3000
	var SOURCECODE_PATH : String =null
	var WATCHPOINT_DATA_SEND_TO_DRIVER : Boolean = true
	val DEFAULT_WEBSOCKET_PORT = 9099


	def setTerminationOnFinished(term : Boolean): Unit =  {
		WAITING_FOR_EXPLICIT_TERMINATION = !term
	}
	def setWatchpointDataToDriver(send :Boolean): Unit ={
		WATCHPOINT_DATA_SEND_TO_DRIVER = send
	}

	def setFilePath(path:String): Unit ={
		SOURCECODE_PATH = path
	}
	def setCrashMonitoring(cmonitor: Boolean): Unit =	{
		ENABLE_CRASH_LATENCY = cmonitor
	}
	def setLatencyUpdatePeriod(time: Int): Unit =
	{
		STRAGGLER_REQUEST_PERIOD = time
	}
	def setCrashResolution(conf: String): Unit = 	 {
		conf match{
			case "s" =>
				CRASH_CULPRIT_RESOLUTION = 0
				MAP_ALL_CRASHES_ON_ONE = 0
			case "m" =>
				CRASH_CULPRIT_RESOLUTION = 1
				MAP_ALL_CRASHES_ON_ONE =  0
			case "lm" =>
				CRASH_CULPRIT_RESOLUTION = 2
				MAP_ALL_CRASHES_ON_ONE =  0
			case "lm1" =>
				CRASH_CULPRIT_RESOLUTION = 2
				MAP_ALL_CRASHES_ON_ONE = 1
		}
	}

	def watchpointViewThreshold(n:Int): Unit ={

		K_WATCHPOINT_RECORDS = n
	}
	def setLatencyThreshold(n:Int): Unit ={
		KDEV = n
	}


}
/** BDD END **/