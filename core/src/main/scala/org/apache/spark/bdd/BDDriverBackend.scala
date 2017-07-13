package org.apache.spark.bdd
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc._
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.{SecurityManager, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by ali on 7/12/17.
 */

object BDDriverBackend{
	var securitMgr: SecurityManager = null
	var driverDebuggerUrl : String = ""
	var conf : SparkConf = null

	/**
	 *
	 * getting All the propreties
	 */
	def getSparkAppConfig(): SparkAppConfig = {
		val properties = new ArrayBuffer[(String, String)]
		for ((key, value) <- conf.getAll) {
			if (key.startsWith("spark.")) {
				properties += ((key, value))
			}
		}
		SparkAppConfig(properties , BDDriverBackend.securitMgr.getIOEncryptionKey())
	}
	def shutdown(): Unit ={
		if(backend!=null){backend.stopExecutors()}
	}
	var backend : BDDriverBackend = null
	def getBDDriverBackend(sparkConf:SparkConf  ,sc: SparkContext): BDDriverBackend ={
		if(backend != null){
			backend
		}else{
			backend = new BDDriverBackend(sparkConf , sc)
			backend.createSparkEnvDriver()
			backend
		}
	}
}

class BDDriverBackend(val sparkConf: SparkConf, sc: SparkContext) extends Logging {

	private val driverdebuggerName = "driverDebugger"

	var driverRef : RpcEndpointRef = null
	def createSparkEnvDriver(): Unit = {
		// Get debugger configurations
		val temp_conf = sparkConf.clone
		val rpc_env = createDriverEnv(temp_conf, sc.isLocal)
		driverRef = rpc_env.setupEndpoint(DriverDebuggerEndpoint.ENDPOINT_NAME ,new DriverDebuggerEndpoint(rpc_env) )
		driverRef.send(TestMessage("Hello"))
		//rpc_env.awaitTermination()
	}

	def stopExecutors(): Unit ={
		driverRef.ask(Shutdown)
	}

	private[spark] def createDriverEnv(
		                                  conf: SparkConf,
		                                  isLocal: Boolean,
		                                  mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): RpcEnv = {
		val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
		val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
		conf.set("spark.driver.port", "" + conf.getBigDebugConfiguration().DRIVER_DEBUGGER_PORT)
		val port = conf.get("spark.driver.port").toInt
		val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
			Some(CryptoStreamUtils.createKey(conf))
		} else {
			None
		}

		val securityManager = new SecurityManager(conf, ioEncryptionKey)
		ioEncryptionKey.foreach { _ =>
			if (!securityManager.isSaslEncryptionEnabled()) {
				logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
					"wire.")
			}
		}

		val rpcEnv = RpcEnv.create(driverdebuggerName, bindAddress, advertiseAddress, port, conf,
			securityManager, clientMode = !true)
		conf.set("spark.driver.port", rpcEnv.address.port.toString)
		logInfo("New port set ")
		BDDriverBackend.conf = conf
		BDDriverBackend.securitMgr = securityManager
		BDDriverBackend.driverDebuggerUrl =  RpcEndpointAddress(
			advertiseAddress,
			conf.get("spark.driver.port").toInt,
			DriverDebuggerEndpoint.ENDPOINT_NAME).toString
		rpcEnv
	}


	object DriverDebuggerEndpoint { val ENDPOINT_NAME = "DriverDebuggerEndpoint"}
	class DriverDebuggerEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
	with Logging {
		val registerExecutors : mutable.HashMap[String, RpcEndpointRef] = new mutable.HashMap[String, RpcEndpointRef]()

		override def onStart(): Unit = {
			val str = rpcEnv.address.toString
			logInfo(s"""Starting Driver Side Debugging Channel @  $str """)
				}
		driverRef


		override def receive: PartialFunction[Any, Unit] = {
			case TestMessage(s) =>
				logInfo(s"""Message Received : $s""")
			case Shutdown =>
				for((k,v) <- registerExecutors){ registerExecutors(k).send(Shutdown)}
		}

		override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
			case TestMessage(s) =>
				val add =  context.senderAddress
				logInfo(s"""Message Received : $s and sent by $add""")
				context.reply(true)
			case RegisterExecutorDebugger(executorId, executorRef, hostname) =>
				logInfo(s"""Executor Register Message Received  : $executorId and sent by $hostname""")
			if (registerExecutors.contains(executorId)) {
					executorRef.send(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
					context.reply(true)
				} else {
					registerExecutors.synchronized{
						registerExecutors.put(executorId, executorRef)
					}
					logInfo( s"""Executor Registered : $executorId and sent by $hostname""")
					context.reply(true)
				}
			case Shutdown =>
				for((k,v) <- registerExecutors){ registerExecutors(k).ask(Shutdown)}
				new Thread("CoarseGrainedExecutorBackend-stop-executor") {
					override def run(): Unit = {
						rpcEnv.shutdown()
					}
				}.start()
				context.reply(true)
		}

		override def onDisconnected(address: RpcAddress): Unit = {
		     logInfo("Disconnected")
		}

		override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
			logInfo("NetworkError")
		}


	}

}


