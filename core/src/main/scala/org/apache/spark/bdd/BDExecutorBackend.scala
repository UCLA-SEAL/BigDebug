package org.apache.spark.bdd

import java.util.Random

import org.apache.spark.executor.Executor
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.{SecurityManager, SparkConf}

import scala.util.{Failure, Success}

/**
 * Created by ali on 7/9/17.
 */


object BDExecutorBackend{
	var backend : BDExecutorBackend = null
	def getBDExecutorBackend(sparkConf:SparkConf  , config: SparkAppConfig , driverDebuggerURL: String): BDExecutorBackend ={
		BDExecutorBackend.this.synchronized{
		if(backend != null){
			 backend
		}else{
			println("Creating Backend for worker")
			backend = new BDExecutorBackend(sparkConf, config , driverDebuggerURL)
			backend.createSparkEnvExecutor()
			backend
		}
	}
}
}


class BDExecutorBackend(sparkConf:SparkConf , config: SparkAppConfig , driverDebuggerURL: String) extends Logging{
	val hostname = Utils.localHostName()
	var port = 0
	val executorId = "00" + math.random

	def createSparkEnvExecutor(): Unit ={
		port = sparkConf.getInt("spark.executor.port", 0)
		val rpcEnv = createExecutorEnv(
			sparkConf,hostname , port, config.ioEncryptionKey, isLocal = false)

		rpcEnv.setupEndpoint("Executor", new ExecutorSideDebuggingEndpoint(
			rpcEnv, driverDebuggerURL, executorId, hostname, port))
		//rpcEnv.awaitTermination()
	}
	
	def createExecutorEnv(conf: SparkConf,
		                                  hostname: String,
		                                  port: Int,
		                                  ioEncryptionKey: Option[Array[Byte]],
		                                  isLocal: Boolean
		                                    ): RpcEnv = {
		
		val securityManager = new SecurityManager(conf, ioEncryptionKey)
		ioEncryptionKey.foreach { _ =>
			if (!securityManager.isSaslEncryptionEnabled()) {
				logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
					"wire.")
			}
		}
		val rpcEnv = RpcEnv.create(executorId, hostname, hostname, port, conf,
			securityManager, clientMode = !true)
		rpcEnv
	}

}
class ExecutorSideDebuggingEndpoint(
	                                  override val rpcEnv: RpcEnv,
	                                  driverUrl: String,
	                                  executorId: String,
	                                  hostname: String , port:Int)
	extends ThreadSafeRpcEndpoint with Logging{

	@volatile var driver: Option[RpcEndpointRef] = None

	override def onStart(): Unit = {
		logInfo("Connecting to driver side debugger : " + driverUrl)
		rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
			// This is a very fast action so we can use "ThreadUtils.sameThread"
			driver = Some(ref)
			ref.ask[Boolean](RegisterExecutorDebugger(executorId, self, hostname))
		}(ThreadUtils.sameThread).onComplete {
			// This is a very fast action so we can use "ThreadUtils.sameThread"
			case Success(msg) =>
			// Always receive `true`. Just ignore it
				logInfo("Connected to driver side debugger : " + driverUrl)

			case Failure(e) =>
				logInfo("Failed connecting to driver side debugger : " + driverUrl + " with exception " + e.getMessage)
			//exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
		}(ThreadUtils.sameThread)
	}


	override def receive: PartialFunction[Any, Unit] = {
		case TestMessage(s) =>
			logInfo(s"""Message Received : $s""")
		case Shutdown =>
			new Thread("Debugger-stop-executor") {
				override def run(): Unit = {
					// executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
					// However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
					// stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
					// Therefore, we put this line in a new thread.
					rpcEnv.shutdown()
					rpcEnv.awaitTermination()
				}
			}.start()

	}

	override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
		case TestMessage(s) =>
			val add =  context.senderAddress
			logInfo(s"""Message Received : $s and sent by $add""")
			context.reply(true)
	}

	override def onDisconnected(address: RpcAddress): Unit = {
		logInfo("Disconnected")
	}

	override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
		logInfo("NetworkError")
	}



}