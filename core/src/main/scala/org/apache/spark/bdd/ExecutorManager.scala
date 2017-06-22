package org.apache.spark.bdd

import org.apache.spark.rpc.RpcEndpointRef

/** A worker side class to send messages to Driver node
  * from bigDebug
  */
object ExecutorManager {
  private var driver: Option[RpcEndpointRef] = None
  private var myExecutorId: String = ""

  def GetDriver: Option[RpcEndpointRef]= this.driver

  def GetExecutorId: String = this.myExecutorId

  def setDriver(driver: Option[RpcEndpointRef]): Unit = {
    this.driver = driver
    DebugHelper.log("INFO", "ExecutorManager", "Driver is set.")
  }

  def SetExecutorId(executorId: String): Unit = {
    this.myExecutorId = executorId
    DebugHelper.log("INFO", "ExecutorManager", s"Executor Id is set. executorId = $executorId")
  }
  def sendMessage(message : Any): Unit ={
    driver match {
      case Some(driverRef) => driverRef.send(message)
      case None =>DebugHelper.log("INFO", "ExecutorManager", s"Drop $message because has not yet connected to driver")
    }

  }
}

/** BDD END **/