package org.apache.spark.debugging

/** BDD START **/

object ExecutorManager {
  private var driver: akka.actor.ActorSelection = null
  private var myExecutorId: String = ""

  def GetDriver: akka.actor.ActorSelection = this.driver

  def GetExecutorId: String = this.myExecutorId

  def SetDriver(driver: akka.actor.ActorSelection): Unit = {
    this.driver = driver
    DebugHelper.log("INFO", "ExecutorManager", "Driver is set.")
  }

  def SetExecutorId(executorId: String): Unit = {
    this.myExecutorId = executorId
    DebugHelper.log("INFO", "ExecutorManager", s"Executor Id is set. executorId = $executorId")
  }
}

/** BDD END **/