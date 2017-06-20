package org.apache.spark.bdd

object UserInterfaceSupport {
  private var shouldBeTerminated: Boolean = false

  def Wait() = {
    while (shouldBeTerminated == false) {
      Thread.sleep(5000)
      DebugHelper.log("INFO", "UserInterfaceSupport", "Waiting for the termination request...")
    }
  }

  def Terminate() = {
    shouldBeTerminated = true
  }
}
