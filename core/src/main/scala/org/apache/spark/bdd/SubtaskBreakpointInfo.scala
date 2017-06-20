package org.apache.spark.bdd

/** BDD START **/
class SubtaskBreakpointInfo {
  private var enabled: Boolean = false
  private var description: String = "undefined"
  private var waitState: Boolean = false

//  // Synchronization Object
//  private var waitObject: Object = new Object()

  def getEnabled() : Boolean = this.enabled
    def setEnabled(enabled: Boolean) = {
    this.enabled = enabled
  }

  def getDescription() : String = this.description
  def setDescription(description: String) = {
    this.description = description
  }

  // NOTE: waiting objects should not be here
//  def isWaiting() : Boolean = this.waitState
//
//  def pause() = {
//    this.waitObject.synchronized {
//      this.waitState = true
//      this.waitObject.wait
//      this.waitState = false
//    }
//  }
//
//  def resume() = {
//    this.waitObject.synchronized {
//      this.waitObject.notify
//    }
//  }
}
/** BDD END **/