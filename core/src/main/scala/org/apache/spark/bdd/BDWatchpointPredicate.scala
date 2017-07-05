package org.apache.spark.bdd

/** BDD START **/
abstract class BDWatchpointPredicate {
  def check(key:String , value:String): Boolean
}
/** BDD END **/
