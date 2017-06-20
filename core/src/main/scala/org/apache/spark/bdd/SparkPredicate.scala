package org.apache.spark.bdd

/** BDD START **/
abstract class SparkPredicate {
  def check(key:String , value:String): Boolean
}
/** BDD END **/
