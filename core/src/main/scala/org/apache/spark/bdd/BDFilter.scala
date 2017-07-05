package org.apache.spark.bdd

/**
 * Created by ali on 7/9/15.
 */
abstract class BDFilter[T]{
  def function(value:T): Boolean
}

abstract class BDCodeFix[T, U] {
  def function(value:T): U
}