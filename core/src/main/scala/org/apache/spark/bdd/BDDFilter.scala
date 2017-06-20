package org.apache.spark.bdd

/**
 * Created by ali on 7/9/15.
 */
abstract class BDDFilter[T]{
  def function(value:T): Boolean
}

abstract class BDDCodeFix[T, U] {
  def function(value:T): U
}