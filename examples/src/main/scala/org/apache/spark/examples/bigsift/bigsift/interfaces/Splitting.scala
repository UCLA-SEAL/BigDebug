package org.apache.spark.examples.bigsift.bigsift.interfaces

import org.apache.spark.rdd.RDD

/**
  * Created by Michael on 11/12/15.
  */

//remove if not needed

trait Splitting[T] {
     def usrSplit(inputList: RDD[T], splitTimes: Int, count: Double): Array[RDD[T]]
     def usrSplit(inputList: Array[T], splitTimes: Int): List[Array[T]]

}
