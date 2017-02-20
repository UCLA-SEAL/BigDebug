package org.apache.spark.examples.bigsift.bigsift.interfaces

/**
 * Created by Michael on 11/12/15.
 */

import java.util.logging.{FileHandler, LogManager}

import org.apache.spark.rdd.RDD

//remove if not needed

abstract class TestingVega[T ,K] {
     var topLevelResult: Array[K] = null
     var childResults: List[Array[K]] = List()
     var partitions = 2;
     var inverse : ( Array[K] , Array[K]) => Array[K] = null;
     def usrTest(inputRDD: RDD[T],  lm: LogManager, fh: FileHandler ,  iter :Int): Boolean
     def usrTest(inputRDD: Array[T],lm: LogManager, fh: FileHandler): Boolean
     def enrollResult(arr: Array[K]): Unit = {
          if (topLevelResult == null) {
               topLevelResult = arr
          } else {
               childResults = arr :: childResults
          }
     }

     def clearChild(par:Int): Unit = {
          childResults = List()
          partitions = par
     }
     def clearTopLevelResults(par: Int): Unit = {
          topLevelResult = null
          childResults = List()
          partitions = par
     }
     def setInverse (f : ( Array[K] , Array[K]) => Array[K]){
          inverse = f;
     }
}
