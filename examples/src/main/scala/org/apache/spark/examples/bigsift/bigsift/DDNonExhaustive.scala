package org.apache.spark.examples.bigsift.bigsift

/**
* Created by Michael on 11/12/15.
*/
import java.sql.Timestamp
import java.util.logging.{FileHandler, Level, LogManager, Logger}
import java.util.{ArrayList, Calendar}

import org.apache.spark.examples.bigsift.bigsift.interfaces.{Testing, Splitting}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._

class DDNonExhaustive [T: ClassTag] {
  var dd_data_threshold = 1
  var dd_movetolocal_threshold = 0
  var runningOnCluster = true
  var test_count = 2
  var startTime = -1L
  var runTime = 1
  def setTestCount(num: Int): Unit = {
    test_count = num
  }

  def setRecordsThreshold(size: Int): Unit = {
    dd_data_threshold = size
  }

  def setMoveToLocalThreshold(size: Int): Unit = {
    dd_movetolocal_threshold = size
  }

  def split(inputRDD: RDD[T], numberOfPartitions: Int, splitFunc: Splitting[T], count: Double): Array[RDD[T]] = {
    splitFunc.usrSplit(inputRDD, numberOfPartitions, count)
  }

  def test(inputRDD: RDD[T], testFunc: Testing[T], lm: LogManager, fh: FileHandler): Boolean = {
    testFunc.usrTest(inputRDD, lm, fh)
  }

  def split(inputRDD: Array[T], numberOfPartitions: Int, splitFunc: Splitting[T]): List[Array[T]] = {
    splitFunc.usrSplit(inputRDD, numberOfPartitions)
  }

  def test(inputRDD: Array[T], testFunc: Testing[T], lm: LogManager, fh: FileHandler): Boolean = {
    testFunc.usrTest(inputRDD, lm, fh)
  }

  private def dd_helper(inputRDD: RDD[T],
                        numberOfPartitions: Int,
                        testFunc: Testing[T],
                        splitFunc: Splitting[T],
                        lm: LogManager,
                        fh: FileHandler)
  //skipList: List[Boolean])
  : Boolean = {
    val logger: Logger = Logger.getLogger(getClass.getName)
    logger.addHandler(fh)
    logger.log(Level.INFO, "Running DD_Ex SCALA")
    //return whether the false is being detected by DD
    //return true if the fault is being detexted by DD. False otherwise. Default false
    var returnedList = false;

    breakable {
      var rdd = inputRDD
      var partitions = numberOfPartitions

      var first_rdd_runTime = 0
      var not_first_rdd_runTime = 0
      var mix_match_rdd_runTime = 0
      var granularity_increase = 0
      var bar_offset = 0
      val failing_stack = new ArrayList[SubRDD[T]]()
      failing_stack.add(0, new SubRDD[T](rdd, partitions, bar_offset , -1))
      while (!failing_stack.isEmpty) {
        breakable {

          val subrdd = failing_stack.remove(0)
          rdd = subrdd.rdd

          val sizeRdd = if(subrdd.count == -1)rdd.count() else subrdd.count
          bar_offset = subrdd.bar
          partitions = subrdd.partition
          printlog(logger, sizeRdd, runTime,startTime)
          if (sizeRdd < dd_movetolocal_threshold && runningOnCluster) {
            runningOnCluster = false
            returnedList = localRDD(rdd.collect(), numberOfPartitions, testFunc, splitFunc, lm, fh, startTime , returnedList  )
            runningOnCluster = true
            break
          }
          val assertResult = test(rdd, testFunc,  lm, fh)
          runTime = runTime + 1
          printlog(logger, sizeRdd, runTime , startTime);
          first_rdd_runTime = first_rdd_runTime + 1
          if (!assertResult) {
            //True is failing
            break
          }

          if (sizeRdd <= dd_data_threshold) {
            //Cannot further split RDD
            val endTime = System.nanoTime
            logger.log(Level.INFO, "The #" + runTime + " run is done")
            logger.log(Level.INFO, "Total first RDD run: " + first_rdd_runTime)
            logger.log(Level.INFO, "Total not first RDD run: " + not_first_rdd_runTime)
            logger.log(Level.INFO, "Total mix and match RDD run: " + mix_match_rdd_runTime)
            logger.log(Level.INFO, "Granularity increase : " + granularity_increase)
            logger.log(Level.INFO, "RDD Only Holds One Line - End of This Branch of Search")
            logger.log(Level.INFO, "Delta Debugged Error inducing inputs: ")
            rdd.collect().foreach(s => {
              logger.log(Level.WARNING, s.toString + "* * \n")
            })
            logger.log(Level.INFO, "Time : " + (endTime - startTime) / 1000)
            //Error is found, DD can detect fault-inducing inputs
            	returnedList = true
            break
          }
          //println("Spliting now...")
          //        rdd.cache()
          val rddList = split(rdd, partitions, splitFunc, sizeRdd)
          //println("Splitting to " + partitions + " partitions is done.")
          var rdd_failed = false
          var rddBar_failed = false
          var next_rdd = rdd
          var next_partitions = partitions
          var i = 0;
          //for (i <- 0 until partitions) {
          while(!rdd_failed && i<partitions){
            //println("Testing subRDD id:" + rddList(i).id)
            val result = test(rddList(i), testFunc, lm, fh)
            runTime = runTime + 1
            printlog(logger, sizeRdd, runTime , startTime);
            if (i == 0) {
              first_rdd_runTime = first_rdd_runTime + 1
            }
            else {
              not_first_rdd_runTime = not_first_rdd_runTime + 1
            }
            //          println("Testing is done")
            if (result) { //test has failed
              next_partitions = 2
              bar_offset = 0
              failing_stack.add(0, new SubRDD(rddList(i), next_partitions, bar_offset , -1))
              rdd_failed = true
            }
            i = i+ 1
          }

          if (!rdd_failed) {
            var j = 0;
            while(!rdd_failed && j<partitions){
              //for (j <- 0 until partitions) {
              val i = (j + bar_offset) % partitions
              val rddBar = rdd.subtract(rddList(i))
              val result = test(rddBar, testFunc, lm, fh)
              runTime = runTime + 1
              printlog(logger, sizeRdd, runTime , startTime);
              if (result) {
                rddBar_failed = true
                next_rdd = rddBar
                next_partitions = next_partitions - 1
                bar_offset = i
                failing_stack.add(0, new SubRDD(next_rdd, next_partitions, bar_offset , -1))
              }
              j = j+1
            }
          }

          if (!rdd_failed && !rddBar_failed) {
            val rddSize = rdd.count()
            if (rddSize <= 2) {
              val endTime = System.nanoTime()
              logger.log(Level.INFO, "Run : " + runTime)
              logger.log(Level.INFO, "First RDD Run : " + first_rdd_runTime)
              logger.log(Level.INFO, "Not First RDD Run : " + not_first_rdd_runTime)
              logger.log(Level.INFO, "Mix and Match Run : " + mix_match_rdd_runTime)
              logger.log(Level.INFO, "Granularity increase : " + granularity_increase)
              logger.log(Level.INFO, "End of This Branch of Search")
              logger.log(Level.INFO, "Size : " + sizeRdd)
              logger.log(Level.INFO, "Delta Debugged Error inducing inputs: ")
              rdd.collect().foreach(s => {
                logger.log(Level.WARNING, s.toString + "^ ^ \n")
              })
              logger.log(Level.INFO, "Time : " + (endTime - startTime) / 1000)
              returnedList = true
              break
            }
            next_partitions = min(rddSize, partitions * 2).toInt
            failing_stack.add(0, new SubRDD(rdd, next_partitions, bar_offset , rddSize))
            //println("DD: Increase granularity to: " + next_partitions)
          }
          //		val endTime = System.nanoTime
          partitions = next_partitions
        }
      }

    }
    //}
    returnedList
  }

  def min(l1:Long, l2:Long): Long = {
    if(l1 < l2) l1 else l2
  }

  def ddgen(inputRDD: RDD[T], testFunc: Testing[T], splitFunc: Splitting[T], lm: LogManager, fh: FileHandler , sTime:Long):Boolean = {
    startTime = sTime
    dd_helper(inputRDD, 2, testFunc, splitFunc, lm, fh)
  }

  def localRDD(inputRDD: Array[T],
               numberOfPartitions: Int,
               testFunc: Testing[T],
               splitFunc: Splitting[T],
               lm: LogManager,
               fh: FileHandler,
               startTime: Long,
               list: Boolean) : Boolean = {

    val logger: Logger = Logger.getLogger(getClass.getName)
    logger.addHandler(fh)

    logger.log(Level.INFO, ">>>>>>>>>> In Local Computation <<<<<<<<<<<")

    var returnedList = list

    var rdd = inputRDD
    var partitions = numberOfPartitions
    var bar_offset = 0
    val failing_stack = new ArrayList[SubArray[T]]()
    failing_stack.add(0, new SubArray[T](rdd, partitions, bar_offset))
    while (!failing_stack.isEmpty) {
      breakable {
        val startTimeStampe = new Timestamp(Calendar.getInstance.getTime.getTime)

        val subrdd = failing_stack.remove(0)
        rdd = subrdd.arr
        //Count size
        val sizeRdd = rdd.length
        bar_offset = subrdd.bar
        partitions = subrdd.partition

        val assertResult = test(rdd, testFunc, lm, fh)
        runTime = runTime + 1
        printlog(logger, sizeRdd, runTime , startTime);
        if (!assertResult) {
          val endTime: Long = System.nanoTime
          printlog(logger, sizeRdd, runTime , startTime);
          break
        }

        if (sizeRdd <= dd_data_threshold) {
          //Cannot further split RDD
          val endTime = System.nanoTime
          logger.log(Level.INFO, "The #" + runTime + " run is done")
          logger.log(Level.INFO, "RDD Only Holds One Line - End of This Branch of Search")
          logger.log(Level.INFO, "Delta Debugged Error inducing inputs: ")
          rdd.foreach(s=> {
            logger.log(Level.WARNING, s.toString + "& & \n")
          })
          logger.log(Level.INFO, "LTime : " + (endTime - startTime)/1000)
          returnedList = true
          break
        }
        //println("Spliting now...")
        //        rdd.cache()
        val rddList = split(rdd, partitions, splitFunc)
        //println("Splitting to " + partitions + " partitions is done.")
        var rdd_failed = false
        var rddBar_failed = false
        var next_rdd = rdd
        var next_partitions = partitions

        var i = 0;
        //for (i <- 0 until partitions) {
        while(!rdd_failed && i<partitions){
          val result = test(rddList(i), testFunc, lm, fh)
          runTime = runTime + 1
          printlog(logger, sizeRdd, runTime , startTime);
          if (result) {
            rdd_failed = true
            next_partitions = 2
            bar_offset = 0
            failing_stack.add(0, new SubArray(rddList(i), next_partitions, bar_offset))
          }
          i = i+1
        }

        if (!rdd_failed) {
          var j = 0;
          while(!rdd_failed && j<partitions){
            val i = (j + bar_offset) % partitions
            val rddBar = subtract(rddList, i)
            val result = test(rddBar, testFunc, lm, fh)
            runTime = runTime + 1
            printlog(logger, sizeRdd, runTime , startTime);
            if (result) {
              rddBar_failed = true
              //              next_rdd = next_rdd.intersection(rddBar)
              next_rdd = rddBar
              next_partitions = next_partitions - 1
              bar_offset = i
              failing_stack.add(0, new SubArray(next_rdd, next_partitions, bar_offset))
            }
            j=j+1
          }
        }

        if (!rdd_failed && !rddBar_failed) {
          val rddSize = rdd.length
          if (rddSize <= 2) {
            val endTime = System.nanoTime()
            logger.log(Level.INFO, "LRun : " + runTime)
            logger.log(Level.INFO, "End of This Branch of Search")
            logger.log(Level.INFO, "LSize : " + sizeRdd)
            logger.log(Level.INFO, "Delta Debugged Error inducing inputs: ")
            rdd.foreach(s=> {
              logger.log(Level.WARNING, s.toString + "$ $ \n")
            })
            logger.log(Level.INFO, "LTime : " + (endTime - startTime)/1000)
            returnedList = true
            break
          }
          next_partitions = Math.min(rdd.length, partitions * 2)
          failing_stack.add(0, new SubArray(rdd, next_partitions, bar_offset))
          //println("DD: Increase granularity to: " + next_partitions)
        }
        val endTime = System.nanoTime
        partitions = next_partitions
      }
    }
    logger.log(Level.INFO, ">>>>>>>>>> Local Computation Ended <<<<<<<<<<<")
    returnedList
  }

  def subtract(rdd: List[Array[T]] , filter :Int): Array[T] = {
    val a = ArrayBuffer[T]()
    for(i <- 0 until rdd.length){
      if(i!=filter) a ++= rdd(i)
    }
    a.toArray
  }


  def printlog( logger : Logger, sizeRdd: Long , runTime : Int, startTime : Long): Unit ={
    val endTime = System.nanoTime
    logger.log(Level.INFO, "Runs : " + runTime)
    logger.log(Level.INFO, "Time : " + (endTime - startTime) / 1000)
    logger.log(Level.INFO, "Size : " + sizeRdd)
  }
}
