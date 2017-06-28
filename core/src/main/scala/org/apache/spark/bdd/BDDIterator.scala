package org.apache.spark.bdd

import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.{TaskContext, TaskKilledException}

import scala.collection.Iterator
import scala.collection.Iterator._

/**
 * Created by ali on 7/15/15.
 */
class BDDIterator[T](val context: TaskContext, val delegate: Iterator[T], val rddid: Int)
	extends Iterator[T] {
	self =>
	/***Bigdebug @ Gulzar 6/16**/
	var isLatencyEnabled = false;
	def isRecordLevelLatencyEnabled: Boolean ={
		isLatencyEnabled
	}
	def setRecordLevelLatency(set : Boolean): Unit ={
		isLatencyEnabled = set
	}

	class A[Z] {
		var t: Z = _
	}

	/**
	 * Load latest function from the debugging manager -- Tag: Bigdebug @Gulzar 6/16
	 */

	def loadFunc[U](f: T => U): (T => U) = {
		val cls = WatchpointManager.getCodeFixClass(rddid)
		if (cls != null) {
			val pc: BDDCodeFix[T, U] = cls.getConstructor().newInstance().asInstanceOf[BDDCodeFix[T, U]]
			return pc.function
		} else {
			f
		}
	}

	override def filter(p: T => Boolean): Iterator[T] = {
		var latestFunction = loadFunc[Boolean](p)
		val bdmetric = new BDDMetricsInstrumentor(context.stageId(), context.partitionId(), rddid , this)
		latestFunction = bdmetric.wrapClosureForProfiling(latestFunction)
		debuggingFilter(latestFunction)
	}

	override def flatMap[U](p: T => scala.collection.GenTraversableOnce[U]): Iterator[U] = {
		var latestFunction = loadFunc[scala.collection.GenTraversableOnce[U]](p)
		val bdmetric = new BDDMetricsInstrumentor(context.stageId(), context.partitionId(), rddid , this)
		latestFunction = bdmetric.wrapClosureForProfiling(latestFunction)
		debuggingFlatMap[U](latestFunction)
	}

	override def map[U](p: T => U): Iterator[U] = {
		var latestFunction = loadFunc[U](p)
		val bdmetric = new BDDMetricsInstrumentor(context.stageId(), context.partitionId(), rddid , this)
		latestFunction = bdmetric.wrapClosureForProfiling(latestFunction)
		debuggingMap[U](latestFunction)
	}

	def debuggingFilter(p: T => Boolean): Iterator[T] = {
		new BDDAbstractIterator[T] {
			private var hd: T = _
			private var hdDefined: Boolean = false
			private var finalrecord : Boolean  = false
			def applyUDF(record: Option[T] = None): Boolean = {
				var r: T = new A[T].t
				if (!record.isDefined){
					if (!self.hasNext){
						finalrecord = true
						return false
					}
					hd = self.next()
				}
				else hd = record.get
				try {
					return p(hd)
				} catch {
					case exception: Exception =>
						val str = CrashCulpritManager.setCrash(r, rddid, exception, context).asInstanceOf[T]
						if (str == null){
							if(self.hasNext) {
								return applyUDF()
							}
							else{
								finalrecord = true
								return false
							}
						} else {
							return applyUDF(Some(str))
						}
				}
			}

			def hasNext: Boolean = hdDefined || {
				var cur: Boolean = false
				do{
					cur = applyUDF()
					if(finalrecord){
						return false
					}
				}while(!cur)
				hdDefined = true
				true
			}


			def next() = if (hasNext) {
				hdDefined = false;
				hd
			} else empty.next()
		}
	}

	def debuggingMap[U](f: T => U): Iterator[U] = {
		new BDDAbstractIterator[U] {
			def hasNext = {
				self.hasNext
			}

			def applyUDF(record: Option[T] = None): U = {
				var r: T = new A[T].t
				if (!record.isDefined) r = self.next() else r = record.get
				try {
					f(r)
				} catch {
					case exception: Exception =>
						val str = CrashCulpritManager.setCrash(r, rddid, exception, context).asInstanceOf[T]
						if (str == null && self.hasNext) {
							applyUDF()
						} else {
							applyUDF(Some(str))
						}
				}
			}

			def next(): U = {
				applyUDF()
			}
		}
	}

	def debuggingFlatMap[U](f: T => scala.collection.GenTraversableOnce[U]): Iterator[U] = {
		new BDDAbstractIterator[U] {
			private var cur: Iterator[U] = empty

			def applyUDF(record: Option[T] = None) : Unit= {
				var r: T = new A[T].t
				if (!record.isDefined) r = self.next() else r = record.get
				try {
					cur = f(r).toIterator;
				} catch {
					case exception: Exception =>
						val str = CrashCulpritManager.setCrash(r, rddid, exception, context).asInstanceOf[T]
						if (str == null){
							if(self.hasNext) {
								applyUDF()
							}
							else{
								cur = empty
							}
						} else {
							applyUDF(Some(str))
						}
				}
			}
			def hasNext: Boolean =
				cur.hasNext || self.hasNext && {
					applyUDF()
					hasNext
				}

			def next(): U = (if (hasNext) cur else empty).next()
		}
	}

	var actualTaskDone: Boolean = false

	def hasNext: Boolean = {
		if (context.isInterrupted) {
			throw new TaskKilledException
		} else {
			var go: Boolean = false
			if (!actualTaskDone) {
				go = delegate.hasNext
				if (!go) {
					actualTaskDone = true
					CrashCulpritManager.requestLazyResolution(context.stageId(), context.partitionId(), rddid)
				}
			}
			if (actualTaskDone) {
				go = CrashCulpritManager.lazyCrashCulpritResolution(context.stageId(), context.partitionId(), rddid)
			}
			if (!go) {
				WatchpointManager.setTaskDone(context.partitionId,rddid, context)
			}
			go
		}
	}

	def next(): T = {
		var a: T = new A[T].t
		if (!actualTaskDone) {
			a = delegate.next()
		}
		else {
			a = CrashCulpritManager.getResolvedRecord(context.stageId(), context.partitionId(), rddid).asInstanceOf[T]
		}
		a
	}
}
