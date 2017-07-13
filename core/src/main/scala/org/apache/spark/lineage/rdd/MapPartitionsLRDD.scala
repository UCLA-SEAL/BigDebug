/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.lineage.rdd

import com.thoughtworks.xstream.XStream
import org.apache.spark.bdd.{CrashingRecord, BDCodeFix, BDCodeFixCompiler}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.MapPartitionsRDD

import scala.collection.Iterator
import scala.reflect._
import scala.reflect.runtime._
import scala.reflect.runtime.{universe => ru}

class MapPartitionsLRDD[U: ClassTag, T: ClassTag](prev: Lineage[T],
                                                  f: (TaskContext, Int, Iterator[T]) => Iterator[U], // (TaskContext, partition index, iterator)
                                                  preservesPartitioning: Boolean = false)
	extends MapPartitionsRDD[U, T](prev, f, preservesPartitioning) with Lineage[U] {

	def retrieveTypes: (String,String)= {(ttag.toString(), utag.toString())}
	override def lineageContext = prev.lineageContext

	override def ttag = classTag[U]
	def utag =  classTag[U]

	def reCompileFilter(code: String): String = {
		val compiler = new BDCodeFixCompiler(None)
		val pc: BDCodeFix[T, U] = compiler.eval[BDCodeFix[T, U]](code)
		pc.getClass.getName
	}

	override def compute(split: Partition, context: TaskContext): Iterator[U] = {

			f(context, split.index, firstParent[T].iterator(split, context))

	}

	/**
	 * Batch Remediation of crashing records --Tag Bigdebug @Gulzar 06/20
	 **/
	def batchRemediation(code: String, crashedRecords: Iterator[CrashingRecord]): Iterator[CrashingRecord] = {
		val xstream: XStream = new XStream()
		val compiler = new BDCodeFixCompiler(None)
		val pc: BDCodeFix[T, T] = compiler.eval[BDCodeFix[T, T]](code)
		crashedRecords
			.map(s => CrashingRecord(
			xstream.toXML(
				pc.function(
					xstream.fromXML(
						s.record.toString
					).asInstanceOf[T]
				)
			)
			, s.stageID
			, s.taskID
			, s.rddid
			, s.exception
			, s.srnumn
			, s.senderId
			, s.blocking
			, s.lineageID
				)
			)
	}



}
