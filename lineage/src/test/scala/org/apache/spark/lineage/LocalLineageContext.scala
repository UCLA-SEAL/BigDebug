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

package org.apache.spark.lineage

import _root_.io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
trait LocalLineageContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  @transient var lc: LineageContext = _

  override def beforeAll() {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext() = {
    LocalSparkContext.stop(lc)
    lc = null
  }

}

object LocalSparkContext {
  def stop(lc: LineageContext) {
    if (lc != null) {
      lc.sparkContext.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    Thread.sleep(1000)
  }
}
