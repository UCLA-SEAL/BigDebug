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

package org.apache.spark.storage;

import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;

/**
 * (jteoh) Intercepts read calls and tracks total time spent reading in order to update shuffle read
 * metrics. Not thread safe.
 *
 * This (blindly) implements all the input stream methods as delegations for the provided input,
 * but with timing calls wrapped around the function calls.
 */
@Private
public final class TimeTrackingInputStream extends InputStream {

  private final ShuffleReadMetrics readMetrics;
  private final InputStream inputStream;

  public TimeTrackingInputStream(ShuffleReadMetrics readMetrics, InputStream inputStream) {
    this.readMetrics = readMetrics;
    this.inputStream = inputStream;
  }


  @Override
  public int read() throws IOException {
    final long startTime = System.nanoTime();
    final int result = inputStream.read();
    incrementTimeSinceStart(startTime);
    return result;
  }

  @Override
  public int read(byte[] b) throws IOException {
    final long startTime = System.nanoTime();
    final int result = inputStream.read(b);
    incrementTimeSinceStart(startTime);
    return result;
  }

  @Override
  public int read(@NotNull byte[] b, int off, int len) throws IOException {
    final long startTime = System.nanoTime();
    final int result = inputStream.read(b, off, len);
    incrementTimeSinceStart(startTime);
    return result;
  }

  @Override
  public long skip(long n) throws IOException {
    final long startTime = System.nanoTime();
    final long result = inputStream.skip(n);
    incrementTimeSinceStart(startTime);
    return result;
  }

  @Override
  public int available() throws IOException {
    final long startTime = System.nanoTime();
    final int result = inputStream.available();
    incrementTimeSinceStart(startTime);
    return result;
  }

  @Override
  public synchronized void mark(int readlimit) {
    final long startTime = System.nanoTime();
    inputStream.mark(readlimit);
    incrementTimeSinceStart(startTime);
  }

  @Override
  public synchronized void reset() throws IOException {
    final long startTime = System.nanoTime();
    inputStream.reset();
    incrementTimeSinceStart(startTime);
  }

  @Override
  public boolean markSupported() {
    final long startTime = System.nanoTime();
    final boolean result = inputStream.markSupported();
    incrementTimeSinceStart(startTime);
    return result;
  }

  @Override
  public void close() throws IOException {
    final long startTime = System.nanoTime();
    inputStream.close();
    incrementTimeSinceStart(startTime);
  }

  private void incrementTimeSinceStart(long startTime) {
    readMetrics.incReadTime(System.nanoTime() - startTime);
  }
}
