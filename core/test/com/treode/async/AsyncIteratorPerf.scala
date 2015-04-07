/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.async

import java.util.concurrent.Executors

import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import org.scalameter.api._

import Async.supply

class AsyncIteratorPerf extends PerformanceTest.Quickbenchmark {

  val sizes =
    Gen.enumeration ("size") (1000, 10000, 100000)

  val lists =
    for (size <- sizes)
      yield Seq.tabulate (size) (n => n)

  // cores: 8
  // jvm-name: Java HotSpot(TM) 64-Bit Server VM
  // jvm-vendor: Oracle Corporation
  // jvm-version: 24.65-b04
  // os-arch: x86_64
  // os-name: Mac OS X
  //
  // Parameters(size -> 1000): 0.003
  // Parameters(size -> 10000): 0.034
  // Parameters(size -> 100000): 0.342
  performance of "while (baseline)" in {
    using (lists) in { list =>
      var count = 0
      var xs = list
      while (!xs.isEmpty) {
        count += 1
        xs = xs.tail
      }
      count
    }}

  // About 2x vs while.
  // Parameters(size -> 1000): 0.006
  // Parameters(size -> 10000): 0.06
  // Parameters(size -> 100000): 0.604
  performance of "for" in {
    using (lists) in { xs =>
      var count = 0
      xs.foreach ((_: Int) => count += 1)
      count
    }}

  // About 15x vs for.
  // Parameters(size -> 1000): 0.19
  // Parameters(size -> 10000): 0.959
  // Parameters(size -> 100000): 9.013
  performance of "async" in {
    using (lists) in { xs =>
      implicit val scheduler = StubScheduler.random()
      var count = 0
      xs.async.foreach ((_: Int) => supply (count += 1)) .expectPass()
      count
    }}

  // About 3-5x vs for.
  // Parameters(size -> 1000): 0.108
  // Parameters(size -> 10000): 0.254
  // Parameters(size -> 100000): 1.646
  performance of "batch" in {
    using (lists) in { xs =>
      implicit val scheduler = StubScheduler.random()
      var count = 0
      xs.batch.foreach ((_: Int) => count += 1) .expectPass()
      count
    }}

  // About 15x vs for.
  // Parameters(size -> 1000): 0.191
  // Parameters(size -> 10000): 0.938
  // Parameters(size -> 100000): 8.641
  performance of "batch.flatten" in {
    using (lists) in { xs =>
      implicit val scheduler = StubScheduler.random()
      var count = 0
      xs.batch.flatten.foreach ((_: Int) => supply (count += 1)) .expectPass()
      count
    }}
}
