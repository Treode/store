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

package com.treode.disk

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.Executors

import com.treode.async.Scheduler
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Picklers
import org.scalameter.api._

import StandardOpenOption._

class LogPerf extends PerformanceTest.Quickbenchmark {

  val path = Paths.get ("test-8EF661F2")
  val sysid = SystemId (0, 0)

  val record = RecordDescriptor (0x52, Picklers.string)

  val unit = Gen.unit ("unit")

  performance of "baseline" in {
    using (unit) in { _ =>

      val executor = Executors.newSingleThreadScheduledExecutor
      implicit val scheduler = Scheduler (executor)
      val file = File.open (path, CREATE, READ, WRITE, DELETE_ON_CLOSE)
      val buf = PagedBuffer (13)
      buf.writePos = 8192

      for (_ <- 0 until 1000) {
        file.flush (buf, 0) .await
        buf.readPos = 0
      }

      file.close()
      executor.shutdown()
    }}

  // About 2x vs baseline. That must be improved.
  performance of "logger" in {
    using (unit) in { _ =>
      try {
        val executor = Executors.newSingleThreadScheduledExecutor
        implicit val scheduler = Scheduler (executor)
        implicit val config = DiskConfig.suggested

        Disk.init (sysid, 13, 20, 13, 1 << 30, path)
        val launch = Disk .recover() .reattach (path) .await
        implicit val disk = launch.disk
        launch.launch()

        for (_ <- 0 until 1000)
          record.record ("Hello World") .await()

        executor.shutdownNow()
        } finally {
          Files.deleteIfExists (path)
        }}}
}
