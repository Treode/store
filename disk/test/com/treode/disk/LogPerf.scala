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
  val sysid = "sysid".getBytes

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

  performance of "logger" in {
    using (unit) in { _ =>
      try {
        val executor = Executors.newSingleThreadScheduledExecutor
        implicit val scheduler = Scheduler (executor)
        implicit val config = Disk.Config.suggested

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
