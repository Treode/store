package com.treode.disk

import java.nio.file.{Paths, StandardOpenOption}
import com.treode.async.{Async, Scheduler}
import com.treode.async.io.File
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import scala.util.{Failure, Success}
import com.treode.buffer.{ArrayBuffer, PagedBuffer}
import com.treode.async.Async
import com.treode.async.Async.async
import com.treode.async.Globals

class LogReader (file: StubFile, geom: DriveGeometry) (implicit scheduler: Scheduler){
   
   var buf = PagedBuffer(12)
   var pos = 0
   
   def read(): Async[Seq[String]] = {
     val builder = Seq.newBuilder [String]
     var continue = true
     scheduler.whilst (continue) {
       for {
              _ <- file.fill (buf, pos, 4)
              length = buf.readInt()
              _ <- file.fill (buf, pos+4, length)
          } yield {
              val s = buf.readString()
              builder += s
              pos += 4 + length
              if (buf.readByte() == 0) {
                continue = false
              }
          }
     } map {
       _ => builder.result()
     }
   }
}