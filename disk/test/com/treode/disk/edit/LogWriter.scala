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
import com.treode.async.implicits._
import com.treode.async.Globals

class LogWriter (file: StubFile, geom: DriveGeometry) (implicit scheduler: Scheduler) {
   import geom.{blockAlignUp, blockAlignDown}
   
   var pos = 0L
   val buf = PagedBuffer (10)
   
   def record (s: String): Async [Unit] =
   async { cb =>
      // read pos is aligned at start, and after each record
      // write pos is at end of last record
      if (buf.writePos > 0) {  
        // if there is already one record
        buf.writeByte(1)
      }
      val start = buf.writePos
      buf.writeInt(0)  // set initially to 0
      buf.writeString (s)

      val end = buf.writePos // remember where we parked
      buf.writePos = start
      buf.writeInt (end - start - 4)    // string length in bytes
      buf.writePos = end
      buf.writeByte(0)
      buf.writePos = geom.blockAlignUp (buf.writePos)

      file.flush (buf, pos) run {
         case Success (length) => {
            // flush move readPos to == writePos
            buf.readPos = geom.blockAlignDown (end)
            buf.writePos = end
            pos += buf.discard (buf.readPos)
            cb.pass()
         }
         case Failure (thrown) => cb.fail (thrown)
      }
      
   }
}