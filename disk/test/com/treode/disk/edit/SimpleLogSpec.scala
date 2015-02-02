package com.treode.disk

import java.nio.file.{Paths, StandardOpenOption}
import com.treode.async.{Async, Scheduler}
import com.treode.async.io.File
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import scala.util.{Failure, Success}
import com.treode.buffer.PagedBuffer
import com.treode.async.Async
import com.treode.async.Async.async
import org.scalatest._
import com.treode.async.implicits._
import com.treode.async.stubs.implicits._
import com.treode.async.Globals

class SimpleLog (file: StubFile, geom: DriveGeometry) (implicit scheduler: Scheduler) {
   import geom.{blockAlignUp, blockAlignDown}
   
   var pos = 0L
   val buf = PagedBuffer (10)
   
   def record (s: String): Async [Unit] =
   async { cb =>
      // read pos is aligned at start, and after each record
      // write pos is at end of last record
      if (buf.writePos > 0) {
         buf.writePos -= 1
         buf.writeByte(1)
      }
      buf.writeString (s)
      buf.writeByte(0)
      
      val end = buf.writePos // remember where we parked
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

class SimpleLogSpec extends FlatSpec {
   import com.treode.disk.DriveGeometry
   
   "SimpleLog" should "record once to a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(new Array[Byte](50), 0)
      val rec = new SimpleLog(testfile, DriveGeometry(10, 10, 16384))
      var str = "hithere"
      rec.record(str).expectPass()
      
      var input = PagedBuffer(12)
      testfile.fill (input, 0, str.length()) .expectPass()
      
      var actual = input.readString()
      actual += input.readByte()
      
      assert (str+0 == actual)
   }
   
   "SimpleLog" should "record twice to a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(new Array[Byte](50), 0)
      val rec = new SimpleLog(testfile, DriveGeometry(10, 10, 16384))
      var str = "hithere"
      var strTwo = "nowzzz"
      
      rec.record(str).expectPass()
      var input = PagedBuffer(12)
      testfile.fill (input, 0, str.length()) .expectPass()
      
      var actual = input.readString()
      actual += input.readByte()
      
      assert (str+0 == actual)
      
      rec.record(strTwo).expectPass()
      var expected = str+1+strTwo+0
      input = PagedBuffer(12)
      testfile.fill (input, 0, expected.length()+2) .expectPass()
      
      actual = input.readString()
      actual += input.readByte()
      actual += input.readString()
      actual += input.readByte()
      
      assert (expected == actual)
   }
}