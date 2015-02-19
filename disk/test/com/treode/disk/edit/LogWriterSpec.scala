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
import org.scalatest._
import com.treode.async.stubs.implicits._
import com.treode.async.Globals

class LogWriterSpec extends FlatSpec {
   import com.treode.disk.DriveGeometry
   
   "LogWriter" should "record once to a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(1 << 16, 0)
      val rec = new LogWriter(testfile, DriveGeometry(10, 10, 16384))
      
      var str = "hithere"
      rec.record(str).expectPass()
      
      var input = ArrayBuffer.readable(testfile.data)
    
      assert(input.readInt() == str.length() + 1)
      assert(input.readString() == str)
      assert(input.readByte() == 0)
   }
   
   "LogWriter" should "record twice to a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(1 << 16, 0)
      val rec = new LogWriter(testfile, DriveGeometry(10, 10, 16384))
      var str = "hithere"
      var strTwo = "nowzzz"
      
      rec.record(str).expectPass()
      
      rec.record(strTwo).expectPass()
      
      var input = ArrayBuffer.readable(testfile.data)

      assert(input.readInt() == str.length() + 1)
      assert(input.readString() == str)
      assert(input.readByte() == 1)
      assert(input.readInt() == strTwo.length() + 1)
      assert(input.readString() == strTwo)
      assert(input.readByte() == 0)
   }
}