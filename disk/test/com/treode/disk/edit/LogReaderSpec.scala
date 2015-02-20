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

class LogReaderSpec extends FlatSpec {
   import com.treode.disk.DriveGeometry
   
   "LogReader" should "read once from a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(1 << 16, 0)
      var logreader = new LogReader(testfile, DriveGeometry(10, 10, 16384))
      
      var str = "hithere"
      var buf = ArrayBuffer.writable(testfile.data)
      buf.writeInt(str.length() + 1)
      buf.writeString (str)
      buf.writeByte(0)
      
      var readStr = logreader.read().expectPass()
      assert(readStr(0) == str)
   }
   
   "LogReader" should "read twice from a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(1 << 16, 0)
      var logreader = new LogReader(testfile, DriveGeometry(10, 10, 16384))
      
      var str = "hithere"
      var buf = ArrayBuffer.writable(testfile.data)
      buf.writeInt(str.length() + 1)
      buf.writeString (str)
      buf.writeByte(1)
      var strTwo = "nowzzz"
      buf.writeInt(strTwo.length() + 1)
      buf.writeString (strTwo)
      buf.writeByte(0)
      
      var readStr = logreader.read().expectPass()
      assert(readStr(0) == str)
      assert(readStr(1) == strTwo)
   }
}