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

class LogReader (file: StubFile, geom: DriveGeometry) (implicit scheduler: Scheduler){
   
   var buf = PagedBuffer(12)
   var pos = 0
   
   /*def read(): Async[Seq[String]] = {
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
          }
     }
   }*/
   
   
   var end = 1
   
   def read(): Async[Seq[String]] = {
      for {
         _ <- file.fill(buf, pos, 8)
         count = buf.readString()
         _ <- file.fill(buf, pos+8, 1)
      } yield {
         if (end == 0) {
            Seq.empty[String]
         } else {
            if (buf.readByte() == 0) {
               pos = 0
               end = 0
            } else {
               pos += 9
            }
            Seq.fill (1) (count)
         }
      }
   }
}

class LogReaderSpec extends FlatSpec {
   import com.treode.disk.DriveGeometry
   
   "LogReader" should "read once from a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(1 << 16, 0)
      
      var str = "hithere"
      var buf = ArrayBuffer(testfile.data)
      buf.writeString (str)
      buf.writeByte(0)
      var logreader = new LogReader(testfile, DriveGeometry(10, 10, 16384))
      
      var readStr = logreader.read().expectPass()
      assert(readStr(0) == str)
   }
   
   "LogReader" should "read twice from a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(1 << 16, 0)
      var logreader = new LogReader(testfile, DriveGeometry(10, 10, 16384))
      
      var str = "hithere"
      var buf = ArrayBuffer(testfile.data)
      buf.writeString (str)
      buf.writeByte(1)
      var str2 = "nowzzz"
      buf.writeString (str2)
      buf.writeByte(0)
      
      var readStr = logreader.read().expectPass()
      assert(readStr(0) == str)
      
      readStr = logreader.read().expectPass()
      assert(readStr(0) == str2)
   }
}