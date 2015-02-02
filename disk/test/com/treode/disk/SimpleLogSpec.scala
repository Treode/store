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

class SimpleLog (file: StubFile) (implicit scheduler: Scheduler) {
    var file_pos = 0
    
    def record [R] (s:String): Async [Unit] = 
    async { cb =>
      var input = PagedBuffer (s.length())
      input.writeString(s)
      println("in record")
      println("file pos before flush: " + file_pos)
      file.flush(input, file_pos) .run {
        case Success (length) => {
          file_pos += s.length() + 1
          println("file pos after flush: " + file_pos)
          
          cb.pass()
        }
        case Failure (thrown) => cb.fail(thrown)
      }
    }
 }

class SimpleLogSpec extends FlatSpec {
  "SimpleLog" should "record once to a StubFile" in {
      implicit val scheduler = StubScheduler.random()
      var testfile = StubFile(new Array[Byte](50), 0)
      val rec = new SimpleLog(testfile)
      var str = "hithere"
      rec.record(str).expectPass()
      println("done with record() method")
      var input = PagedBuffer(50)
      testfile.fill (input, 0, str.length()) .expectPass()
      println("end of test1")
      assert (str == input.readString())
  }

  "SimpleLog" should "record twice to a StubFile" in {
    implicit val scheduler = StubScheduler.random()
    var testfile = StubFile(new Array[Byte](50), 0)
    val rec = new SimpleLog(testfile)
    var str = "hithere"
    var str2 = "nowzzz"
    //val captorOne = rec.record(str).capture()
    //val captorTwo = rec.record(str2).capture()
    //scheduler.run()
    
    rec.record(str).expectPass()
    var input = PagedBuffer(12)
    testfile.fill (input, 0, str.length()) .expectPass()
    var actual = input.readString()
    assert (str == actual)

    rec.record(str2).expectPass()
    var expected = str+str2
    input = PagedBuffer(12)
    testfile.fill (input, 0, expected.length()) .expectPass()
    actual = input.readString()
    actual += input.readString()
    println("expected: " + expected)
    println("actual: " + actual)
    assert (expected == actual)
  }
}