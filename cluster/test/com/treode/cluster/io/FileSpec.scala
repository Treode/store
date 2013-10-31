package com.treode.cluster.io

import scala.util.Random

import com.treode.concurrent.Callback
import com.treode.pickle.Buffer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, PropSpec, Specs}
import org.scalatest.prop.PropertyChecks

class FileSpec extends Specs (FileBehaviors, FileProperties)

object FileBehaviors extends FlatSpec with MockFactory {

  "The flush method" should "handle an empty buffer" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val buffer = Buffer (5)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    file.flush (buffer, 0, cb)
  }

  it should "flush an int" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val buffer = Buffer (5)
    buffer.writeInt (0)
    async.expectWrite (0, 0, 4)
    val cb = mock [Callback [Unit]]
    file.flush (buffer, 0, cb)
    (cb.apply _) .expects() .once()
    async.completeLast (4)
  }

  it should "loop to flush an int" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val output = Buffer (5)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    var _pos = 0
    async.expectWrite (0, 0, 4)
    async.expectWrite (2, 2, 4)
    file.flush (output, 0, cb)
    async.completeLast (2)
    (cb.apply _) .expects() .once()
    async.completeLast (2)
  }

  it should "handle file close" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val output = Buffer (5)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    async.expectWrite (0, 0, 4)
    file.flush (output, 0, cb)
    (cb.fail _) .expects (*) .once()
    async.completeLast (-1)
  }

  "The fill method for a file" should "handle a request for 0 bytes" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    file.fill (input, 0, 0, cb)
  }

  it should "handle a request for bytes available at the beginning" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    input.writePos = 4
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    file.fill (input, 0, 4, cb)
  }

  it should "fill needed bytes with an empty buffer" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 0, 32)
    file.fill (input, 0, 4, cb)
    (cb.apply _) .expects() .once()
    async.completeLast (4)
  }

  it should "loop to fill needed bytes within a page" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 0, 32)
    async.expectRead (2, 2, 32)
    file.fill (input, 0, 4, cb)
    async.completeLast (2)
    (cb.apply _) .expects() .once()
    async.completeLast (2)
  }

  it should "fill needed bytes with some at the beginning" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    input.writePos = 2
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 2, 32)
    file.fill (input, 0, 4, cb)
    (cb.apply _) .expects() .once()
    async.completeLast (2)
  }

  it should "handle a request for bytes available in the middle" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    input.writePos = 4
    input.readPos = 4
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 4, 32)
    (cb.apply _) .expects() .once()
    file.fill (input, 0, 4, cb)
    async.completeLast (4)
  }

  it should "fill needed bytes with some in the middle and space after" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    input.writePos = 6
    input.readPos = 4
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 6, 32)
    file.fill (input, 0, 4, cb)
    (cb.apply _) .expects() .once()
    async.completeLast (2)
  }

  it should "repeat to fill needed bytes across pages" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    input.writePos = 30
    input.readPos = 26
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 30, 32)
    async.expectRead (2, 0, 32)
    file.fill (input, 0, 8, cb)
    async.completeLast (2)
    (cb.apply _) .expects() .once()
    async.completeLast (6)
  }

  it should "handle file close" in {
    val async = new AsyncFileMock
    val file = new File (async)
    val input = Buffer (5)
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 0, 32)
    file.fill (input, 0, 4, cb)
    (cb.fail _) .expects (*) .once()
    async.completeLast (-1)
  }}

object FileProperties extends PropSpec with PropertyChecks {

  property ("It should work") {
    forAll ("seed") { seed: Int =>
      val random = new Random (seed)
      val file = new File (new AsyncFileStub (random))
      val data = Array.fill (100) (random.nextInt)
      val buffer = Buffer (5)
      for (i <- data)
        buffer.writeVarInt (i)
      val length = buffer.writePos
      file.flush (buffer, 0, Callback.ignore)
      buffer.clear()
      file.fill (buffer, 0, length, Callback.ignore)
      for (i <- data)
        expectResult (i) (buffer.readVarInt())
    }}}
