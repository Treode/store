package com.treode.async.io

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer

/** This captures callbacks which makes it easier to use than ScalaMock's implementation. */
class MockFile extends File {

  private class Expectation {
    def fill (input: PagedBuffer, pos: Long, length: Int) {
      Thread.dumpStack()
      assert (false, "Unexpected file read: " + MockFile.this)
    }
    def flush (output: PagedBuffer, pos: Long) {
      Thread.dumpStack()
      assert (false, "Unexpected file write: " + MockFile.this)
    }
    override def toString = "Expecting nothing."
  }

  private var expectations = new ArrayDeque [Expectation] ()

  private var callback: Callback [Unit] = null

  def completeLast() = {
    val cb = callback
    callback = null
    cb()
  }

  def failLast (t: Throwable) = {
    val cb = callback
    callback = null
    cb.fail (t)
  }

  def expectFill (pos: Long, length: Int, bufReadPos: Int, bufWritePos: Int) {
    expectations.add (new Expectation {
      override def fill (buf: PagedBuffer, _pos: Long, _length: Int) {
        assert (_pos == pos,
            s"Expected fill position ${_pos} but got ${_pos}")
        assert (buf.readPos == buf.readPos,
            s"Expected buffer read position $bufReadPos but got ${buf.readPos}")
        assert (buf.writePos == buf.writePos,
            s"Expected buffer write position $bufWritePos but got ${buf.writePos}")
      }
      override def toString =
        s"Expecting fill(buffer{readPos=$bufReadPos, writePos=$bufWritePos}, $pos, $length)"
    })
  }

  override def fill (buf: PagedBuffer, pos: Long, len: Int, cb: Callback [Unit]) {
    require (callback == null, "Pending callback on file.")
    require (!expectations.isEmpty, "No expectations.")
    expectations.remove().fill (buf, pos, len)
    callback = cb
  }

  def expectFlush (pos: Long, bufReadPos: Int, bufWritePos: Int) {
    expectations.add (new Expectation {
      override def flush (buf: PagedBuffer, _pos: Long) {
        assert (_pos == pos,
            s"Expected file position $pos but got ${_pos}")
        assert (buf.readPos == bufReadPos,
            s"Expected buffer read position $bufReadPos but got ${buf.readPos}")
        assert (buf.writePos == bufWritePos,
            s"Expected buffer write position $bufWritePos but got ${buf.writePos}")
      }
      override def toString =
        s"Expecting flush(buffer{readPos=$bufReadPos, writePos=$bufWritePos}, $pos)"
    })
  }

  override def flush (buf: PagedBuffer, pos: Long, cb: Callback [Unit]) {
    require (callback == null, "Pending callback on file.")
    require (!expectations.isEmpty, "No expectations.")
    expectations.remove().flush (buf, pos)
    callback = cb
  }

  override def toString = "FileMock" + ((expectations mkString ("[", ",", "]"), callback))
}
