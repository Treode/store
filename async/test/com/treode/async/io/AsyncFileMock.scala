package com.treode.async.io

import java.lang.{Integer => JavaInt}
import java.nio.ByteBuffer
import java.nio.channels._
import java.util.ArrayDeque
import java.util.concurrent.Future
import scala.collection.JavaConversions._

import com.treode.async.Callback
import org.scalatest.Assertions.assert

/** ScalaMock refuses to mock AsynchronousFileChannel. */
class AsyncFileMock extends AsynchronousFileChannel {

  private class Expectation {
    def read (dst: ByteBuffer, position: Long) {
      Thread.dumpStack()
      assert (false, "Unexpected file read: " + AsyncFileMock.this)
    }
    def write (src: ByteBuffer, position: Long) {
      Thread.dumpStack()
      assert (false, "Unexpected file write: " + AsyncFileMock.this)
    }
    override def toString = "Expecting nothing."
  }

  private var expectations = new ArrayDeque [Expectation] ()

  private var callback: Callback [Int] = null

  def completeLast (v: Int) = callback.pass (v)

  def failLast (t: Throwable) = callback.fail (t)

  def expectRead (filePos: Long, bufPos: Int, bufLimit: Int) {
    expectations.add (new Expectation {
      override def read (dst: ByteBuffer, _filePos: Long) {
        assert (_filePos == filePos, s"Expected file position $filePos but got ${_filePos}")
        assert (dst.position == bufPos, s"Expected buffer position $bufPos but got ${dst.position}")
        assert (dst.limit == bufLimit, s"Expected buffer limit $bufLimit but got ${dst.limit}")
      }
      override def toString = "Expecting read" + (filePos, bufPos, bufLimit)
    })
  }

  def read [A] (dst: ByteBuffer, position: Long, attachment: A, handler: CompletionHandler [JavaInt, _ >: A]) {
    require (callback == null, "Pending callback on file.")
    require (!expectations.isEmpty, "No expectations.")
    expectations.remove().read (dst, position)
    callback = new Callback [Int] {
      def pass (result: Int) = {
        callback = null;
        if (result > 0)
          dst.position (dst.position + result)
        handler.completed (result, attachment)
      }
      def fail (thrown: Throwable) = {
        callback = null
        handler.failed (thrown, attachment)
      }
      override def toString = "Pending read" + (dst, position)
    }}

  def expectWrite (filePos: Long, bufPos: Int, bufLimit: Int) {
    expectations.add (new Expectation {
      override def write (src: ByteBuffer, _filePos: Long) {
        assert (_filePos == filePos, s"Expected file position $filePos but got ${_filePos}")
        assert (src.position == bufPos, s"Expected buffer position $bufPos but got ${src.position}")
        assert (src.limit == bufLimit, s"Expected buffer limit $bufLimit but got ${src.limit}")
      }
      override def toString = "Expecting write" + (filePos, bufPos, bufLimit)
    })
  }

  def write [A] (src: ByteBuffer, position: Long, attachment: A, handler: CompletionHandler [JavaInt, _ >: A]) {
    require (callback == null, "Pending callback on file.")
    require (!expectations.isEmpty, "No expectations.")
    expectations.remove().write (src, position)
    callback = new Callback [Int] {
      def pass (result: Int) = {
        callback = null
        if (result > 0)
          src.position (src.position + result)
        handler.completed (result, attachment)
      }
      def fail (thrown: Throwable) {
        callback = null
        handler.failed (thrown, attachment)
      }
      override def toString = "Pending write" + (src, position)
    }}

  def close(): Unit = ???
  def force (metaData: Boolean): Unit = ???
  def isOpen(): Boolean = ???
  def lock (position: Long, size: Long, shared: Boolean): Future [FileLock] = ???
  def lock [A] (position: Long, size: Long, shared: Boolean, attachment: A, handler: CompletionHandler [FileLock, _ >: A]): Unit = ???
  def read (dst: ByteBuffer, position: Long): Future [JavaInt] = ???
  def size (): Long = ???
  def truncate (size: Long): AsynchronousFileChannel = ???
  def tryLock (position: Long, size: Long, shared: Boolean): FileLock = ???
  def write (src: ByteBuffer, position: Long): Future [JavaInt] = ???

  override def toString = "AsyncFileMock" + (expectations mkString ("[", ",", "]"), callback)
}
