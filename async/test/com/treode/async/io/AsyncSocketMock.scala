package com.treode.async.io

import java.lang.{Integer => JavaInt, Long => JavaLong}
import java.net.{SocketAddress, SocketOption}
import java.nio.ByteBuffer
import java.nio.channels._
import java.util.{Set => JavaSet, ArrayDeque}
import java.util.concurrent.{Future, TimeUnit}
import scala.collection.JavaConversions._

import com.treode.async.Callback
import org.scalatest.Assertions.assert

/** ScalaMock refuses to mock AsynchronousSocketChannel. */
class AsyncSocketMock extends AsynchronousSocketChannel (null) {

  private class Expectation {
    def read (dst: ByteBuffer) {
      Thread.dumpStack()
      assert (false, "Unexpected socket read: " + AsyncSocketMock.this)
    }
    def write (src: ByteBuffer) {
      Thread.dumpStack()
      assert (false, "Unexpected socket write: " + AsyncSocketMock.this)
    }
    def close() {
      Thread.dumpStack()
      assert (false, "Unexpected socket close: " + AsyncSocketMock.this)
    }
    override def toString = "Expecting nothing."
  }

  private var expectations = new ArrayDeque [Expectation] ()

  private var callback: Callback [Int] = null

  def completeLast (v: Int) = callback (v)

  def failLast (t: Throwable) = callback.fail (t)

  def expectRead (bufPos: Int, bufLimit: Int) {
    expectations.add (new Expectation {
      override def read (dst: ByteBuffer) {
        assert (dst.position == bufPos, s"Expected buffer position $bufPos but got ${dst.position}")
        assert (dst.limit == bufLimit, s"Expected buffer limit $bufLimit but got ${dst.limit}")
      }
      override def toString = "Expecting read" + (bufPos, bufLimit)
    })
  }

  def read [A] (dsts: Array [ByteBuffer], offset: Int, length: Int, timeout: Long, unit: TimeUnit,
      attachment: A, handler: CompletionHandler [JavaLong, _ >: A]) {
    require (offset == 0, "This mock is not that sophisticated.")
    require (callback == null, "Pending callback on socket.")
    require (!expectations.isEmpty, "No expectations.")
    val dst = dsts (0)
    expectations.remove().read (dst)
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
      override def toString = s"Pending read($dst)"
    }}

  def expectWrite (bufPos: Int, bufLimit: Int) {
    expectations.add (new Expectation {
      override def write (src: ByteBuffer) {
        assert (src.position == bufPos, s"Expected buffer position $bufPos but got ${src.position}")
        assert (src.limit == bufLimit, s"Expected buffer limit $bufLimit but got ${src.limit}")
      }
      override def toString = "Expecting write" + (bufPos, bufLimit)
    })
  }

  def write [A] (srcs: Array [ByteBuffer], offset: Int, length: Int, timout: Long, unit: TimeUnit,
      attachment: A, handler: CompletionHandler [JavaLong, _ >: A]) {
    require (offset == 0, "This mock is not that sophisticated.")
    require (callback == null, "Pending callback on socket.")
    require (!expectations.isEmpty, "No expectations.")
    val src = srcs (0)
    expectations.remove().write (src)
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
      override def toString = s"Pending write($src)"
    }}

  def expectClose() {
    expectations.add (new Expectation {
      override def close() = ()
      override def toString = "Expecting close()"
    })
  }

  def close() {
    if (callback != null)
      callback.fail (new AsynchronousCloseException)
    require (!expectations.isEmpty, "No expectations.")
    expectations.remove().close()
  }

  def bind (local: SocketAddress): AsynchronousSocketChannel = ???
  def connect (remote: SocketAddress): Future [Void] = ???
  def connect [A] (remote: SocketAddress, attachment: A, handler: CompletionHandler [Void, _ >: A]): Unit = ???
  def getRemoteAddress(): SocketAddress = ???
  def read [A] (dst: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler [JavaInt, _ >: A]): Unit = ???
  def read (dst: ByteBuffer): java.util.concurrent.Future[Integer] = ???
  def setOption [T] (name: java.net.SocketOption[T], value: T): AsynchronousSocketChannel = ???
  def shutdownInput(): AsynchronousSocketChannel = ???
  def shutdownOutput(): AsynchronousSocketChannel = ???
  def write [A] (src: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler [JavaInt, _ >: A]): Unit = ???
  def write (src: ByteBuffer): Future [Integer] = ???
  def isOpen(): Boolean = ???
  def getLocalAddress(): SocketAddress = ???
  def getOption [T] (name: SocketOption [T]): T = ???
  def supportedOptions(): JavaSet [SocketOption[_]] = ???

  override def toString = "AsyncSocketStub" + (expectations mkString ("[", ",", "]"), callback)
}
