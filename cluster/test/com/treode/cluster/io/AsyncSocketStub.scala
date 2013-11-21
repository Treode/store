package com.treode.cluster.io

import java.lang.{Integer => JavaInt, Long => JavaLong}
import java.net.{SocketAddress, SocketOption}
import java.nio.ByteBuffer
import java.nio.channels._
import java.util.{Arrays, Set => JavaSet}
import java.util.concurrent.{Future, TimeUnit}
import scala.util.Random

import com.treode.async.Callback

class AsyncSocketStub (random: Random) extends AsynchronousSocketChannel (null) {

  private var data = new Array [Byte] (0)

  def read [A] (dst: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A,
      handler: CompletionHandler [JavaInt, _ >: A]) {
    require (data.length > 0)
    val length = random.nextInt (math.min (dst.remaining, data.length)) + 1
    val remaining = data.length - length
    System.arraycopy (data, 0, dst.array, dst.position, length)
    dst.position (dst.position + length)
    val tmp = new Array [Byte] (remaining)
    System.arraycopy (data, length, tmp, 0, remaining)
    data = tmp
    handler.completed (length, attachment)
  }

  def read [A] (dsts: Array [ByteBuffer], offset: Int, length: Int, timeout: Long, unit: TimeUnit,
      attachment: A, handler: CompletionHandler [JavaLong, _ >: A]) {
    require (data.length > 0)
    val total = dsts .map (_.remaining) .sum
    require (total > 0)
    val length = random.nextInt (math.min (total, data.length)) + 1
    var remaining = length
    var position = 0
    val iter = dsts.iterator
    while (iter.hasNext && remaining > 0) {
      val dst = iter.next
      val partial = math.min (dst.remaining, remaining)
      System.arraycopy (data, position, dst.array, dst.position, partial)
      dst.position (dst.position + partial)
      remaining -= partial
      position += partial
    }
    remaining = data.length - length
    val tmp = new Array [Byte] (remaining)
    System.arraycopy (data, position, tmp, 0, remaining)
    data = tmp
    handler.completed (length, attachment)
  }

  def write [A] (src: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A,
      handler: CompletionHandler [JavaInt, _ >: A]) {
    val position = data.length
    val length = random.nextInt (src.remaining) + 1
    data = Arrays.copyOf (data, position + length)
    System.arraycopy (src.array, src.position, data, position, length)
    src.position (src.position + length)
    handler.completed (length, attachment)
  }

  def write [A] (srcs: Array [ByteBuffer], offset: Int, length: Int, timeout: Long, unit: TimeUnit,
      attachment: A, handler: CompletionHandler [JavaLong, _ >: A]) {
    val total = srcs .map (_.remaining) .sum
    require (total > 0)
    val length = random.nextInt (total) + 1
    var remaining = length
    var position = data.length
    data = Arrays.copyOf (data, position + length)
    val iter = srcs.iterator
    while (iter.hasNext && remaining > 0) {
      val src = iter.next
      val partial = math.min (src.remaining, remaining)
      System.arraycopy (src.array, src.position, data, position, partial)
      src.position (src.position + partial)
      remaining -= partial
      position += partial
    }
    handler.completed (length, attachment)
  }

  def bind (local: SocketAddress): AsynchronousSocketChannel = ???
  def close(): Unit = ???
  def connect (remote: SocketAddress): Future [Void] = ???
  def connect [A] (remote: SocketAddress, attachment: A, handler: CompletionHandler [Void, _ >: A]): Unit = ???
  def getRemoteAddress(): SocketAddress = ???
  def read (dst: ByteBuffer): java.util.concurrent.Future[Integer] = ???
  def setOption [T] (name: java.net.SocketOption[T], value: T): AsynchronousSocketChannel = ???
  def shutdownInput(): AsynchronousSocketChannel = ???
  def shutdownOutput(): AsynchronousSocketChannel = ???
  def write (src: ByteBuffer): Future [Integer] = ???
  def isOpen(): Boolean = ???
  def getLocalAddress(): SocketAddress = ???
  def getOption [T] (name: SocketOption [T]): T = ???
  def supportedOptions(): JavaSet [SocketOption[_]] = ???

  override def toString = "AsyncFileStub"
}
