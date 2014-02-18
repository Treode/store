package com.treode.async.io

import java.lang.{Integer => JavaInt}
import java.nio.ByteBuffer
import java.nio.channels._
import java.util.Arrays
import java.util.concurrent.Future
import scala.util.Random

import com.treode.async.{Callback, StubScheduler}

class AsyncFileStub (implicit random: Random, scheduler: StubScheduler) extends AsynchronousFileChannel {

  private var data = new Array [Byte] (0)

  def read [A] (dst: ByteBuffer, position: Long, attachment: A, handler: CompletionHandler [JavaInt, _ >: A]) {
    require (position <= Int.MaxValue)
    if (position > data.length) {
      handler.completed (-1, attachment)
      scheduler.runTasks()
    } else {
      val length = random.nextInt (math.min (dst.remaining, data.size - position.toInt)) + 1
      System.arraycopy (data, position.toInt, dst.array, dst.position, length)
      dst.position (dst.position + length)
      handler.completed (length, attachment)
      scheduler.runTasks()
    }}

  def write [A] (src: ByteBuffer, position: Long, attachment: A, handler: CompletionHandler [JavaInt, _ >: A]) {
    require (position <= Int.MaxValue)
    val length = random.nextInt (src.remaining) + 1
    if (position + length > data.length)
      data = Arrays.copyOf (data, position.toInt + length)
    System.arraycopy (src.array, src.position, data, position.toInt, length)
    src.position (src.position + length)
    handler.completed (length, attachment)
    scheduler.runTasks()
  }

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

  override def toString = "AsyncFileStub"
}
