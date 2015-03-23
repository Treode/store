/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.async.io

import java.lang.{Integer => JavaInt}
import java.nio.ByteBuffer
import java.nio.channels._
import java.util.Arrays
import java.util.concurrent.Future
import scala.util.Random

import com.treode.async.Callback
import com.treode.async.stubs.StubScheduler

class AsyncFileStub (implicit random: Random, scheduler: StubScheduler) extends AsynchronousFileChannel {

  private var data = new Array [Byte] (0)

  def read [A] (dst: ByteBuffer, position: Long, attachment: A, handler: CompletionHandler [JavaInt, _ >: A]) {
    require (position <= Int.MaxValue)
    if (position > data.length) {
      handler.completed (-1, attachment)
      scheduler.run()
    } else {
      val length = random.nextInt (math.min (dst.remaining, data.size - position.toInt)) + 1
      System.arraycopy (data, position.toInt, dst.array, dst.position, length)
      dst.position (dst.position + length)
      handler.completed (length, attachment)
      scheduler.run()
    }}

  def write [A] (src: ByteBuffer, position: Long, attachment: A, handler: CompletionHandler [JavaInt, _ >: A]) {
    require (position <= Int.MaxValue)
    val length = random.nextInt (src.remaining) + 1
    if (position + length > data.length)
      data = Arrays.copyOf (data, position.toInt + length)
    System.arraycopy (src.array, src.position, data, position.toInt, length)
    src.position (src.position + length)
    handler.completed (length, attachment)
    scheduler.run()
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
