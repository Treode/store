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
import java.util.ArrayDeque
import java.util.concurrent.Future
import scala.collection.JavaConversions._
import scala.util.{Success, Failure}

import com.treode.async.Callback
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import org.scalatest.Assertions._

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

  private val expectations = new ArrayDeque [Expectation] ()
  private var completion: Callback [Int] = null

  def completeLast (v: Int) (implicit scheduler: StubScheduler) {
    completion.pass (v)
    scheduler.run()
  }

  def failLast (t: Throwable) (implicit scheduler: StubScheduler) {
    completion.fail (t)
    scheduler.run()
  }

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
    require (completion == null, "Pending callback on file.")
    require (!expectations.isEmpty, "No expectations.")
    expectations.remove().read (dst, position)
    completion = {
      case Success (result) =>
        completion = null;
        if (result > 0)
          dst.position (dst.position + result)
        handler.completed (result, attachment)
      case Failure (thrown) =>
        completion = null
        handler.failed (thrown, attachment)
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
    require (completion == null, "Pending callback on file.")
    require (!expectations.isEmpty, "No expectations.")
    expectations.remove().write (src, position)
    completion = {
      case Success (result) =>
        completion = null
        if (result > 0)
          src.position (src.position + result)
        handler.completed (result, attachment)
      case Failure (thrown) =>
        completion = null
        handler.failed (thrown, attachment)
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

  override def toString = "AsyncFileMock" + (expectations mkString ("[", ",", "]"), completion)
}
