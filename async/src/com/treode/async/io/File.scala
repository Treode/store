package com.treode.async.io

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{OpenOption, Path}
import java.nio.file.attribute.FileAttribute
import java.util.concurrent.{Executor, ExecutorService}
import scala.collection.JavaConversions._

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.buffer.PagedBuffer

import Async.async

/** A file that has useful behavior (flush/fill) and that can be mocked. */
class File private [io] (file: AsynchronousFileChannel, exec: Executor) {

  private def execute (cb: Callback [Unit]): Unit =
    exec.execute (Scheduler.toRunnable (cb, ()))

  private def fail (cb: Callback [Unit], t: Throwable): Unit =
    exec.execute (Scheduler.toRunnable (cb, t))

  private class Filler (input: PagedBuffer, pos: Long, len: Int, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] var bytebuf = input.buffer (input.writePos, input.writeableBytes)
    private [this] var _pos = pos

    def fill() {
      if (len <= input.readableBytes)
        execute (cb)
      else {
        if (bytebuf.remaining == 0)
          bytebuf = input.buffer (input.writePos, input.writeableBytes)
        file.read (bytebuf, _pos, this, Callback.IntHandler)
      }}

    def pass (result: Int) {
      if (result < 0) {
        File.this.fail (cb, new Exception ("End of file reached."))
      } else {
        input.writePos = input.writePos + result
        _pos += result
        fill()
      }}

    def fail (t: Throwable) = File.this.fail (cb, t)
  }

  def fill (input: PagedBuffer, pos: Long, len: Int, cb: Callback [Unit]): Unit =
    try {
      if (len <= input.readableBytes) {
        execute (cb)
      } else {
        input.capacity (input.readPos + len)
        new Filler (input, pos, len, cb) .fill()
      }
    } catch {
      case t: Throwable => fail (cb, t)
    }

  def fillA (input: PagedBuffer, pos: Long, len: Int): Async [Unit] =
    try {
      if (len <= input.readableBytes) {
        async (execute (_))
      } else {
        input.capacity (input.readPos + len)
        async (cb => new Filler (input, pos, len, cb) .fill())
      }
    } catch {
      case t: Throwable =>
        async (fail (_, t))
    }

  def deframe (input: PagedBuffer, pos: Long, cb: Callback [Int]) {
    def body (len: Int) = new Callback [Unit] {
      def pass (v: Unit) = cb (len)
      def fail (t: Throwable) = cb.fail (t)
    }
    val header = new Callback [Unit] {
      def pass (v: Unit) = {
        val len = input.readInt()
        fill (input, pos+4, len, body (len))
      }
      def fail (t: Throwable) = cb.fail (t)
    }
    fill (input, pos, 4, header)
  }

  def deframe (input: PagedBuffer, pos: Long): Async [Int] = {
    for {
      _ <- fillA (input, pos, 4)
      len = input.readInt()
      _ <- fillA (input, pos+4, len)
    } yield len
  }

  private class Flusher (output: PagedBuffer, pos: Long, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] var bytebuf = output.buffer (output.readPos, output.readableBytes)
    private [this] var _pos = pos

    def flush() {
      if (output.readableBytes == 0) {
        //buffer.release()
        execute (cb)
      } else {
        if (bytebuf.remaining == 0)
          bytebuf = output.buffer (output.readPos, output.readableBytes)
        file.write (bytebuf, _pos, this, Callback.IntHandler)
      }}

    def pass (result: Int) {
      if (result < 0) {
        File.this.fail (cb, new Exception ("File write failed."))
      } else {
        output.readPos = output.readPos + result
        _pos += result
        flush()
      }}

    def fail (t: Throwable) = File.this.fail (cb, t)
  }

  def flush (output: PagedBuffer, pos: Long, cb: Callback [Unit]): Unit =
    try {
      if (output.readableBytes == 0)
        execute (cb)
      else
        new Flusher (output, pos, cb) .flush()
    } catch {
      case t: Throwable => fail (cb, t)
    }

  def close(): Unit = file.close()
}

object File {

  def open (path: Path, exec: ExecutorService, opts: OpenOption*): File =
    new File (AsynchronousFileChannel.open (path, opts.toSet, exec, null), exec)
}
