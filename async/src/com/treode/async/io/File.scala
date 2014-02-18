package com.treode.async.io

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{OpenOption, Path}
import java.nio.file.attribute.FileAttribute
import java.util.concurrent.{Executor, ExecutorService}
import scala.collection.JavaConversions._

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.buffer.PagedBuffer

import Async.{async, whilst}
import Scheduler.toRunnable

/** A file that has useful behavior (flush/fill) and that can be mocked. */
class File private [io] (file: AsynchronousFileChannel) (implicit exec: Executor) {

  private def read (dst: ByteBuffer, pos: Long): Async [Int] =
    async (file.read (dst, pos, _, Callback.IntHandler))

  private def write (src: ByteBuffer, pos: Long): Async [Int] =
    async (file.write (src, pos, _, Callback.IntHandler))

  def fill (input: PagedBuffer, pos: Long, len: Int): Async [Unit] = {
    input.capacity (input.readPos + len)
    var _pos = pos
    var _buf = input.buffer (input.writePos, input.writeableBytes)
    whilst (input.readableBytes < len) {
      for (result <- read (_buf, _pos)) yield {
        if (result < 0)
          throw new Exception ("End of file reached.")
        input.writePos = input.writePos + result
        _pos += result
        if (_buf.remaining == 0)
          _buf = input.buffer (input.writePos, input.writeableBytes)
      }}}

  def fill (input: PagedBuffer, pos: Long, len: Int, cb: Callback [Unit]): Unit = // TODO: remove
    fill (input, pos, len) run (cb)

  def deframe (input: PagedBuffer, pos: Long): Async [Int] = {
    for {
      _ <- fill (input, pos, 4)
      len = input.readInt()
      _ <- fill (input, pos+4, len)
    } yield len
  }

  def deframe (input: PagedBuffer, pos: Long, cb: Callback [Int]): Unit =
    deframe (input, pos) run (cb)

  def flush (output: PagedBuffer, pos: Long): Async [Unit] = {
    var _pos = pos
    var _buf = output.buffer (output.readPos, output.readableBytes)
    whilst (output.readableBytes > 0) {
      for (result <- write (_buf, _pos)) yield {
        if (result < 0)
          throw new Exception ("File write failed.")
        output.readPos = output.readPos + result
        _pos += result
        if (_buf.remaining == 0)
          _buf = output.buffer (output.readPos, output.readableBytes)
      }}}

  def flush (output: PagedBuffer, pos: Long, cb: Callback [Unit]): Unit = // TODO: remove
    flush (output, pos) run (cb)

  def close(): Unit = file.close()
}

object File {

  def open (path: Path, exec: ExecutorService, opts: OpenOption*): File =
    new File (AsynchronousFileChannel.open (path, opts.toSet, exec, null)) (exec)
}
