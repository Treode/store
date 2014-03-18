package com.treode.async.io

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{OpenOption, Path}
import java.nio.file.attribute.FileAttribute
import java.util.concurrent.{Executor, ExecutorService}
import scala.collection.JavaConversions._

import com.google.common.hash.{HashCode, HashFunction}
import com.treode.async.{Async, Callback, Scheduler, Whilst}
import com.treode.buffer.PagedBuffer

import Async.async

/** A file that has useful behavior (flush/fill) and that can be mocked. */
class File private [io] (file: AsynchronousFileChannel) (implicit exec: Executor) {

  private def read (dst: ByteBuffer, pos: Long): Async [Int] =
    async (file.read (dst, pos, _, Callback.IntHandler))

  private def write (src: ByteBuffer, pos: Long): Async [Int] =
    async (file.write (src, pos, _, Callback.IntHandler))

  private def whilst = new Whilst (exec)

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

  def deframe (input: PagedBuffer, pos: Long): Async [Int] = {
    for {
      _ <- fill (input, pos, 4)
      len = input.readInt()
      _ <- fill (input, pos+4, len)
    } yield len
  }

  def deframe (hashf: HashFunction, input: PagedBuffer, pos: Long): Async [Int] = {
    val head = 4 + (hashf.bits >> 3)
    for {
      _ <- fill (input, pos, head)
      len = input.readInt()
      _ <- fill (input, pos+head, len)
    } yield {
      val bytes = new Array [Byte] (hashf.bits >> 3)
      input.readBytes (bytes, 0, bytes.length)
      val expected = HashCode.fromBytes (bytes)
      val found = input.hash (input.readPos, len, hashf)
      if (found != expected) {
        input.readPos += len
        throw new HashMismatchException
      }
      len
    }}

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

  def close(): Unit = file.close()
}

object File {

  def open (path: Path, exec: ExecutorService, opts: OpenOption*): File = {
    val attrs = new Array [FileAttribute [_]] (0)
    new File (AsynchronousFileChannel.open (path, opts.toSet, exec, attrs: _*)) (exec)
  }}
