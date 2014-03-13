package com.treode.async.io

import java.io.EOFException
import java.util.Arrays
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncConversions, Callback, CallbackCaptor, StubScheduler}
import com.treode.buffer.PagedBuffer

import Async.async
import AsyncConversions._

class StubFile (implicit scheduler: StubScheduler) extends File (null) {

  private var data = new Array [Byte] (0)
  private var _last: Callback [Unit] = null

  def last: Callback [Unit] = _last

  var stop: Boolean = false

  private def _stop (f: Callback [Unit] => Any): Async [Unit] =
    async { cb =>
      if (stop) {
        require (_last == null)
        _last = {
          case Success (v) =>
            _last = null
            f (cb)
          case Failure (t) =>
            _last = null
            cb.fail (t)
        }
      } else {
        f (cb)
      }}

  override def fill (input: PagedBuffer, pos: Long, len: Int): Async [Unit] =
    _stop { cb =>
      try {
        require (pos + len < Int.MaxValue)
        if (len <= input.readableBytes) {
          scheduler.pass (cb, ())
        } else if (data.length < pos) {
          scheduler.fail (cb, new EOFException)
        } else  {
          input.capacity (input.readPos + len)
          val p = pos.toInt + input.readableBytes
          val n = math.min (data.length - p, input.writeableBytes)
          input.writeBytes (data, pos.toInt + input.readableBytes, n)
          if (data.length < pos + len) {
            val e = new EOFException
            scheduler.fail (cb, e)
          } else {
            scheduler.pass (cb, ())
          }}
      } catch {
        case t: Throwable => scheduler.fail (cb, t)
      }}

  override def flush (output: PagedBuffer, pos: Long): Async [Unit] =
    _stop { cb =>
      try {
        require (pos + output.readableBytes < Int.MaxValue)
        if (data.length < pos + output.readableBytes)
          data = Arrays.copyOf (data, pos.toInt + output.readableBytes)
        output.readBytes (data, pos.toInt, output.readableBytes)
        scheduler.pass (cb, ())
      } catch {
        case t: Throwable => scheduler.fail (cb, t)
      }}

  override def toString = s"StubFile(size=${data.length})"
}
