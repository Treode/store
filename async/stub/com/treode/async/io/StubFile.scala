package com.treode.async.io

import java.io.EOFException
import java.util.Arrays

import com.treode.async.{Callback, CallbackCaptor, StubScheduler}
import com.treode.buffer.PagedBuffer

class StubFile (implicit scheduler: StubScheduler) extends File (null, scheduler) {

  private var data = new Array [Byte] (0)
  private var _last: Callback [Unit] = null

  def last: Callback [Unit] = _last

  var stop: Boolean = false

  private def _stop (cb: Callback [Unit]) (f: => Unit) {
    if (stop) {
      require (_last == null)
      _last = new Callback [Unit] {
        def pass (v: Unit): Unit = {
          _last = null
          f
        }
        def fail (t: Throwable): Unit = {
          _last = null
          cb.fail (t)
        }}
    } else {
      f
    }}

  override def fill (input: PagedBuffer, pos: Long, len: Int, cb: Callback [Unit]): Unit =
    _stop (cb) {
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

  def fill (input: PagedBuffer, pos: Long, len: Int): Unit =
    CallbackCaptor.pass [Unit] (fill (input, pos, len, _))

  override def flush (output: PagedBuffer, pos: Long, cb: Callback [Unit]): Unit =
    _stop (cb) {
      try {
        require (pos + output.readableBytes < Int.MaxValue)
        if (data.length < pos + output.readableBytes)
          data = Arrays.copyOf (data, pos.toInt + output.readableBytes)
        output.readBytes (data, pos.toInt, output.readableBytes)
        scheduler.pass (cb, ())
      } catch {
        case t: Throwable => scheduler.fail (cb, t)
      }}

  def flush (output: PagedBuffer, pos: Long): Unit =
    CallbackCaptor.pass [Unit] (flush (output, pos, _))

  override def toString = s"StubFile(size=${data.length})"
}
