package com.treode.async.io

import java.io.EOFException
import java.util.Arrays

import com.treode.async.{Callback, Scheduler}
import com.treode.buffer.PagedBuffer

class StubFile (scheduler: Scheduler) extends File {

  private var data = new Array [Byte] (0)

  override def fill (input: PagedBuffer, pos: Long, len: Int, cb: Callback [Unit]): Unit =
    try {
      require (pos + len < Int.MaxValue)
      if (len <= input.readableBytes) {
        scheduler.execute (cb())
      } else if (data.length < pos) {
        scheduler.execute (cb.fail (new EOFException))
      } else  {
        input.capacity (input.writePos + len)
        val n = math.min (data.length - pos.toInt, input.writeableBytes)
        input.writeBytes (data, pos.toInt, n)
        if (data.length < pos + len)
          scheduler.execute (cb.fail (new EOFException))
        else
          scheduler.execute (cb())
      }
    } catch {
      case t: Throwable => scheduler.execute (cb.fail (t))
    }

  override def flush (output: PagedBuffer, pos: Long, cb: Callback [Unit]): Unit =
    try {
      require (pos + output.readableBytes < Int.MaxValue)
      if (data.length < pos + output.readableBytes)
        data = Arrays.copyOf (data, pos.toInt + output.readableBytes)
      output.readBytes (data, pos.toInt, output.readableBytes)
      scheduler.execute (cb())
    } catch {
      case t: Throwable => scheduler.execute (cb.fail (t))
    }

  override def toString = s"StubFile(size=${data.length})"
}
