package com.treode.async.io

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousSocketChannel}
import java.net.SocketAddress
import java.util.concurrent.{Executor, TimeUnit}

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.buffer.PagedBuffer

import Async.{async, whilst}
import TimeUnit.MILLISECONDS

/** A socket that has useful behavior (flush/fill) and that can be mocked. */
class Socket (socket: AsynchronousSocketChannel) (implicit exec: Executor) {

  private def execute (cb: Callback [Unit]): Unit =
    exec.execute (Scheduler.toRunnable (cb, ()))

  private def fail (cb: Callback [Unit], t: Throwable): Unit =
    exec.execute (Scheduler.toRunnable (cb, t))

  def connect (addr: SocketAddress): Async [Unit] =
    async { cb =>
      try {
        socket.connect (addr, cb, Callback.UnitHandler)
      } catch {
        case t: Throwable => cb.fail (t)
      }}

  def close(): Unit =
    socket.close()

  private def read (dsts: Array [ByteBuffer]): Async [Long] =
    async (socket.read (dsts, 0, dsts.length, -1, MILLISECONDS, _, Callback.LongHandler))

  private def write (srcs: Array [ByteBuffer]): Async [Long] =
    async (socket.write (srcs, 0, srcs.length, -1, MILLISECONDS, _, Callback.LongHandler))

  def fill (input: PagedBuffer, len: Int): Async [Unit] = {
    input.capacity (input.writePos + len)
    val bufs = input.buffers (input.writePos, input.writeableBytes)
    whilst (input.readableBytes < len) {
      for (result <- read (bufs)) yield {
        require (result <= Int.MaxValue)
        if (result < 0)
          throw new Exception ("End of file reached.")
        input.writePos = input.writePos + result.toInt
      }}}

  def fill (input: PagedBuffer, len: Int, cb: Callback [Unit]): Unit = // TODO: remove
    fill (input, len) run (cb)

  def deframe (input: PagedBuffer): Async [Int] = {
    for {
      _ <- fill (input, 4)
      len = input.readInt()
      _ <- fill (input, len)
    } yield len
  }

  def deframe (input: PagedBuffer, cb: Callback [Int]): Unit =
    deframe (input) run (cb)

  def flush (output: PagedBuffer): Async [Unit] = {
    val bufs = output.buffers (output.readPos, output.readableBytes)
    whilst (output.readableBytes > 0) {
      for (result <- write (bufs)) yield {
        require (result <= Int.MaxValue)
        if (result < 0)
          throw new Exception ("File write failed.")
        output.readPos = output.readPos + result.toInt
      }}}

  def flush (output: PagedBuffer, cb: Callback [Unit]): Unit = // TODO: remove
    flush (output) run (cb)
}

object Socket {

  def open (group: AsynchronousChannelGroup, exec: Executor): Socket =
    new Socket (AsynchronousSocketChannel.open (group)) (exec)
}
