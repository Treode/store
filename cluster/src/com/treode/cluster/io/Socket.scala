package com.treode.cluster.io

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousSocketChannel}
import java.net.SocketAddress
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.concurrent.Callback

/** Something that can be mocked in tests. */
class Socket (socket: AsynchronousSocketChannel) {

  /** For testing mocks only. */
  def this() = this (null)

  def connect (addr: SocketAddress, cb: Callback [Unit]): Unit =
    Callback.guard (cb) (socket.connect (addr, cb, Callback.UnitHandler))

  def close(): Unit =
    socket.close()

  private class SocketFiller (input: Input, length: Int, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] val buffer = KryoPool.wrap (input)

    def fill() {
      if (length <= buffer.position - input.position)
        cb()
      else
        socket.read (buffer, -1, MILLISECONDS, this, Callback.IntHandler)
    }

    def pass (result: Int) {
      if (result < 0) {
        socket.close()
      } else {
        input.setLimit (input.limit + result)
        fill()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }

  def fill (input: Input, length: Int, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      if (length <= input.limit - input.position) {
        cb()
      } else {
        KryoPool.ensure (input, length)
        new SocketFiller (input, length, cb) .fill()
      }}

  private class SocketFlusher (output: Output, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] val buffer = KryoPool.wrap (output)

    def flush() {
      if (buffer.remaining == 0) {
        output.setPosition (0)
        cb()
      } else {
        socket.write (buffer, -1, MILLISECONDS, this, Callback.IntHandler)
      }}

    def pass (result: Int) {
      if (result < 0)
        socket.close()
      else
        flush()
    }

    def fail (t: Throwable) = cb.fail (t)
  }

  def flush (output: Output, cb: Callback [Unit]): Unit =
    Callback.guard (cb) (new SocketFlusher (output, cb) .flush())
}

object Socket {

  def open (group: AsynchronousChannelGroup): Socket =
    new Socket (AsynchronousSocketChannel.open (group))
}
