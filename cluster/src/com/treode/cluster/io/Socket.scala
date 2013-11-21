package com.treode.cluster.io

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousSocketChannel}
import java.net.SocketAddress
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.treode.async.Callback
import com.treode.pickle.Buffer

/** A socket that has useful behavior (flush/fill) and that can be mocked. */
class Socket (socket: AsynchronousSocketChannel) {

  /** For testing mocks only. */
  def this() = this (null)

  def connect (addr: SocketAddress, cb: Callback [Unit]): Unit =
    Callback.guard (cb) (socket.connect (addr, cb, Callback.UnitHandler))

  def close(): Unit =
    socket.close()

  private class Filler (input: Buffer, length: Int, cb: Callback [Unit])
  extends Callback [Long] {

    def fill() {
      if (length <= input.readableBytes)
        cb()
      else {
        val bytebufs = input.buffers (input.writePos, input.writeableBytes)
        socket.read (bytebufs, 0, bytebufs.length, -1, MILLISECONDS, this, Callback.LongHandler)
      }}

    def pass (result: Long) {
      require (result <= Int.MaxValue)
      if (result < 0) {
        cb.fail (new Exception ("End of file reached."))
      } else {
        input.writePos = input.writePos + result.toInt
        fill()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }

  def fill (input: Buffer, length: Int, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      if (length <= input.readableBytes) {
        cb()
      } else {
        input.capacity (input.readPos + length)
        new Filler (input, length, cb) .fill()
      }}

  private class Flusher (output: Buffer, cb: Callback [Unit])
  extends Callback [Long] {

    def flush() {
      if (output.readableBytes == 0) {
        //buffer.release()
        cb()
      } else {
        val bytebufs = output.buffers (output.readPos, output.readableBytes)
        socket.write (bytebufs, 0, bytebufs.length, -1, MILLISECONDS, this, Callback.LongHandler)
      }}

    def pass (result: Long) {
      require (result <= Int.MaxValue)
      if (result < 0) {
        cb.fail (new Exception ("File write failed."))
      } else {
        output.readPos = output.readPos + result.toInt
        flush()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }


  def flush (output: Buffer, cb: Callback [Unit]): Unit =
    Callback.guard (cb) (new Flusher (output, cb) .flush())
}

object Socket {

  def open (group: AsynchronousChannelGroup): Socket =
    new Socket (AsynchronousSocketChannel.open (group))
}
