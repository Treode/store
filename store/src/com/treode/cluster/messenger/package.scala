package com.treode.cluster

import java.nio.channels.{AsynchronousSocketChannel => Socket, CompletionHandler}
import java.lang.{Long => JavaLong}
import java.util.concurrent.TimeUnit._

import io.netty.buffer.{PooledByteBufAllocator, ByteBuf}
import com.treode.cluster.fiber.Scheduler

package object messenger {

  private [messenger] val ByteBufPool = PooledByteBufAllocator.DEFAULT

  private class Ensurer (socket: Socket, input: ByteBuf, length: Int, callback: CompletionHandler [Void, Void])
      extends CompletionHandler [JavaLong, Void] {

    def ensure() {
      if (length <= input.readableBytes) {
        callback.completed (null, null)
      } else {
        if (input.writableBytes < length)
          input.capacity (input.writerIndex + length)
        val buffers = input.nioBuffers (input.writerIndex, input.writableBytes)
        socket.read (buffers, 0, buffers.length, 0, MILLISECONDS, null, this)
      }}

    def completed (result: JavaLong, attachment: Void) {
      input.writerIndex (input.writerIndex + result.toInt)
      ensure()
    }

    def failed (exc: Throwable, attachment: Void) {
      callback.failed (exc, null)
    }}

  private [messenger] def ensure (socket: Socket, input: ByteBuf, length: Int,
      callback: CompletionHandler [Void, Void]): Unit =
    new Ensurer (socket, input, length, callback) .ensure()

  private class Flusher (socket: Socket, output: ByteBuf, callback: CompletionHandler [Void, Void])
      extends CompletionHandler [JavaLong, Void] {

    def flush() {
      if (output.readableBytes == 0) {
        output.release()
        callback.completed (null, null)
      } else {
        val buffers = output.nioBuffers (output.readerIndex, output.readableBytes)
        socket.write (buffers, 0, buffers.length, 0, MILLISECONDS, null, this)
      }}

    def completed (result: JavaLong, attachment: Void) {
      output.readerIndex (output.readerIndex + result.toInt)
      flush()
    }

    def failed (exc: Throwable, attachment: Void) {
      output.release()
      callback.failed (exc, null)
    }}

  private [messenger] def flush (socket: Socket, output: ByteBuf, callback: CompletionHandler [Void, Void]): Unit =
    new Flusher (socket, output, callback) .flush()

}
