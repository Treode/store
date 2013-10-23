package com.treode.cluster.io

import java.nio.channels._
import java.net.SocketAddress

import com.treode.concurrent.Callback

/** Something that can be mocked in tests. */
class ServerSocket (socket: AsynchronousServerSocketChannel) {

  /** For testing mocks only. */
  def this() = this (null)

  def bind (addr: SocketAddress): Unit =
    socket.bind (addr)

  def accept (cb: Callback [Socket]): Unit =
    Callback.guard (cb) (socket.accept (cb, ServerSocket.SocketHandler))

  def close(): Unit =
    socket.close()
}

object ServerSocket {

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object SocketHandler extends CompletionHandler [AsynchronousSocketChannel, Callback [Socket]] {
    def completed (v: AsynchronousSocketChannel, cb: Callback [Socket]) = cb (new Socket (v))
    def failed (t: Throwable, cb: Callback [Socket]) = cb.fail (t)
  }

  def open (group: AsynchronousChannelGroup): ServerSocket =
    new ServerSocket (AsynchronousServerSocketChannel.open (group))
}
