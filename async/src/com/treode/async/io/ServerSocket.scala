package com.treode.async.io

import java.nio.channels._
import java.net.SocketAddress

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._

import Async.async

/** Something that can be mocked in tests. */
class ServerSocket (socket: AsynchronousServerSocketChannel) (implicit scheduler: Scheduler) {

  /** Adapts Callback to Java's NIO CompletionHandler. */
  object SocketHandler extends CompletionHandler [AsynchronousSocketChannel, Callback [Socket]] {
    def completed (v: AsynchronousSocketChannel, cb: Callback [Socket]) = cb.pass (new Socket (v))
    def failed (t: Throwable, cb: Callback [Socket]) = cb.fail (t)
  }

  def bind (addr: SocketAddress): Unit =
    socket.bind (addr)

  def accept(): Async [Socket] =
    async { cb =>
      try {
        socket.accept (cb, SocketHandler)
      } catch {
        case t: Throwable => cb.fail (t)
      }}

  def close(): Unit =
    socket.close()
}

object ServerSocket {

  def open (group: AsynchronousChannelGroup, scheduler: Scheduler): ServerSocket =
    new ServerSocket (AsynchronousServerSocketChannel.open (group)) (scheduler)
}
