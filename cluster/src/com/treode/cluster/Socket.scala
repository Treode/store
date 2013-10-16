package com.treode.cluster

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousSocketChannel, CompletionHandler}
import java.net.SocketAddress
import java.lang.{Integer => JavaInt}

import com.treode.cluster.concurrent.Callback

/** Something that can be mocked in tests. */
private class Socket (socket: AsynchronousSocketChannel) {

  // For testing only.
  def this() = this (null)

  def connect (addr: SocketAddress, cb: Callback [Unit]): Unit =
    socket.connect (addr, cb, Socket.UnitHandler)

  def read (dst: ByteBuffer, cb: Callback [Int]): Unit =
    socket.read (dst, cb, Socket.IntHandler)

  def write (src: ByteBuffer, cb: Callback [Int]): Unit =
    socket.write (src, cb, Socket.IntHandler)

  def close(): Unit =
    socket.close()
}

private object Socket {

  def open (group: AsynchronousChannelGroup): Socket =
    new Socket (AsynchronousSocketChannel.open (group))

  object IntHandler extends CompletionHandler [JavaInt, Callback [Int]]{
    def completed (v: JavaInt, cb: Callback [Int]) = cb (v)
    def failed (t: Throwable, cb: Callback [Int]) = cb.fail (t)
  }

  object UnitHandler extends CompletionHandler [Void, Callback [Unit]]{
    def completed (v: Void, cb: Callback [Unit]) = cb()
    def failed (t: Throwable, cb: Callback [Unit]) = cb.fail (t)
  }}
