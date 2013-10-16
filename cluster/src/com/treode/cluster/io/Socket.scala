package com.treode.cluster.io

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousSocketChannel}
import java.net.SocketAddress

import com.treode.cluster.concurrent.Callback

/** Something that can be mocked in tests. */
class Socket (socket: AsynchronousSocketChannel) {

  /** For testing mocks only. */
  def this() = this (null)

  def connect (addr: SocketAddress, cb: Callback [Unit]): Unit =
    socket.connect (addr, cb, Callback.UnitHandler)

  def read (dst: ByteBuffer, cb: Callback [Int]): Unit =
    socket.read (dst, cb, Callback.IntHandler)

  def write (src: ByteBuffer, cb: Callback [Int]): Unit =
    socket.write (src, cb, Callback.IntHandler)

  def close(): Unit =
    socket.close()
}

object Socket {

  def open (group: AsynchronousChannelGroup): Socket =
    new Socket (AsynchronousSocketChannel.open (group))
}
