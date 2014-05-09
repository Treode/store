package com.treode.cluster

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousCloseException}
import scala.util.{Failure, Success}

import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.{Socket, ServerSocket}
import com.treode.buffer.PagedBuffer

private class Listener (
  localId: HostId,
  localAddr: SocketAddress,
  group: AsynchronousChannelGroup,
  peers: PeerRegistry
) (implicit
    scheduler: Scheduler
) {

  private var server: ServerSocket = null

  private def sayHello (socket: Socket, input: PagedBuffer, remoteId: HostId) {
    val buffer = PagedBuffer (12)
    Hello.pickler.pickle (Hello (localId), buffer)
    socket.flush (buffer) run {
      case Success (v) =>
        peers.get (remoteId) connect (socket, input, remoteId)
      case Failure (t) =>
        log.exceptionWhileGreeting (t)
        socket.close()
    }}

  private def hearHello (socket: Socket) {
    val buffer = PagedBuffer (12)
    socket.fill (buffer, 9) run {
      case Success (v) =>
        val Hello (remoteId) = Hello.pickler.unpickle (buffer)
        sayHello (socket, buffer, remoteId)
      case Failure (t) =>
        log.exceptionWhileGreeting (t)
        socket.close()
    }}

  private def loop() {
    server.accept() run {
      case Success (socket) =>
        scheduler.execute (hearHello (socket))
        loop()
      case Failure (e: AsynchronousCloseException) =>
        server.close()
      case Failure (e: Throwable) =>
        log.recyclingMessengerSocket (e)
        server.close()
        scheduler.delay (200) (startup())
        throw e
    }}

  def startup() {
    server = ServerSocket.open (group)
    server.bind (localAddr)
    if (localAddr.isInstanceOf [InetSocketAddress])
      println ("Accepting messenger connections on " + localAddr)
    loop()
  }

  def shutdown(): Unit =
    if (server != null) server.close()
}
