package com.treode.cluster

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousCloseException}

import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.{Socket, ServerSocket}
import com.treode.buffer.PagedBuffer

class Listener (
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
    socket.flush (buffer) run (new Callback [Unit] {
      def pass (v: Unit) {
        peers.get (remoteId) connect (socket, input, remoteId)
      }
      def fail (t: Throwable) {
        log.exceptionWhileGreeting (t)
        socket.close()
      }})
  }

  private def hearHello (socket: Socket) {
    val buffer = PagedBuffer (12)
    socket.fill (buffer, 9) run (new Callback [Unit] {
      def pass (v: Unit) {
        val Hello (remoteId) = Hello.pickler.unpickle (buffer)
        sayHello (socket, buffer, remoteId)
      }
      def fail (t: Throwable) {
        log.exceptionWhileGreeting (t)
        socket.close()
      }})
  }

  private def loop() {
    server.accept() run (new Callback [Socket] {
      def pass (socket: Socket) {
        scheduler.execute (hearHello (socket))
        loop()
      }
      def fail (t: Throwable) {
        t match {
          case e: AsynchronousCloseException =>
            server.close()
          case e: Throwable =>
            log.recyclingMessengerSocket (e)
            server.close()
            scheduler.delay (200) (startup())
            throw e
        }
        server.close()
      }})
  }

  def startup() {
    server = ServerSocket.open (group, scheduler)
    server.bind (localAddr)
    if (localAddr.isInstanceOf [InetSocketAddress])
      println ("Accepting messenger connections on " + localAddr)
    loop()
  }

  def shutdown(): Unit =
    if (server != null) server.close()
}
