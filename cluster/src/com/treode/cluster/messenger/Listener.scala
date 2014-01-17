package com.treode.cluster.messenger

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousCloseException}

import com.treode.async.{Callback, Scheduler}
import com.treode.async.io.{Socket, ServerSocket}
import com.treode.buffer.PagedBuffer
import com.treode.cluster.{ClusterEvents, HostId, messenger}
import com.treode.cluster.events.Events
import com.treode.pickle.{pickle, unpickle}

class Listener (
  localId: HostId,
  localAddr: SocketAddress,
  group: AsynchronousChannelGroup,
  peers: PeerRegistry) (
    implicit scheduler: Scheduler,
    events: Events) {

  private var server: ServerSocket = null

  private def sayHello (socket: Socket, input: PagedBuffer, remoteId: HostId) {
    val buffer = PagedBuffer (12)
    pickle (Hello.pickle, Hello (localId), buffer)
    socket.flush (buffer, new Callback [Unit] {
      def pass (v: Unit) {
        peers.get (remoteId) connect (socket, input, remoteId)
      }
      def fail (t: Throwable) {
        events.exceptionWhileGreeting (t)
        socket.close()
      }})
  }

  private def hearHello (socket: Socket) {
    val buffer = PagedBuffer (12)
    socket.fill (buffer, 9, new Callback [Unit] {
      def pass (v: Unit) {
        val Hello (remoteId) = unpickle (Hello.pickle, buffer)
        sayHello (socket, buffer, remoteId)
      }
      def fail (t: Throwable) {
        events.exceptionWhileGreeting (t)
        socket.close()
      }})
  }

  private def loop() {
    server.accept (new Callback [Socket] {
      def pass (socket: Socket) {
        scheduler.execute (hearHello (socket))
        loop()
      }
      def fail (t: Throwable) {
        t match {
          case e: AsynchronousCloseException =>
            server.close()
          case e: Throwable =>
            events.recyclingMessengerSocket (e)
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
