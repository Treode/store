package com.treode.cluster.messenger

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.{
  AsynchronousServerSocketChannel => ServerSocket,
  AsynchronousSocketChannel => Socket,
  AsynchronousChannelGroup,
  AsynchronousCloseException,
  CompletionHandler}

import com.treode.cluster.{ClusterEvents, HostId, messenger}
import com.treode.cluster.events.Events
import com.treode.cluster.fiber.Scheduler
import com.treode.pickle._
import io.netty.buffer.ByteBuf

class Listener (
  localId: HostId,
  localAddr: SocketAddress,
  group: AsynchronousChannelGroup,
  peers: PeerRegistry) (
    implicit scheduler: Scheduler,
    events: Events) {

  private var server: ServerSocket = null

  private def sayHello (socket: Socket, input: ByteBuf, remoteId: HostId) {
    val buffer = ByteBufPool.buffer (16)
    pickle (Hello.pickle, Hello (localId), buffer)
    messenger.flush (socket, buffer, new CompletionHandler [Void, Void] {
      def completed (result: Void, attachment: Void) {
        peers.get (remoteId) connect (socket, input, remoteId)
      }
      def failed (exc: Throwable, attachment: Void) {
        events.exceptionWhileGreeting (exc)
        socket.close()
      }})
  }

  private def hearHello (socket: Socket) {
    val buffer = ByteBufPool.buffer()
    messenger.ensure (socket, buffer, 9, new CompletionHandler [Void, Void] {
      def completed (result: Void, attachment: Void) {
        val Hello (remoteId) = unpickle (Hello.pickle, buffer)
        sayHello (socket, buffer, remoteId)
      }
      def failed (exc: Throwable, attachment: Void) {
        events.exceptionWhileGreeting (exc)
        buffer.release()
        socket.close()
      }})
  }

  private def loop() {
    server.accept (null, new CompletionHandler [Socket, Void] {
      def completed (socket: Socket, attachment: Void) {
        scheduler.execute (hearHello (socket))
        loop()
      }
      def failed (exc: Throwable, attachment: Void) {
        exc match {
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
    server = ServerSocket.open (group)
    server.bind (localAddr)
    if (localAddr.isInstanceOf [InetSocketAddress])
      println ("Accepting messenger connections on " + localAddr)
    loop()
  }

  def shutdown(): Unit =
    if (server != null) server.close()
}
