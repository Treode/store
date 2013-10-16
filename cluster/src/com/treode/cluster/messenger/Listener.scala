package com.treode.cluster.messenger

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.{
  AsynchronousServerSocketChannel => ServerSocket,
  AsynchronousSocketChannel => JavaSocket,
  AsynchronousChannelGroup,
  AsynchronousCloseException,
  CompletionHandler}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.cluster.{ClusterEvents, HostId, messenger}
import com.treode.cluster.concurrent.{Callback, Scheduler}
import com.treode.cluster.events.Events
import com.treode.pickle._
import com.treode.cluster.Socket

class Listener (
  localId: HostId,
  localAddr: SocketAddress,
  group: AsynchronousChannelGroup,
  peers: PeerRegistry) (
    implicit scheduler: Scheduler,
    events: Events) {

  private var server: ServerSocket = null

  private def sayHello (socket: Socket, input: Input, remoteId: HostId) {
    val buffer = new Output (256)
    pickle (Hello.pickle, Hello (localId), buffer)
    messenger.flush (socket, buffer, new Callback [Unit] {
      def apply (v: Unit) {
        peers.get (remoteId) connect (socket, input, remoteId)
      }
      def fail (t: Throwable) {
        events.exceptionWhileGreeting (t)
        socket.close()
      }})
  }

  private def hearHello (socket: Socket) {
    val buffer = new Input (256)
    messenger.fill (socket, buffer, 9, new Callback [Unit] {
      def apply (v: Unit) {
        val Hello (remoteId) = unpickle (Hello.pickle, buffer)
        sayHello (socket, buffer, remoteId)
      }
      def fail (t: Throwable) {
        events.exceptionWhileGreeting (t)
        socket.close()
      }})
  }

  private def loop() {
    server.accept (null, new CompletionHandler [JavaSocket, Void] {
      def completed (socket: JavaSocket, attachment: Void) {
        scheduler.execute (hearHello (new Socket (socket)))
        loop()
      }
      def failed (t: Throwable, attachment: Void) {
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
    server = ServerSocket.open (group)
    server.bind (localAddr)
    if (localAddr.isInstanceOf [InetSocketAddress])
      println ("Accepting messenger connections on " + localAddr)
    loop()
  }

  def shutdown(): Unit =
    if (server != null) server.close()
}
