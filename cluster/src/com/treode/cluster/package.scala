package com.treode

import java.net.SocketAddress
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.util.logging.{Level, Logger}

import Level.{INFO, WARNING}

package object cluster {

  class IgnoreRequestException extends Exception

  class RemoteException extends Exception

  private [cluster] def isClosedException (t: Throwable): Boolean =
    t match {
      case t: AsynchronousCloseException => true
      case t: ClosedChannelException => true
      case t: Exception if t.getMessage == "End of file reached." => true
      case _ => false
    }

  private [cluster] object log {

    val logger = Logger.getLogger ("com.treode.cluster")

    def acceptingConnections (localId: HostId, localAddr: SocketAddress): Unit =
      logger.log (INFO, s"Accepting peer connections to $localId on $localAddr")

    def connected (remoteId: HostId, localAddr: SocketAddress, remoteAddr: SocketAddress): Unit =
      logger.log (INFO, s"Connected to $remoteId at $localAddr : $remoteAddr")

    def disconnected (remoteId: HostId, localAddr: SocketAddress, remoteAddr: SocketAddress): Unit =
      logger.log (INFO, s"Disconnected from $remoteId on $localAddr : $remoteAddr")

    def errorWhileGreeting (expected: HostId, found: HostId): Unit =
      logger.log (WARNING, s"Error while greeting: expected remote host $expected but found $found")

    def exceptionWhileGreeting (e: Throwable): Unit =
      logger.log (WARNING, s"Error while greeting", e)

    def exceptionReadingMessage (e: Throwable): Unit =
      logger.log (WARNING, s"Exception reading message", e)

    def exceptionWritingMessage (e: Throwable): Unit =
      logger.log (WARNING, s"Exception writing message", e)

    def recyclingMessengerSocket (e: Throwable): Unit =
      logger.log (WARNING, s"Recycling messenger socket", e)

    def rejectedForeignCell (remoteId: HostId, remoteCellId: CellId): Unit =
      logger.log (INFO, s"Rejected foreign cell $remoteId : $remoteCellId")
  }}
