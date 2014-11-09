/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    def acceptingConnections (cellid: CellId, localId: HostId, localAddr: SocketAddress): Unit =
      logger.log (INFO, s"Accepting peer connections to $localId for $cellid on $localAddr")

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
