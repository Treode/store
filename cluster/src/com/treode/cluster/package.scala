package com.treode

import java.util.logging.{Level, Logger}

import Level.WARNING

package object cluster {

  class RemoteException extends Exception

  private [cluster] object log {

    val logger = Logger.getLogger ("com.treode.cluster")

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
  }}
