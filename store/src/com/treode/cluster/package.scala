package com.treode

import com.treode.cluster.events.Events

package object cluster {

  private [cluster] implicit class ClusterEvents (events: Events) {

    def errorWhileGreeting (expected: HostId, found: HostId): Unit =
      events.warning ("Error while greeting: expected remote host %016X but found %016X" format (expected, found))

    def exceptionWhileGreeting (e: Throwable): Unit =
      events.warning ("Error while greeting: " + e.toString)

    def exceptionFromMessageHandler (e: Throwable): Unit =
      events.warning ("A message handler threw an exception.", e)

    def exceptionReadingMessage (e: Throwable): Unit =
      events.warning ("Exception reading message: " + e.toString)

    def exceptionWritingMessage (e: Throwable): Unit =
      events.warning ("Exception writing message: " + e.toString)

    def mailboxNotRecognized (id: MailboxId, length: Int): Unit =
      events.warning ("Mailbox not recognized: " + id)

    def recyclingMessengerSocket (e: Throwable): Unit =
      events.warning ("Recycling messenger socket: " + e.getMessage)
  }}
