package com.treode

import com.treode.cluster.events.Events

package object cluster {

  private [cluster] implicit class ClusterEvents (events: Events) {

    def errorWhileGreeting (expected: HostId, found: HostId): Unit =
      events.warning (s"Error while greeting: expected remote host $expected but found $found")

    def unpicklingMessageConsumedWrongNumberOfBytes (id: MailboxId): Unit =
      events.warning (s"Unpickling a message consumed the wrong number of bytes: $id")

    def exceptionWhileGreeting (e: Throwable): Unit =
      events.warning (s"Error while greeting", e)

    def exceptionFromMessageHandler (e: Throwable): Unit =
      events.warning ("A message handler threw an exception.", e)

    def exceptionReadingMessage (e: Throwable): Unit =
      events.warning (s"Exception reading message", e)

    def exceptionWritingMessage (e: Throwable): Unit =
      events.warning (s"Exception writing message", e)

    def mailboxNotRecognized (id: MailboxId, length: Int): Unit =
      events.warning (s"Mailbox not recognized: $id")

    def recyclingMessengerSocket (e: Throwable): Unit =
      events.warning (s"Recycling messenger socket", e)
  }}
