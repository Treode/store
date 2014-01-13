package com.treode

import com.treode.cluster.events.Events

package object cluster {

  private [cluster] implicit class ClusterEvents (events: Events) {

    def errorWhileGreeting (expected: HostId, found: HostId): Unit =
      events.warning (s"Error while greeting: expected remote host $expected but found $found")

    def exceptionWhileGreeting (e: Throwable): Unit =
      events.warning (s"Error while greeting", e)

    def exceptionReadingMessage (e: Throwable): Unit =
      events.warning (s"Exception reading message", e)

    def exceptionWritingMessage (e: Throwable): Unit =
      events.warning (s"Exception writing message", e)

    def recyclingMessengerSocket (e: Throwable): Unit =
      events.warning (s"Recycling messenger socket", e)
  }}
