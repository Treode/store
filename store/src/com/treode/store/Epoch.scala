package com.treode.store

import org.joda.time.DateTime

trait Epoch {

  def limit: TxClock
}

object Epoch {

  object UnixEpoch extends Epoch {

    def limit: TxClock = TxClock.MinValue
  }

  object StartOfPreviousHour extends Epoch {

    def limit: TxClock =
      DateTime.now.minusHours (1)
        .withMillisOfSecond (0)
        .withSecondOfMinute (0)
        .withMinuteOfHour (0)
  }

  object StartOfPreviousMonth extends Epoch {

    def limit: TxClock =
      DateTime.now.minusMonths (1)
        .withTimeAtStartOfDay()
        .withDayOfMonth (0)
  }

  object StartOfPreviousWeek extends Epoch {

    def limit: TxClock =
      DateTime.now.minusWeeks (1)
        .withTimeAtStartOfDay()
        .withDayOfWeek (0)
  }

  object StartOfYesterday extends Epoch {

    def limit: TxClock =
      DateTime.now.minusDays (1)
        .withTimeAtStartOfDay()
  }}
