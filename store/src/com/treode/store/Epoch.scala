package com.treode.store

import org.joda.time.DateTime

trait Epoch {

  def limit: TxClock
}

object Epoch {

  case class Const (limit: TxClock) extends Epoch

  val zero = Const (TxClock.MinValue)

  object StartOfYesterday extends Epoch {

    def limit: TxClock =
      DateTime.now.minusDays (1) .withTimeAtStartOfDay
  }}
