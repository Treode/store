package com.treode.cluster.events

import com.codahale.metrics.Timer
import org.scalatest.Assertions

object StubEvents extends Events {

  def newGauge [A] (n1: String, n2: String) (v: => A): Unit = ()

  def newTimer [A] (n1: String, n2: String): Timer = StubTimer

  def info (msg: String): Unit = ()

  def warning (msg: String): Unit =
    Assertions.fail (s"Warning emitted: $msg")

  def warning (msg: String, exn: Throwable): Unit =
    Assertions.fail (s"Warning emitted: $msg, ${exn.getMessage}")
}
