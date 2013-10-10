package com.treode.cluster.events

import java.util.logging.{Level, Logger}
import com.yammer.metrics.{Gauge, JmxReporter, MetricRegistry, Timer}

trait Events {

  def newGauge [A] (n1: String, n2: String) (v: => A)
  def newTimer [A] (n1: String, n2: String): Timer
  def info (msg: String)
  def warning (msg: String)
  def warning (msg: String, exn: Throwable)
}

object Events {

  val live = new Events {

    private val log = Logger.getLogger ("com.treode")

    private val registry = new MetricRegistry ("com.treode")
    JmxReporter.forRegistry (registry) .build().start()

    def newGauge [A] (n1: String, n2: String) (v: => A): Unit =
      registry.register (MetricRegistry.name (n1, n2),
        new Gauge [A] {
          def getValue = v
        })

    def newTimer [A] (n1: String, n2: String): Timer =
      registry.timer (MetricRegistry.name (n1, n2))

    def info (msg: String): Unit =
      log.info (msg)

    def warning (msg: String): Unit =
      log.warning (msg)

    def warning (msg: String, exn: Throwable): Unit =
      log.log (Level.WARNING, msg, exn)
  }}
