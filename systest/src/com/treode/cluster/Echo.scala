package com.treode.cluster

import java.util.concurrent.TimeoutException
import scala.util.Random

import com.treode.async.{Async, Backoff, Callback, Fiber, Scheduler}
import com.treode.pickle.Picklers

import Async.async
import Callback.ignore

object Echo {

  private val echo = {
    import Picklers._
    RequestDescriptor (0xA8L, string, string)
  }

  def attach (localId: HostId) (implicit random: Random, scheduler: Scheduler, cluster: Cluster) {

    val fiber = new Fiber (scheduler)
    val backoff = Backoff (100, 200)
    val period = 10000
    var start = 0L
    var count = 0

    echo.listen { case (s, mdtr) =>
      mdtr.respond (s)
    }

    def loop: Async [Unit] = async { cb =>

      val hosts = ReplyTracker.settled (0, 1, 2)

      val port = echo.open { (_, from) =>
        fiber.execute {
          hosts += from
        }}

      val timer = cb.leave {
        port.close()
        count += 1
        if (count % period == 0) {
          val end = System.currentTimeMillis
          val ms = (end - start) .toDouble / period.toDouble
          val qps = period.toDouble / (end - start) .toDouble * 1000.0
          println ("%8d: %10.3f ms, %10.0f qps" format (count, ms, qps))
          start = System.currentTimeMillis
        }
      } .timeout (fiber, backoff) {
        echo ("Hello World")
      }}

    if (localId == HostId (2)) {
      start = System.currentTimeMillis
      scheduler.whilst (true) (loop) run (ignore)
    }}}
