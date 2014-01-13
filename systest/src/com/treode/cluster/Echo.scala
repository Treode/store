package com.treode.cluster

import java.util.concurrent.TimeoutException

import com.treode.pickle.Picklers
import com.treode.cluster.misc.BackoffTimer

object Echo {

  private val _echo = {
    import Picklers._
    new RequestDescriptor (0xFF9F76CB490BE8A8L, string, string)
  }

  def attach () (implicit host: Host) {

    val period = 10000
    val backoff = BackoffTimer (100, 200) (host.random)
    var start = 0L

    _echo.register { case (s, mdtr) =>
      mdtr.respond (s)
    }

    def loop (i: Int) {
      new _echo.QuorumCollector ("Hello World") (host.locate (0), backoff) {

        process (_ => ())

        def quorum() {
          if (i % period == 0) {
            val end = System.currentTimeMillis
            val ms = (end - start) .toDouble / period.toDouble
            val qps = period.toDouble / (end - start) .toDouble * 1000.0
            println ("%8d: %10.3f ms, %10.0f qps" format (i, ms, qps))
            start = System.currentTimeMillis
          }
          loop (i + 1)
        }

        def timeout() = throw new TimeoutException
      }}

    if (host.localId == HostId (2)) {
      start = System.currentTimeMillis
      loop (1)
    }}}
